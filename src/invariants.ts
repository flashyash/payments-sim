import { accountNames } from "./chart.ts";
import type { Pool } from "./db.ts";
import { openingBalance } from "./network/app.ts";

// The oracle. It reads every service's database directly, which the services themselves never do, and checks that the
// three independent records of what happened agree. Each returned string is one violated invariant instance.

export interface InvariantPools {
  processor: Pool;
  network: Pool;
  sink: Pool;
  ledger: Pool;
}

export interface InvariantReport {
  violations: string[];
  checked: Record<string, number>;
}

const big = (v: unknown) => BigInt((v as bigint | string | null) ?? 0);

export async function checkInvariants(p: InvariantPools, opts: { requireSink?: boolean } = {}): Promise<InvariantReport> {
  const v: string[] = [];
  const add = (invariant: string, detail: string) => v.push(`${invariant}: ${detail}`);

  // 1. The ledger balances.
  const unbalanced = await p.ledger.query(
    "select transaction_id, currency, sum(amount)::text as s from entries group by transaction_id, currency having sum(amount) <> 0",
  );
  for (const r of unbalanced.rows) add("ledger_balances", `transaction ${r.transaction_id} sums to ${r.s}`);
  const drift = await p.ledger.query(
    `select b.account_id, b.balance::text as cached, coalesce(sum(e.amount), 0)::text as actual
       from balances b left join entries e on e.account_id = b.account_id
      group by b.account_id, b.balance having b.balance <> coalesce(sum(e.amount), 0)`,
  );
  for (const r of drift.rows) add("ledger_balances", `account ${r.account_id} cached ${r.cached}, entries say ${r.actual}`);

  // 2 + 3. Nothing captured twice or past the authorization, on all three records.
  const { rows: payments } = await p.processor.query<{
    id: string;
    status: string;
    auth_ref: string;
    card_token: string;
    hold_account_id: string | null;
    amount_authorized: bigint;
    amount_captured: bigint;
    amount_released: bigint;
    amount_refunded: bigint;
    reserved_capture: bigint;
    reserved_refund: bigint;
    event_seq: number;
  }>("select * from payments");
  const netAgg = await p.network.query<{ auth_ref: string; type: string; total: bigint; n: bigint }>(
    `select auth_ref, type, sum(amount)::bigint as total, count(*)::bigint as n
       from messages where status = 'approved' and type in ('capture','refund') group by auth_ref, type`,
  );
  const netCaptured = new Map<string, bigint>();
  const netRefunded = new Map<string, bigint>();
  for (const r of netAgg.rows) (r.type === "capture" ? netCaptured : netRefunded).set(r.auth_ref, r.total);

  const opsCaptured = await p.processor.query<{ payment_id: string; total: bigint }>(
    "select payment_id, sum(amount)::bigint as total from operations where kind = 'capture' and state = 'succeeded' group by payment_id",
  );
  const opCaptured = new Map(opsCaptured.rows.map((r) => [r.payment_id, r.total]));

  // Every capture the network approved has to be one of our capture ops. A second approval under another ref is a double capture.
  const knownRefs = await p.processor.query<{ network_ref: string }>("select network_ref from operations where kind in ('capture','refund') and network_ref is not null");
  const known = new Set(knownRefs.rows.map((r) => r.network_ref));
  const netMsgs = await p.network.query<{ ref: string; type: string; auth_ref: string }>(
    "select ref, type, auth_ref from messages where status = 'approved' and type in ('capture','refund')",
  );
  for (const m of netMsgs.rows) if (!known.has(m.ref)) add("nothing_captured_twice", `network approved ${m.type} ${m.ref} that no op of ours sent`);

  const netAuths = await p.network.query<{ ref: string; status: string; amount: bigint; remaining: bigint | null; card_token: string | null }>(
    "select ref, status, amount, remaining, card_token from messages where type = 'auth'",
  );
  const netAuth = new Map(netAuths.rows.map((r) => [r.ref, r]));
  const localRefs = new Set(payments.map((x) => x.auth_ref));

  const holdIds = payments.map((x) => x.hold_account_id).filter((x): x is string => x !== null);
  const holdBalances = await p.ledger.query<{ account_id: string; balance: bigint }>("select account_id, balance from balances where account_id = any($1::uuid[])", [holdIds]);
  const holdBalance = new Map(holdBalances.rows.map((r) => [r.account_id, r.balance]));

  for (const pay of payments) {
    const nc = netCaptured.get(pay.auth_ref) ?? 0n;
    if (nc !== pay.amount_captured) add("nothing_captured_twice", `payment ${pay.id}: network captured ${nc}, we recorded ${pay.amount_captured}`);
    const oc = opCaptured.get(pay.id) ?? 0n;
    if (oc !== pay.amount_captured) add("nothing_captured_twice", `payment ${pay.id}: capture ops total ${oc}, payment says ${pay.amount_captured}`);
    if (pay.amount_captured + pay.amount_released > pay.amount_authorized) add("not_beyond_auth", `payment ${pay.id}: captured + released exceeds authorized`);
    if (nc > pay.amount_authorized) add("not_beyond_auth", `payment ${pay.id}: network captured ${nc} of ${pay.amount_authorized}`);
    const nr = netRefunded.get(pay.auth_ref) ?? 0n;
    if (nr !== pay.amount_refunded) add("refunds_agree", `payment ${pay.id}: network refunded ${nr}, we recorded ${pay.amount_refunded}`);
    if (pay.amount_refunded > pay.amount_captured) add("refunds_agree", `payment ${pay.id}: refunded more than captured`);
    if (pay.reserved_capture !== 0n || pay.reserved_refund !== 0n) add("nothing_left_running", `payment ${pay.id} still holds a reservation`);

    // 4. Holds agree.
    const opened = ["authorized", "partially_captured", "captured", "partially_refunded", "refunded", "voided", "expired"].includes(pay.status);
    if (opened) {
      const expected = pay.amount_authorized - pay.amount_captured - pay.amount_released;
      const actual = pay.hold_account_id ? (holdBalance.get(pay.hold_account_id) ?? 0n) : null;
      if (actual !== expected) add("holds_agree", `payment ${pay.id} (${pay.status}): hold balance ${actual}, expected ${expected}`);
    }

    // 6. The issuer's hold matches what we think is open.
    const a = netAuth.get(pay.auth_ref);
    const issuerHold = a?.status === "approved" ? (a.remaining ?? 0n) : 0n;
    const localHold = ["authorized", "partially_captured"].includes(pay.status) ? pay.amount_authorized - pay.amount_captured - pay.amount_released : 0n;
    if (issuerHold !== localHold) add("no_orphan_holds", `payment ${pay.id} (${pay.status}): issuer holds ${issuerHold}, we think ${localHold}`);
  }
  for (const a of netAuths.rows) {
    if (!localRefs.has(a.ref) && a.status === "approved") add("no_orphan_holds", `issuer has auth ${a.ref} that we don't know about`);
  }

  // 6b. Conservation at the issuer: no card's credit leaked.
  const cards = await p.network.query<{ token: string; available: bigint }>("select token, available from cards");
  const cardAgg = await p.network.query<{ token: string; held: bigint; captured: bigint; refunded: bigint }>(
    `with auths as (select ref, card_token, remaining from messages where type = 'auth' and status = 'approved')
     select a.card_token as token,
            coalesce(sum(a.remaining), 0)::bigint as held,
            coalesce((select sum(m.amount) from messages m join auths x on x.ref = m.auth_ref where x.card_token = a.card_token and m.type = 'capture' and m.status = 'approved'), 0)::bigint as captured,
            coalesce((select sum(m.amount) from messages m join auths x on x.ref = m.auth_ref where x.card_token = a.card_token and m.type = 'refund' and m.status = 'approved'), 0)::bigint as refunded
       from auths a group by a.card_token`,
  );
  const agg = new Map(cardAgg.rows.map((r) => [r.token, r]));
  for (const c of cards.rows) {
    const a = agg.get(c.token);
    const expected = openingBalance(c.token) - (a?.held ?? 0n) - (a?.captured ?? 0n) + (a?.refunded ?? 0n);
    if (expected !== c.available) add("no_orphan_holds", `card ${c.token}: available ${c.available}, expected ${expected}`);
  }

  // 5. Every posting we made is in the ledger exactly once, under the key we meant.
  const localPostings = await p.processor.query<{ key: string; state: string; ledger_txn_id: string | null }>("select key, state, ledger_txn_id from ledger_postings");
  const ledgerKeys = await p.ledger.query<{ key: string; transaction_id: string }>("select key, transaction_id from idempotency_keys where key like 'ps:%'");
  const ledgerKey = new Map(ledgerKeys.rows.map((r) => [r.key, r.transaction_id]));
  const localKey = new Map(localPostings.rows.map((r) => [r.key, r]));
  for (const r of localPostings.rows) {
    const inLedger = ledgerKey.get(r.key);
    if (r.state === "posted" && inLedger !== r.ledger_txn_id) add("postings_exact", `posting ${r.key}: we recorded ${r.ledger_txn_id}, ledger has ${inLedger}`);
    if (r.state === "rejected" && inLedger) add("postings_exact", `posting ${r.key} was rejected and final, but the ledger has transaction ${inLedger}`);
    if (r.state === "pending") add("nothing_left_running", `posting ${r.key} never resolved`);
  }
  for (const k of ledgerKey.keys()) if (!localKey.has(k)) add("postings_exact", `ledger has key ${k} that we have no record of`);

  // 7. Every webhook delivered, validly, and every state change produced one.
  const undelivered = await p.processor.query<{ n: number; oldest: Date | null }>("select count(*)::int as n, min(created_at) as oldest from outbox where delivered_at is null");
  if (undelivered.rows[0].n) add("webhooks_delivered", `${undelivered.rows[0].n} events undelivered`);
  const seqs = await p.processor.query<{ id: string; n: number; event_seq: number }>(
    "select p.id, p.event_seq, (select count(*) from outbox o where o.payment_id = p.id)::int as n from payments p",
  );
  for (const r of seqs.rows) if (r.n !== r.event_seq) add("webhooks_delivered", `payment ${r.id}: ${r.event_seq} state changes but ${r.n} events`);
  let eventsChecked = 0;
  if (opts.requireSink !== false) {
    const expected = await p.processor.query<{ event_id: string }>(
      "select o.event_id from outbox o join merchants m on m.id = o.merchant_id where m.webhook_url is not null",
    );
    const got = await p.sink.query<{ event_id: string }>("select event_id from processed_events");
    const gotSet = new Set(got.rows.map((r) => r.event_id));
    for (const e of expected.rows) if (!gotSet.has(e.event_id)) add("webhooks_delivered", `event ${e.event_id} never reached the merchant`);
    eventsChecked = expected.rows.length;
    const bad = await p.sink.query<{ n: number }>("select count(*)::int as n from deliveries where response = 401");
    if (bad.rows[0].n) add("webhooks_delivered", `${bad.rows[0].n} deliveries failed signature verification`);
  }

  // 8. Nothing left running.
  const open = await p.processor.query<{ kind: string; state: string; n: number }>(
    "select kind, state, count(*)::int as n from operations where state in ('in_progress','stuck') group by kind, state",
  );
  for (const r of open.rows) add("nothing_left_running", `${r.n} ${r.kind} ops ${r.state}`);
  const parked = await p.processor.query<{ n: number }>("select count(*)::int as n from network_inbox where state = 'pending'");
  if (parked.rows[0].n) add("nothing_left_running", `${parked.rows[0].n} notifications unprocessed`);
  const undeliveredNet = await p.network.query<{ n: number }>("select count(*)::int as n from notifications where delivered_at is null");
  if (undeliveredNet.rows[0].n) add("nothing_left_running", `${undeliveredNet.rows[0].n} network notifications undelivered`);

  // 9. Each ledger balance is what the local records imply.
  const chart = await p.processor.query<{ name: string; id: string }>("select name, id from ledger_accounts");
  const ids = chart.rows.map((r) => r.id);
  const bal = await p.ledger.query<{ account_id: string; balance: bigint }>("select account_id, balance from balances where account_id = any($1::uuid[])", [ids]);
  const balById = new Map(bal.rows.map((r) => [r.account_id, r.balance]));
  const balance = new Map(chart.rows.map((r) => [r.name, balById.get(r.id) ?? 0n]));
  const items = await p.processor.query<{ merchant_id: string; type: string; state: string; amount: bigint; fee: bigint | null }>(
    "select merchant_id, type, state, amount, fee from settlement_items",
  );
  const topups = await p.processor.query<{ merchant_id: string; total: bigint }>(
    "select merchant_id, sum(amount)::bigint as total from operations where kind = 'reserve_topup' and state = 'succeeded' group by merchant_id",
  );
  const refunds = await p.processor.query<{ merchant_id: string; total: bigint }>("select merchant_id, sum(amount_refunded)::bigint as total from payments group by merchant_id");
  const merchants = await p.processor.query<{ id: string }>("select id from merchants");

  const exp = new Map<string, { pending: bigint; available: bigint; debt: bigint }>();
  for (const m of merchants.rows) exp.set(m.id, { pending: 0n, available: 0n, debt: 0n });
  let receivable = 0n;
  let cash = 0n;
  for (const it of items.rows) {
    const e = exp.get(it.merchant_id)!;
    const settled = it.state === "settled";
    const fee = big(it.fee);
    if (it.type === "capture") {
      if (settled) {
        e.available += it.amount - fee;
        cash -= it.amount - fee;
      } else {
        e.pending += it.amount;
        receivable -= it.amount;
      }
    } else if (it.type === "refund") {
      if (settled) cash += it.amount;
      else receivable += it.amount;
    } else if (it.type === "chargeback") {
      e.debt -= it.amount;
      if (settled) cash += it.amount;
      else receivable += it.amount;
    } else {
      e.debt += it.amount;
      if (settled) cash -= it.amount;
      else receivable -= it.amount;
    }
  }
  for (const t of topups.rows) {
    exp.get(t.merchant_id)!.available += t.total;
    cash -= t.total;
  }
  for (const r of refunds.rows) exp.get(r.merchant_id)!.available -= r.total;

  for (const [m, e] of exp) {
    for (const [name, want] of [
      [accountNames.pending(m), e.pending],
      [accountNames.available(m), e.available],
      [accountNames.debt(m), e.debt],
    ] as const) {
      const got = balance.get(name) ?? 0n;
      if (got !== want) add("balances_match_records", `${name}: ledger ${got}, records imply ${want}`);
    }
  }
  let gotReceivable = 0n;
  for (const [name, b] of balance) if (name.startsWith("network_receivable:")) gotReceivable += b;
  if (gotReceivable !== receivable) add("balances_match_records", `network_receivable: ledger ${gotReceivable}, records imply ${receivable}`);
  const gotCash = balance.get(accountNames.bankCash()) ?? 0n;
  if (gotCash !== cash) add("balances_match_records", `bank_cash: ledger ${gotCash}, records imply ${cash}`);

  return {
    violations: v,
    checked: {
      payments: payments.length,
      ledgerTransactions: ledgerKeys.rows.length,
      networkAuths: netAuths.rows.length,
      webhookEvents: eventsChecked,
      settlementItems: items.rows.length,
    },
  };
}
