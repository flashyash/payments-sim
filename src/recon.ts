import { createHash, randomUUID } from "node:crypto";
import { createOp } from "./api.ts";
import { accountNames } from "./chart.ts";
import { tx, type Client } from "./db.ts";
import {
  chartId,
  drive,
  finishBare,
  FLOWS,
  preparePosting,
  recordPosting,
  sendPosting,
  stick,
  type Ctx,
  type Step,
} from "./engine.ts";
import { failpoint } from "./failpoints.ts";
import { feeFor } from "./network/app.ts";

// Daily reconciliation: the network's settlement file against our settlement_items, with the ledger as a third witness.
// Only lines that fully match move money. Anything else becomes an exception, and the merchant's other lines still settle.

export interface Line {
  ref: string;
  type: string;
  merchantId: string;
  gross: bigint;
  fee: bigint;
  net: bigint;
}

interface Item {
  ref: string;
  type: string;
  merchant_id: string;
  amount: bigint;
  shard: number;
  ledger_txn_id: string;
  network_at: Date;
  state: string;
}

export type ExceptionType =
  | "missing_in_file"
  | "missing_in_books"
  | "amount_mismatch"
  | "fee_mismatch"
  | "duplicate_in_file"
  | "already_settled"
  | "ledger_mismatch"
  | "merchant_mismatch";

export interface ReconException {
  ref: string;
  type: ExceptionType;
  expected: unknown;
  actual: unknown;
}

export interface ReconReport {
  fileId: string;
  replayed: boolean;
  lines: number;
  matched: number;
  exceptions: ReconException[];
  resolved: string[];
  settlementOps: string[];
}

export function parseSettlementFile(csv: string): Line[] {
  const [header, ...rows] = csv.trim().split("\n");
  if (header !== "ref,type,merchant_id,gross,fee,net,currency") throw new Error(`unexpected settlement file header: ${header}`);
  return rows.map((row) => {
    const [ref, type, merchantId, gross, fee, net] = row.split(",");
    return { ref, type, merchantId, gross: BigInt(gross), fee: BigInt(fee), net: BigInt(net) };
  });
}

const lineJson = (l: Line) => ({ type: l.type, merchantId: l.merchantId, gross: l.gross.toString(), fee: l.fee.toString(), net: l.net.toString() });
const lineFrom = (ref: string, j: ReturnType<typeof lineJson>): Line => ({
  ref,
  type: j.type,
  merchantId: j.merchantId,
  gross: BigInt(j.gross),
  fee: BigInt(j.fee),
  net: BigInt(j.net),
});

// Which entry in the ledger transaction proves this item, and with what signed amount.
function expectedEntry(item: Item): { name: string; amount: bigint } {
  switch (item.type) {
    case "capture":
      return { name: accountNames.pending(item.merchant_id), amount: item.amount };
    case "refund":
      return { name: accountNames.available(item.merchant_id), amount: -item.amount };
    case "chargeback":
      return { name: accountNames.debt(item.merchant_id), amount: -item.amount };
    default:
      return { name: accountNames.debt(item.merchant_id), amount: item.amount };
  }
}

async function ledgerAgrees(ctx: Ctx, item: Item): Promise<boolean> {
  const txn = await ctx.ledger.transaction(item.ledger_txn_id);
  if (!txn) return false;
  const expected = expectedEntry(item);
  const account = await ctx.chart.id(expected.name);
  return txn.entries.some((e) => e.accountId === account && BigInt(e.amount) === expected.amount);
}

async function loadItems(c: Pick<Client, "query">, refs: string[], lock: boolean): Promise<Map<string, Item>> {
  const { rows } = await c.query<Item>(`select * from settlement_items where ref = any($1)${lock ? " for update" : ""}`, [refs]);
  return new Map(rows.map((r) => [r.ref, r]));
}

export async function reconcileFile(ctx: Ctx, fileId: string, feeBps = ctx.cfg.feeBps): Promise<ReconReport> {
  const file = await ctx.network.settlementFile(fileId);
  if (!file) throw new Error(`no settlement file ${fileId}`);
  const sha256 = createHash("sha256").update(file.csv).digest("hex");
  const cutoff = new Date(file.cutoff);
  const lines = parseSettlementFile(file.csv);

  // Lines we couldn't place last time. Usually a timing difference: the network's message landed in the file before
  // our op committed. They get another chance on every run.
  const carried = await ctx.pool.query<{ ref: string; actual: ReturnType<typeof lineJson> }>(
    "select ref, actual from recon_exceptions where type = 'missing_in_books' and state = 'open' and file_id <> $1",
    [fileId],
  );
  const carriedLines = carried.rows.map((r) => lineFrom(r.ref, r.actual));
  const allRefs = [...new Set([...lines, ...carriedLines].map((l) => l.ref))];

  // The ledger check is network I/O, so it runs before the transaction. Entries are immutable, so it can't go stale.
  const candidates = await loadItems(ctx.pool, allRefs, false);
  const ledgerOk = new Map<string, boolean>();
  const items = [...candidates.values()];
  for (let i = 0; i < items.length; i += 20) {
    await Promise.all(items.slice(i, i + 20).map(async (it) => ledgerOk.set(it.ref, await ledgerAgrees(ctx, it))));
  }

  const report = await tx(ctx.pool, async (c): Promise<ReconReport> => {
    await c.query("select pg_advisory_xact_lock(hashtext('reconciliation'))");
    const prior = await c.query<{ sha256: string; lines: number; matched: number }>("select sha256, lines, matched from recon_runs where file_id = $1", [fileId]);
    if (prior.rowCount) {
      if (prior.rows[0].sha256 !== sha256) throw new Error(`settlement file ${fileId} changed since it was reconciled`);
      const { rows } = await c.query<ReconException>("select ref, type, expected, actual from recon_exceptions where file_id = $1", [fileId]);
      return { fileId, replayed: true, lines: prior.rows[0].lines, matched: prior.rows[0].matched, exceptions: rows, resolved: [], settlementOps: [] };
    }

    const locked = await loadItems(c, allRefs, true);
    const counts = new Map<string, number>();
    for (const l of lines) counts.set(l.ref, (counts.get(l.ref) ?? 0) + 1);

    const exceptions: ReconException[] = [];
    const matched: { line: Line; item: Item }[] = [];
    const resolved: string[] = [];

    const classify = (line: Line, isCarried: boolean): void => {
      const item = locked.get(line.ref);
      const add = (type: ExceptionType, expected: unknown, actual: unknown): void => {
        exceptions.push({ ref: line.ref, type, expected, actual });
      };
      if (!item) {
        if (!isCarried) add("missing_in_books", null, lineJson(line));
        return;
      }
      // Its op committed after our pre-transaction ledger check, so it's unverified. Same as not having it yet: next run.
      if (!ledgerOk.has(item.ref)) {
        if (!isCarried) add("missing_in_books", null, lineJson(line));
        return;
      }
      if (item.state !== "unsettled") return add("already_settled", { state: item.state }, lineJson(line));
      if (item.merchant_id !== line.merchantId) return add("merchant_mismatch", { merchantId: item.merchant_id }, { merchantId: line.merchantId });
      if (item.amount !== line.gross) return add("amount_mismatch", { gross: item.amount.toString() }, { gross: line.gross.toString() });
      const fee = feeFor(item.type, item.amount, feeBps);
      if (fee !== line.fee) return add("fee_mismatch", { fee: fee.toString() }, { fee: line.fee.toString() });
      if (!ledgerOk.get(item.ref)) return add("ledger_mismatch", expectedEntry(item).amount.toString(), item.ledger_txn_id);
      matched.push({ line, item });
      if (isCarried) resolved.push(line.ref);
    };

    for (const [ref, n] of counts) {
      if (n > 1) {
        const line = lines.find((l) => l.ref === ref)!;
        exceptions.push({ ref, type: "duplicate_in_file", expected: 1, actual: { count: n, ...lineJson(line) } });
      }
    }
    const seen = new Set<string>();
    for (const line of lines) {
      if (counts.get(line.ref)! > 1 || seen.has(line.ref)) continue;
      seen.add(line.ref);
      classify(line, false);
    }
    for (const line of carriedLines) if (!seen.has(line.ref)) classify(line, true);

    // We have it, it's old enough to be in this file, the file doesn't have it, and nobody's already looking at it.
    const inFile = new Set(lines.map((l) => l.ref));
    const { rows: absent } = await c.query<Item>(
      `select * from settlement_items i
        where i.state = 'unsettled' and i.network_at <= $1
          and not exists (select 1 from recon_exceptions e where e.ref = i.ref and e.state = 'open')
        for update`,
      [cutoff],
    );
    for (const item of absent) {
      if (inFile.has(item.ref) || matched.some((m) => m.item.ref === item.ref)) continue;
      exceptions.push({ ref: item.ref, type: "missing_in_file", expected: { type: item.type, gross: item.amount.toString() }, actual: null });
    }

    await c.query("insert into recon_runs (file_id, sha256, cutoff, lines, matched, exceptions, resolved) values ($1,$2,$3,$4,$5,$6,$7)", [
      fileId,
      sha256,
      cutoff,
      lines.length,
      matched.length,
      exceptions.length,
      resolved.length,
    ]);
    for (const e of exceptions) {
      await c.query(
        `insert into recon_exceptions (file_id, ref, type, expected, actual) values ($1,$2,$3,$4,$5) on conflict (file_id, ref, type) do nothing`,
        [fileId, e.ref, e.type, JSON.stringify(e.expected), JSON.stringify(e.actual)],
      );
    }

    // A line that finally matched closes any exception left from an earlier file: it was only a timing difference.
    const matchedRefs = matched.map((m) => m.item.ref);
    const closed = await c.query<{ ref: string }>(
      `update recon_exceptions set state = 'resolved', resolution = $2
        where ref = any($1) and state = 'open' and type in ('missing_in_file','missing_in_books') returning ref`,
      [matchedRefs, `timing difference: matched in file ${fileId}`],
    );
    for (const r of closed.rows) if (!resolved.includes(r.ref)) resolved.push(r.ref);

    // One settlement op per merchant. Each op's posting is built from exactly the items it claims here.
    const byMerchant = new Map<string, { line: Line; item: Item }[]>();
    for (const m of matched) byMerchant.set(m.item.merchant_id, [...(byMerchant.get(m.item.merchant_id) ?? []), m]);
    const settlementOps: string[] = [];
    for (const [merchantId, group] of byMerchant) {
      const op = await createOp(c, { paymentId: null, merchantId, kind: "settlement", step: "post", networkRef: false, graceless: true });
      settlementOps.push(op);
      for (const { line, item } of group) {
        await c.query("update settlement_items set state = 'settling', file_id = $2, fee = $3, settle_op_id = $4 where ref = $1", [
          item.ref,
          fileId,
          line.fee.toString(),
          op,
        ]);
      }
    }
    return { fileId, replayed: false, lines: lines.length, matched: matched.length, exceptions, resolved, settlementOps };
  });

  failpoint("recon.prepared");
  await Promise.all(report.settlementOps.map((op) => drive(ctx, op)));
  return report;
}

export function settlementEntries(
  items: { type: string; amount: bigint; fee: bigint; shard: number }[],
): { pending: bigint; available: bigint; cash: bigint; receivable: Map<number, bigint> } {
  let pending = 0n;
  let available = 0n;
  let cash = 0n;
  const receivable = new Map<number, bigint>();
  const addReceivable = (shard: number, a: bigint) => receivable.set(shard, (receivable.get(shard) ?? 0n) + a);
  for (const it of items) {
    switch (it.type) {
      case "capture":
        addReceivable(it.shard, it.amount);
        cash -= it.amount - it.fee;
        pending -= it.amount;
        available += it.amount - it.fee;
        break;
      case "refund":
      case "chargeback":
        addReceivable(it.shard, -it.amount);
        cash += it.amount;
        break;
      case "chargeback_reversal":
        addReceivable(it.shard, it.amount);
        cash -= it.amount;
        break;
      default:
        throw new Error(`unknown settlement item type ${it.type}`);
    }
  }
  return { pending, available, cash, receivable };
}

export const settlement: Record<string, Step> = {
  async post(ctx, op, lease) {
    const { rows: items } = await ctx.pool.query<{ ref: string; type: string; amount: bigint; fee: bigint; shard: number }>(
      "select ref, type, amount, fee, shard from settlement_items where settle_op_id = $1",
      [op.id],
    );
    const s = settlementEntries(items);
    const net = -s.cash;
    const done = async (txnId: string | null, c: Client) => {
      await c.query("update settlement_items set state = 'settled' where settle_op_id = $1", [op.id]);
      const eventId = randomUUID();
      await c.query("insert into outbox (event_id, merchant_id, type, payload) values ($1,$2,'settlement.paid',$3)", [
        eventId,
        op.merchant_id,
        JSON.stringify({
          id: eventId,
          type: "settlement.paid",
          createdAt: new Date().toISOString(),
          data: { merchantId: op.merchant_id, lines: items.length, net: net.toString(), ledgerTransactionId: txnId },
        }),
      ]);
    };
    await preparePosting(ctx, op, "settle", async () => {
      const entries: [string, bigint][] = [
        [await chartId(ctx, accountNames.pending(op.merchant_id)), s.pending],
        [await chartId(ctx, accountNames.available(op.merchant_id)), s.available],
        [await chartId(ctx, accountNames.bankCash()), s.cash],
      ];
      for (const [shard, amount] of s.receivable) entries.push([await chartId(ctx, accountNames.receivable(shard)), amount]);
      return { description: `settlement for ${op.merchant_id}`, entries };
    });
    const nonZero = s.pending !== 0n || s.available !== 0n || s.cash !== 0n || [...s.receivable.values()].some((a) => a !== 0n);
    if (!nonZero) {
      return finishBare(ctx, op, lease, 200, { lines: items.length, net: "0" }, async (c) => {
        await c.query("delete from ledger_postings where operation_id = $1", [op.id]);
        await done(null, c);
      });
    }
    const outcome = await sendPosting(ctx, op, "settle");
    failpoint("settlement.post.after");
    if (outcome.kind === "rejected") return stick(ctx, op, lease, `settlement rejected: ${outcome.code} ${outcome.message}`);
    await finishBare(ctx, op, lease, 200, { lines: items.length, net: net.toString(), ledgerTransactionId: outcome.txnId }, async (c) => {
      await recordPosting(c, op, "settle", outcome);
      await done(outcome.txnId, c);
    });
  },
};

FLOWS.settlement = settlement;
