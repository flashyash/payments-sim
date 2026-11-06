import { randomUUID } from "node:crypto";
import { accountNames, shardOf, type Chart } from "./chart.ts";
import { Unknown, type LedgerClient, type NetworkClient, type PostOutcome } from "./clients.ts";
import type { ProcessorConfig } from "./config.ts";
import { tx, type Client, type Pool } from "./db.ts";
import { failpoint } from "./failpoints.ts";
import { transition, type PaymentEvent, type PaymentStatus } from "./stateMachine.ts";

// The operation engine. An operation is a durable to-do list: a driver holding the lease does one step, commits, and
// moves on. If it dies, the lease expires and anyone else resumes from the last committed step. Every local write
// carries the fencing token, so a driver that lost its lease can't overwrite the new owner's progress.

export interface Ctx {
  pool: Pool;
  cfg: ProcessorConfig;
  ledger: LedgerClient;
  network: NetworkClient;
  chart: Chart;
}

export interface Op {
  id: string;
  payment_id: string | null;
  merchant_id: string;
  kind: string;
  amount: bigint | null;
  final_capture: boolean;
  dispute_id: string | null;
  step: string;
  state: "in_progress" | "succeeded" | "failed" | "stuck";
  error_code: string | null;
  network_ref: string | null;
  network_at: Date | null;
  attempts: number;
  deadline_at: Date | null;
  deadline_passed: boolean; // computed by the database: its clock set deadline_at, so its clock judges it
  result: { status: number; body: unknown } | null;
}

export interface Payment {
  id: string;
  merchant_id: string;
  currency: string;
  card_token: string;
  status: PaymentStatus;
  amount_authorized: bigint;
  amount_captured: bigint;
  amount_released: bigint;
  amount_refunded: bigint;
  amount_disputed: bigint;
  reserved_capture: bigint;
  reserved_refund: bigint;
  hold_account_id: string | null;
  auth_ref: string;
  expires_at: Date;
  event_seq: number;
}

export const remaining = (p: Payment) => p.amount_authorized - p.amount_captured - p.amount_released - p.reserved_capture;
export const refundable = (p: Payment) => p.amount_captured - p.amount_refunded - p.amount_disputed - p.reserved_refund;

export function paymentJson(p: Payment) {
  return {
    id: p.id,
    merchantId: p.merchant_id,
    status: p.status,
    currency: p.currency.trim(),
    amountAuthorized: p.amount_authorized.toString(),
    amountCaptured: p.amount_captured.toString(),
    amountReleased: p.amount_released.toString(),
    amountRefunded: p.amount_refunded.toString(),
    amountDisputed: p.amount_disputed.toString(),
  };
}

// Thrown when a step needs another attempt later (network or ledger didn't give an answer).
export class Retry extends Error {
  name = "Retry";
}
// Thrown when our fencing token is stale: someone else owns this op now, stop touching it.
class Fenced extends Error {
  name = "Fenced";
}

export interface Lease {
  token: bigint;
}

export type Step = (ctx: Ctx, op: Op, lease: Lease) => Promise<void>;

export async function fence(c: Client, opId: string, lease: Lease): Promise<void> {
  // Row lock + token check: if this passes, nobody else can take over until we commit.
  const { rowCount } = await c.query("select 1 from operations where id = $1 and lease_token = $2 for update", [
    opId,
    lease.token.toString(),
  ]);
  if (!rowCount) throw new Fenced(opId);
}

// One statement is its own fence: it waits for the row lock, then rechecks the token against the row as committed.
async function setStep(ctx: Ctx, op: Op, lease: Lease, step: string, networkAt?: string): Promise<void> {
  const { rowCount } = await ctx.pool.query("update operations set step = $3, network_at = coalesce($4, network_at) where id = $1 and lease_token = $2", [
    op.id,
    lease.token.toString(),
    step,
    networkAt ?? null,
  ]);
  if (!rowCount) throw new Fenced(op.id);
}

async function lockPayment(c: Client, id: string): Promise<Payment> {
  const { rows } = await c.query<Payment>("select * from payments where id = $1 for update", [id]);
  return rows[0];
}

export async function emit(c: Client, p: Payment, type: string, extra: object = {}): Promise<void> {
  const eventId = randomUUID();
  const seq = p.event_seq + 1;
  await c.query("update payments set event_seq = $2 where id = $1", [p.id, seq]);
  p.event_seq = seq;
  await c.query(
    `insert into outbox (event_id, merchant_id, payment_id, type, payload) values ($1, $2, $3, $4, $5)`,
    [
      eventId,
      p.merchant_id,
      p.id,
      type,
      JSON.stringify({ id: eventId, type, seq, createdAt: new Date().toISOString(), data: { ...paymentJson(p), ...extra } }),
    ],
  );
}

// Finishes an op: the payment change, its webhook and the frozen response commit together or not at all.
async function finish(
  ctx: Ctx,
  op: Op,
  lease: Lease,
  outcome: { state: "succeeded" | "failed"; error?: string; httpStatus: number },
  apply: (c: Client, p: Payment) => Promise<void>,
): Promise<void> {
  await tx(ctx.pool, async (c) => {
    await fence(c, op.id, lease);
    const p = await lockPayment(c, op.payment_id!);
    await apply(c, p);
    const body = outcome.state === "failed" ? { error: outcome.error, payment: paymentJson(p) } : paymentJson(p);
    await c.query(
      `update operations set state = $2, error_code = $3, result = $4, finished_at = now(), lease_until = null where id = $1`,
      [op.id, outcome.state, outcome.error ?? null, JSON.stringify({ status: outcome.httpStatus, body })],
    );
  });
}

export async function savePayment(c: Client, p: Payment): Promise<void> {
  await c.query(
    `update payments set status = $2, amount_captured = $3, amount_released = $4, amount_refunded = $5,
       amount_disputed = $6, reserved_capture = $7, reserved_refund = $8, updated_at = now() where id = $1`,
    [
      p.id,
      p.status,
      p.amount_captured.toString(),
      p.amount_released.toString(),
      p.amount_refunded.toString(),
      p.amount_disputed.toString(),
      p.reserved_capture.toString(),
      p.reserved_refund.toString(),
    ],
  );
}

// An op whose books disagree with a fact we've already acted on. A human has to look. Never guess.
export async function stick(ctx: Ctx, op: Op, lease: Lease, why: string): Promise<void> {
  await tx(ctx.pool, async (c) => {
    await fence(c, op.id, lease);
    await c.query("update operations set state = 'stuck', error_code = $2, lease_until = null, finished_at = now() where id = $1", [
      op.id,
      why.slice(0, 500),
    ]);
  });
  console.error(`[engine] op ${op.id} (${op.kind}) stuck: ${why}`);
}

// --- Ledger postings -------------------------------------------------------------------------------------------

type Entries = [accountId: string, amount: bigint][];

function bodyFor(entries: Entries, description: string) {
  return {
    description,
    entries: entries.filter(([, a]) => a !== 0n).map(([accountId, a]) => ({ accountId, amount: a.toString() })),
  };
}

export const postingKey = (opId: string, purpose: string) => `ps:${opId}:${purpose}`;

// Writes the exact request before the first attempt. Retries resend this row, never a rebuilt one.
export async function preparePosting(
  ctx: Ctx,
  op: Op,
  purpose: string,
  build: () => Promise<{ entries: Entries; description: string }>,
): Promise<void> {
  const key = postingKey(op.id, purpose);
  const seen = await ctx.pool.query("select 1 from ledger_postings where key = $1", [key]);
  if (seen.rowCount) return;
  const { entries, description } = await build();
  await ctx.pool.query(
    `insert into ledger_postings (key, operation_id, purpose, body, state) values ($1, $2, $3, $4, 'pending') on conflict (key) do nothing`,
    [key, op.id, purpose, JSON.stringify(bodyFor(entries, description))],
  );
}

export async function sendPosting(ctx: Ctx, op: Op, purpose: string): Promise<PostOutcome> {
  const key = postingKey(op.id, purpose);
  const { rows } = await ctx.pool.query<{ state: string; body: { entries: []; description: string }; ledger_txn_id: string; rejection: string }>(
    "select state, body, ledger_txn_id, rejection from ledger_postings where key = $1",
    [key],
  );
  const row = rows[0];
  if (row.state === "posted") return { kind: "posted", txnId: row.ledger_txn_id };
  if (row.state === "rejected") return { kind: "rejected", code: row.rejection, message: "" };
  try {
    return await ctx.ledger.post(key, row.body);
  } catch (err) {
    if (err instanceof Unknown) throw new Retry(err.message);
    throw err;
  }
}

export async function recordPosting(c: Client, op: Op, purpose: string, outcome: PostOutcome): Promise<void> {
  const key = postingKey(op.id, purpose);
  if (outcome.kind === "posted") {
    await c.query("update ledger_postings set state = 'posted', ledger_txn_id = $2, posted_at = now() where key = $1", [key, outcome.txnId]);
  } else {
    await c.query("update ledger_postings set state = 'rejected', rejection = $2 where key = $1", [key, outcome.code]);
  }
}

async function networkCall<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (err) {
    if (err instanceof Unknown) throw new Retry(err.message);
    throw err;
  }
}

async function payment(ctx: Ctx, id: string): Promise<Payment> {
  const { rows } = await ctx.pool.query<Payment>("select * from payments where id = $1", [id]);
  return rows[0];
}

async function addSettlementItem(
  c: Client,
  item: { ref: string; type: string; merchantId: string; paymentId: string; amount: bigint; shard: number; ledgerTxn: string; networkAt: Date | string },
): Promise<void> {
  await c.query(
    `insert into settlement_items (ref, type, merchant_id, payment_id, amount, shard, ledger_txn_id, network_at)
     values ($1,$2,$3,$4,$5,$6,$7,$8)`,
    [item.ref, item.type, item.merchantId, item.paymentId, item.amount.toString(), item.shard, item.ledgerTxn, item.networkAt],
  );
}

// --- Authorize -------------------------------------------------------------------------------------------------

const authorize: Record<string, Step> = {
  // Retransmitting the same ref is safe: the network replays its original answer. Past the deadline, stop asking and
  // reverse. That covers both "approved but the response was lost" and "still in flight".
  async network(ctx, op, lease) {
    const p = await payment(ctx, op.payment_id!);
    failpoint("authorize.network.before");
    let reply;
    try {
      reply = await ctx.network.auth({ ref: p.auth_ref, merchantId: p.merchant_id, cardToken: p.card_token, amount: p.amount_authorized.toString() });
    } catch (err) {
      if (!(err instanceof Unknown)) throw err;
      if (op.deadline_passed) return setStep(ctx, op, lease, "reverse");
      throw new Retry(err.message);
    }
    failpoint("authorize.network.after");
    if (reply.status === "approved") return setStep(ctx, op, lease, "open_hold", reply.appliedAt);
    await finish(ctx, op, lease, { state: "failed", error: "card_declined", httpStatus: 402 }, async (c, pay) => {
      pay.status = transition(pay.status, "decline");
      await savePayment(c, pay);
      await emit(c, pay, "payment.declined", { reason: reply.reason });
    });
  },

  async reverse(ctx, op, lease) {
    const p = await payment(ctx, op.payment_id!);
    await networkCall(() =>
      ctx.network.void({ ref: `rev:${op.id}`, authRef: p.auth_ref, merchantId: p.merchant_id, amount: p.amount_authorized.toString() }),
    );
    failpoint("authorize.reverse.after");
    // 402, not 504: this outcome is final, and a merchant's client treats any 5xx as "safe to retry".
    await finish(ctx, op, lease, { state: "failed", error: "network_timeout", httpStatus: 402 }, async (c, pay) => {
      pay.status = transition(pay.status, "fail");
      await savePayment(c, pay);
      await emit(c, pay, "payment.failed", { reason: "network_timeout" });
    });
  },

  async open_hold(ctx, op, lease) {
    let p = await payment(ctx, op.payment_id!);
    failpoint("authorize.open_hold.before");
    if (!p.hold_account_id) {
      let holdId: string;
      try {
        holdId = await ctx.ledger.createAccount(`hold:${p.id}`, false);
      } catch (err) {
        if (err instanceof Unknown) throw new Retry(err.message);
        throw err;
      }
      const { rowCount } = await ctx.pool.query(
        `with fenced as (select 1 from operations where id = $1 and lease_token = $2 for update)
         update payments set hold_account_id = $4 where id = $3 and exists (select 1 from fenced)`,
        [op.id, lease.token.toString(), p.id, holdId],
      );
      if (!rowCount) throw new Fenced(op.id);
      p = { ...p, hold_account_id: holdId };
    }
    const contra = await chartId(ctx, accountNames.holdContra(shardOf(p.id, ctx.cfg.shards)));
    await preparePosting(ctx, op, "open_hold", async () => ({
      description: `authorize ${p.id}`,
      entries: [
        [p.hold_account_id!, p.amount_authorized],
        [contra, -p.amount_authorized],
      ],
    }));
    const outcome = await sendPosting(ctx, op, "open_hold");
    failpoint("authorize.open_hold.after");
    if (outcome.kind === "rejected") return stick(ctx, op, lease, `open_hold rejected: ${outcome.code} ${outcome.message}`);
    await finish(ctx, op, lease, { state: "succeeded", httpStatus: 201 }, async (c, pay) => {
      await recordPosting(c, op, "open_hold", outcome);
      pay.status = transition(pay.status, "approve");
      await savePayment(c, pay);
      await emit(c, pay, "payment.authorized");
    });
  },
};

export async function chartId(ctx: Ctx, name: string): Promise<string> {
  try {
    return await ctx.chart.id(name);
  } catch (err) {
    if (err instanceof Unknown) throw new Retry(err.message);
    throw err;
  }
}

// --- Capture ---------------------------------------------------------------------------------------------------

const capture: Record<string, Step> = {
  // Network first: the books record what happened. If the ledger is down afterwards, we retry until it isn't.
  async network(ctx, op, lease) {
    const p = await payment(ctx, op.payment_id!);
    failpoint("capture.network.before");
    const reply = await networkCall(() =>
      ctx.network.capture({ ref: op.network_ref!, authRef: p.auth_ref, amount: op.amount!.toString(), final: op.final_capture }),
    );
    failpoint("capture.network.after");
    if (reply.status === "approved") return setStep(ctx, op, lease, "post", reply.appliedAt);
    await finish(ctx, op, lease, { state: "failed", error: "capture_declined", httpStatus: 402 }, async (c, pay) => {
      pay.reserved_capture -= op.amount!;
      await savePayment(c, pay);
      await emit(c, pay, "capture.failed", { reason: reply.reason, amount: op.amount!.toString() });
    });
  },

  async post(ctx, op, lease) {
    const p = await payment(ctx, op.payment_id!);
    const shard = shardOf(p.id, ctx.cfg.shards);
    failpoint("capture.post.before");
    await preparePosting(ctx, op, "capture", async () => {
      const c = op.amount!;
      // `remaining` already excludes this op's reservation.
      const release = op.final_capture ? remaining(p) : 0n;
      return {
        description: `capture ${p.id}`,
        entries: [
          [p.hold_account_id!, -(c + release)],
          [await chartId(ctx, accountNames.holdContra(shard)), c + release],
          [await chartId(ctx, accountNames.pending(p.merchant_id)), c],
          [await chartId(ctx, accountNames.receivable(shard)), -c],
        ],
      };
    });
    const outcome = await sendPosting(ctx, op, "capture");
    failpoint("capture.post.after");
    // The network already took this capture, so the books can't veto it. Reaching here means a bug: stop and alert.
    if (outcome.kind === "rejected") return stick(ctx, op, lease, `capture rejected by ledger: ${outcome.code} ${outcome.message}`);
    await finish(ctx, op, lease, { state: "succeeded", httpStatus: 201 }, async (c, pay) => {
      await recordPosting(c, op, "capture", outcome);
      const amount = op.amount!;
      pay.reserved_capture -= amount;
      const release = op.final_capture ? remaining(pay) - amount : 0n;
      pay.amount_captured += amount;
      pay.amount_released += release;
      const fully = op.final_capture || remaining(pay) === 0n;
      pay.status = transition(pay.status, fully ? "capture_full" : "capture_partial");
      await savePayment(c, pay);
      await addSettlementItem(c, {
        ref: op.network_ref!,
        type: "capture",
        merchantId: pay.merchant_id,
        paymentId: pay.id,
        amount,
        shard,
        ledgerTxn: outcome.txnId,
        networkAt: op.network_at!,
      });
      await emit(c, pay, fully ? "payment.captured" : "payment.partially_captured", { amount: amount.toString() });
    });
  },
};

// --- Void / expiry ---------------------------------------------------------------------------------------------

function voidSteps(event: PaymentEvent, label: "void" | "expire"): Record<string, Step> {
  return {
    async network(ctx, op, lease) {
      const p = await payment(ctx, op.payment_id!);
      await networkCall(() =>
        ctx.network.void({ ref: op.network_ref!, authRef: p.auth_ref, merchantId: p.merchant_id, amount: p.amount_authorized.toString() }),
      );
      failpoint("void.network.after");
      await setStep(ctx, op, lease, "post");
    },

    async post(ctx, op, lease) {
      const p = await payment(ctx, op.payment_id!);
      const release = remaining(p);
      const contra = await chartId(ctx, accountNames.holdContra(shardOf(p.id, ctx.cfg.shards)));
      await preparePosting(ctx, op, "release", async () => ({
        description: `${label} ${p.id}`,
        entries: [
          [p.hold_account_id!, -release],
          [contra, release],
        ],
      }));
      const outcome = await sendPosting(ctx, op, "release");
      failpoint("void.post.after");
      if (outcome.kind === "rejected") return stick(ctx, op, lease, `release rejected: ${outcome.code} ${outcome.message}`);
      await finish(ctx, op, lease, { state: "succeeded", httpStatus: 200 }, async (c, pay) => {
        await recordPosting(c, op, "release", outcome);
        pay.amount_released += remaining(pay);
        pay.status = transition(pay.status, event);
        await savePayment(c, pay);
        await emit(c, pay, label === "void" ? "payment.voided" : "payment.expired");
      });
    },
  };
}

// --- Refund ----------------------------------------------------------------------------------------------------

// The one flow that posts first. It has a real funds check, so if the merchant can't cover it nothing leaves the
// building. A rejection is recorded and final: this op never re-sends that posting, whatever the merchant's balance
// does next. That's our answer to the ledger not storing rejected keys.
const refund: Record<string, Step> = {
  async post(ctx, op, lease) {
    const p = await payment(ctx, op.payment_id!);
    const shard = shardOf(p.id, ctx.cfg.shards);
    failpoint("refund.post.before");
    await preparePosting(ctx, op, "refund", async () => ({
      description: `refund ${p.id}`,
      entries: [
        [await chartId(ctx, accountNames.available(p.merchant_id)), -op.amount!],
        [await chartId(ctx, accountNames.receivable(shard)), op.amount!],
      ],
    }));
    const outcome = await sendPosting(ctx, op, "refund");
    failpoint("refund.post.after");
    if (outcome.kind === "rejected") {
      if (outcome.code !== "insufficient_funds") return stick(ctx, op, lease, `refund rejected: ${outcome.code} ${outcome.message}`);
      return finish(ctx, op, lease, { state: "failed", error: "insufficient_merchant_funds", httpStatus: 422 }, async (c, pay) => {
        await recordPosting(c, op, "refund", outcome);
        pay.reserved_refund -= op.amount!;
        await savePayment(c, pay);
        await emit(c, pay, "refund.failed", { reason: "insufficient_merchant_funds", amount: op.amount!.toString() });
      });
    }
    await tx(ctx.pool, async (c) => {
      await fence(c, op.id, lease);
      await recordPosting(c, op, "refund", outcome);
      await c.query("update operations set step = 'network' where id = $1", [op.id]);
    });
  },

  async network(ctx, op, lease) {
    const p = await payment(ctx, op.payment_id!);
    const reply = await networkCall(() => ctx.network.refund({ ref: op.network_ref!, authRef: p.auth_ref, amount: op.amount!.toString() }));
    failpoint("refund.network.after");
    if (reply.status !== "approved") return setStep(ctx, op, lease, "reverse_post");
    const { rows } = await ctx.pool.query<{ ledger_txn_id: string }>("select ledger_txn_id from ledger_postings where key = $1", [postingKey(op.id, "refund")]);
    await finish(ctx, op, lease, { state: "succeeded", httpStatus: 201 }, async (c, pay) => {
      const amount = op.amount!;
      pay.reserved_refund -= amount;
      pay.amount_refunded += amount;
      const full = pay.amount_refunded === pay.amount_captured;
      pay.status = transition(pay.status, full ? "refund_full" : "refund_partial");
      await savePayment(c, pay);
      await addSettlementItem(c, {
        ref: op.network_ref!,
        type: "refund",
        merchantId: pay.merchant_id,
        paymentId: pay.id,
        amount,
        shard: shardOf(pay.id, ctx.cfg.shards),
        ledgerTxn: rows[0].ledger_txn_id,
        networkAt: reply.appliedAt,
      });
      await emit(c, pay, full ? "payment.refunded" : "payment.partially_refunded", { amount: amount.toString() });
    });
  },

  // The network refused after we debited the merchant. Undo our posting with the ledger's own reversal.
  async reverse_post(ctx, op, lease) {
    const { rows } = await ctx.pool.query<{ ledger_txn_id: string }>("select ledger_txn_id from ledger_postings where key = $1", [postingKey(op.id, "refund")]);
    const original = rows[0].ledger_txn_id;
    const key = postingKey(op.id, "refund_reversal");
    const description = `refund declined, reversing ${original}`;
    await ctx.pool.query(
      `insert into ledger_postings (key, operation_id, purpose, body, reverses_txn, state) values ($1,$2,'refund_reversal',$3,$4,'pending') on conflict (key) do nothing`,
      [key, op.id, JSON.stringify({ description }), original],
    );
    let outcome: PostOutcome;
    try {
      outcome = await ctx.ledger.reverse(original, key, description);
    } catch (err) {
      if (err instanceof Unknown) throw new Retry(err.message);
      throw err;
    }
    failpoint("refund.reverse_post.after");
    if (outcome.kind === "rejected") return stick(ctx, op, lease, `refund reversal rejected: ${outcome.code} ${outcome.message}`);
    await finish(ctx, op, lease, { state: "failed", error: "refund_declined", httpStatus: 402 }, async (c, pay) => {
      await recordPosting(c, op, "refund_reversal", outcome);
      pay.reserved_refund -= op.amount!;
      await savePayment(c, pay);
      await emit(c, pay, "refund.failed", { reason: "network_declined", amount: op.amount!.toString() });
    });
  },
};

// --- Chargebacks -----------------------------------------------------------------------------------------------

// A chargeback is a fact the network imposes, so it can't be rejected. dispute_debt allows negative balances for that
// reason. It's kept apart from `available` because the overdraft flag is per account: refunds must not overdraw.
const chargebackOpen: Record<string, Step> = {
  async post(ctx, op, lease) {
    const p = await payment(ctx, op.payment_id!);
    const shard = shardOf(p.id, ctx.cfg.shards);
    await preparePosting(ctx, op, "chargeback", async () => ({
      description: `chargeback ${op.dispute_id} on ${p.id}`,
      entries: [
        [await chartId(ctx, accountNames.debt(p.merchant_id)), -op.amount!],
        [await chartId(ctx, accountNames.receivable(shard)), op.amount!],
      ],
    }));
    const outcome = await sendPosting(ctx, op, "chargeback");
    failpoint("chargeback.post.after");
    if (outcome.kind === "rejected") return stick(ctx, op, lease, `chargeback rejected: ${outcome.code} ${outcome.message}`);
    await finish(ctx, op, lease, { state: "succeeded", httpStatus: 200 }, async (c, pay) => {
      await recordPosting(c, op, "chargeback", outcome);
      pay.amount_disputed += op.amount!;
      await savePayment(c, pay);
      await addSettlementItem(c, {
        ref: op.network_ref!,
        type: "chargeback",
        merchantId: pay.merchant_id,
        paymentId: pay.id,
        amount: op.amount!,
        shard,
        ledgerTxn: outcome.txnId,
        networkAt: op.network_at!,
      });
      await emit(c, pay, "dispute.opened", { disputeId: op.dispute_id, amount: op.amount!.toString() });
    });
  },
};

const chargebackWon: Record<string, Step> = {
  async post(ctx, op, lease) {
    const p = await payment(ctx, op.payment_id!);
    const shard = shardOf(p.id, ctx.cfg.shards);
    await preparePosting(ctx, op, "chargeback_won", async () => ({
      description: `chargeback ${op.dispute_id} won on ${p.id}`,
      entries: [
        [await chartId(ctx, accountNames.debt(p.merchant_id)), op.amount!],
        [await chartId(ctx, accountNames.receivable(shard)), -op.amount!],
      ],
    }));
    const outcome = await sendPosting(ctx, op, "chargeback_won");
    failpoint("chargeback.post.after");
    if (outcome.kind === "rejected") return stick(ctx, op, lease, `chargeback reversal rejected: ${outcome.code} ${outcome.message}`);
    await finish(ctx, op, lease, { state: "succeeded", httpStatus: 200 }, async (c, pay) => {
      await recordPosting(c, op, "chargeback_won", outcome);
      pay.amount_disputed -= op.amount!;
      await savePayment(c, pay);
      await c.query("update disputes set state = 'won' where id = $1", [op.dispute_id]);
      await addSettlementItem(c, {
        ref: op.network_ref!,
        type: "chargeback_reversal",
        merchantId: pay.merchant_id,
        paymentId: pay.id,
        amount: op.amount!,
        shard,
        ledgerTxn: outcome.txnId,
        networkAt: op.network_at!,
      });
      await emit(c, pay, "dispute.won", { disputeId: op.dispute_id, amount: op.amount!.toString() });
    });
  },
};

// --- Merchant reserve ------------------------------------------------------------------------------------------

// For ops with no payment. Same fencing, no payment row to update.
export async function finishBare(
  ctx: Ctx,
  op: Op,
  lease: Lease,
  httpStatus: number,
  body: unknown,
  apply: (c: Client) => Promise<void>,
): Promise<void> {
  await tx(ctx.pool, async (c) => {
    await fence(c, op.id, lease);
    await apply(c);
    await c.query(
      `update operations set state = 'succeeded', result = $2, finished_at = now(), lease_until = null where id = $1`,
      [op.id, JSON.stringify({ status: httpStatus, body })],
    );
  });
}

// A merchant deposits a reserve. That's cash in, and the money refunds draw on before any capture has settled.
const reserveTopup: Record<string, Step> = {
  async post(ctx, op, lease) {
    await preparePosting(ctx, op, "topup", async () => ({
      description: `reserve top-up for ${op.merchant_id}`,
      entries: [
        [await chartId(ctx, accountNames.available(op.merchant_id)), op.amount!],
        [await chartId(ctx, accountNames.bankCash()), -op.amount!],
      ],
    }));
    const outcome = await sendPosting(ctx, op, "topup");
    if (outcome.kind === "rejected") return stick(ctx, op, lease, `top-up rejected: ${outcome.code} ${outcome.message}`);
    await finishBare(ctx, op, lease, 201, { merchantId: op.merchant_id, amount: op.amount!.toString() }, (c) => recordPosting(c, op, "topup", outcome));
  },
};

// --- The driver ------------------------------------------------------------------------------------------------

export const FLOWS: Record<string, Record<string, Step>> = {
  // settlement registers itself from recon.ts

  authorize,
  capture,
  void: voidSteps("void", "void"),
  expire: voidSteps("expire", "expire"),
  refund,
  chargeback_open: chargebackOpen,
  chargeback_won: chargebackWon,
  reserve_topup: reserveTopup,
};

async function claim(ctx: Ctx, opId: string): Promise<Lease | null> {
  const { rows } = await ctx.pool.query<{ lease_token: bigint }>(
    `update operations set lease_token = lease_token + 1, lease_until = now() + ($2 || ' milliseconds')::interval, attempts = attempts + 1
      where id = $1 and state = 'in_progress' and (lease_until is null or lease_until < now())
      returning lease_token`,
    [opId, String(ctx.cfg.leaseMs)],
  );
  return rows[0] ? { token: rows[0].lease_token } : null;
}

export async function loadOp(ctx: Pick<Ctx, "pool">, opId: string): Promise<Op> {
  const { rows } = await ctx.pool.query<Op>("select *, coalesce(deadline_at < now(), false) as deadline_passed from operations where id = $1", [opId]);
  return rows[0];
}

export type DriveResult = "finished" | "retry" | "busy";

export async function drive(ctx: Ctx, opId: string): Promise<DriveResult> {
  const lease = await claim(ctx, opId);
  if (!lease) return "busy";
  let renewedAt = performance.now();
  try {
    for (;;) {
      const op = await loadOp(ctx, opId);
      if (op.state !== "in_progress") return "finished";
      // A renewal is a commit, so skip it while most of the lease is left. Every write is fenced either way: this only
      // changes how soon another driver may take over, never whether a driver that lost its lease can write.
      if (performance.now() - renewedAt > ctx.cfg.leaseMs / 3) {
        renewedAt = performance.now();
        const renewed = await ctx.pool.query(
          `update operations set lease_until = now() + ($3 || ' milliseconds')::interval where id = $1 and lease_token = $2`,
          [opId, lease.token.toString(), String(ctx.cfg.leaseMs)],
        );
        if (!renewed.rowCount) return "busy";
      }
      const step = FLOWS[op.kind]?.[op.step];
      if (!step) throw new Error(`no step ${op.kind}.${op.step}`);
      await step(ctx, op, lease);
    }
  } catch (err) {
    if (err instanceof Fenced) return "busy";
    const expected = err instanceof Retry || isConnectionError(err);
    if (!expected) console.error(`[engine] op ${opId}: unexpected error`, err);
    // Exponential backoff with jitter. An unreachable database here just means the lease runs out instead.
    const { attempts } = await loadOp(ctx, opId).catch(() => ({ attempts: 1 }));
    const delay = Math.min(ctx.cfg.retryMaxMs, ctx.cfg.retryBaseMs * 2 ** Math.min(attempts, 16)) * (0.5 + Math.random() / 2);
    await ctx.pool
      .query(
        `update operations set next_attempt_at = now() + ($3 || ' milliseconds')::interval, lease_until = null
          where id = $1 and lease_token = $2`,
        [opId, lease.token.toString(), String(Math.floor(delay))],
      )
      .catch(() => {});
    return "retry";
  }
}

function isConnectionError(err: unknown): boolean {
  const code = (err as { code?: string }).code ?? "";
  return code.startsWith("ECONN") || code.startsWith("57") || code.startsWith("08") || code === "EPIPE";
}

// Picks up everything that's due: fresh retries and work whose driver died.
export async function sweep(ctx: Ctx, limit = 100): Promise<number> {
  const { rows } = await ctx.pool.query<{ id: string }>(
    `select id from operations
      where state = 'in_progress' and next_attempt_at <= now() and (lease_until is null or lease_until < now())
      order by next_attempt_at limit $1`,
    [limit],
  );
  await Promise.all(rows.map((r) => drive(ctx, r.id)));
  return rows.length;
}
