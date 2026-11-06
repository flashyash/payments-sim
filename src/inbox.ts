import { createOp } from "./api.ts";
import { tx, type Client } from "./db.ts";
import { emit, type Ctx, type Payment } from "./engine.ts";

// Notifications from the network: duplicated and out of order by design. Store first, dedupe on message id,
// resequence on the per-dispute seq. A message that arrives early is parked and looked at again on every pass.

export interface Notification {
  messageId: string;
  type: string;
  payload: Record<string, unknown>;
}

interface ChargebackPayload {
  disputeId: string;
  authRef: string;
  amount: string;
  seq: number;
  ref?: string;
  appliedAt?: string;
}

export async function receiveNotification(ctx: Ctx, n: Notification): Promise<void> {
  await ctx.pool.query(
    `insert into network_inbox (message_id, type, payload, dispute_id, seq) values ($1, $2, $3, $4, $5) on conflict (message_id) do nothing`,
    [n.messageId, n.type, JSON.stringify(n.payload), (n.payload.disputeId as string) ?? null, (n.payload.seq as number) ?? null],
  );
}

type Outcome = "processed" | "ignored" | "parked";

async function applyChargeback(c: Client, type: string, m: ChargebackPayload): Promise<Outcome> {
  await c.query("select pg_advisory_xact_lock(hashtext($1))", [m.disputeId]);
  const { rows: payments } = await c.query<Payment>("select * from payments where auth_ref = $1 for update", [m.authRef]);
  const p = payments[0];
  if (!p) return "parked"; // we haven't heard of this authorization yet
  const { rows: disputes } = await c.query<{ last_seq: number; state: string }>("select last_seq, state from disputes where id = $1", [m.disputeId]);
  const dispute = disputes[0];

  if (type === "chargeback.opened") {
    if (dispute) return "ignored"; // already applied, resent under a new message id
    if (p.amount_captured === 0n) return "parked"; // the capture that this disputes is still being recorded locally
    await c.query("insert into disputes (id, payment_id, amount, state, last_seq) values ($1,$2,$3,'open',1)", [m.disputeId, p.id, m.amount]);
    await createOp(c, {
      paymentId: p.id,
      merchantId: p.merchant_id,
      kind: "chargeback_open",
      step: "post",
      amount: BigInt(m.amount),
      networkRef: m.ref,
      networkAt: m.appliedAt,
      disputeId: m.disputeId,
      graceless: true,
    });
    return "processed";
  }

  // won/lost: only valid straight after "opened", and only once its books are done, so debt can't be undone before it exists.
  if (!dispute) return "parked";
  if (dispute.last_seq >= m.seq) return "ignored";
  const opened = await c.query(
    "select 1 from operations where dispute_id = $1 and kind = 'chargeback_open' and state = 'succeeded'",
    [m.disputeId],
  );
  if (!opened.rowCount) return "parked";
  await c.query("update disputes set last_seq = $2 where id = $1", [m.disputeId, m.seq]);

  if (type === "chargeback.lost") {
    await c.query("update disputes set state = 'lost' where id = $1", [m.disputeId]);
    await emit(c, p, "dispute.lost", { disputeId: m.disputeId, amount: m.amount });
    return "processed";
  }
  await createOp(c, {
    paymentId: p.id,
    merchantId: p.merchant_id,
    kind: "chargeback_won",
    step: "post",
    amount: BigInt(m.amount),
    networkRef: m.ref,
    networkAt: m.appliedAt,
    disputeId: m.disputeId,
    graceless: true,
  });
  return "processed";
}

// Advice is the network telling us an outcome we may have missed. It's a hint, not a fact we act on directly: wake
// the op, which retransmits and gets the authoritative answer. That's why duplicates and reordering can't hurt here.
async function applyAdvice(c: Client, type: string, payload: { ref: string }): Promise<Outcome> {
  if (type === "auth.advice") {
    await c.query(
      `update operations o set next_attempt_at = now() from payments p
        where p.auth_ref = $1 and o.payment_id = p.id and o.kind = 'authorize' and o.state = 'in_progress'`,
      [payload.ref],
    );
  } else {
    await c.query("update operations set next_attempt_at = now() where network_ref = $1 and state = 'in_progress'", [payload.ref]);
  }
  return "processed";
}

export async function processNotification(ctx: Ctx, messageId: string): Promise<Outcome | "gone"> {
  return tx(ctx.pool, async (c) => {
    const { rows } = await c.query<{ type: string; payload: Record<string, unknown>; state: string }>(
      "select type, payload, state from network_inbox where message_id = $1 for update skip locked",
      [messageId],
    );
    if (!rows[0] || rows[0].state !== "pending") return "gone";
    const { type, payload } = rows[0];
    let outcome: Outcome;
    if (type.startsWith("chargeback.")) outcome = await applyChargeback(c, type, payload as unknown as ChargebackPayload);
    else if (type.endsWith(".advice")) outcome = await applyAdvice(c, type, payload as unknown as { ref: string });
    else outcome = "ignored";

    if (outcome === "parked") {
      await c.query("update network_inbox set parked_count = parked_count + 1 where message_id = $1", [messageId]);
    } else {
      await c.query("update network_inbox set state = $2, processed_at = now() where message_id = $1", [messageId, outcome]);
    }
    return outcome;
  });
}

export async function processInbox(ctx: Ctx, limit = 200): Promise<number> {
  const { rows } = await ctx.pool.query<{ message_id: string }>(
    "select message_id from network_inbox where state = 'pending' order by received_at limit $1",
    [limit],
  );
  let done = 0;
  for (const r of rows) {
    const outcome = await processNotification(ctx, r.message_id);
    if (outcome === "processed" || outcome === "ignored") done++;
  }
  return done;
}
