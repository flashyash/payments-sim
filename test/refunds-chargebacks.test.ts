import { randomUUID } from "node:crypto";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { accountNames } from "../src/chart.ts";
import { processInbox, receiveNotification } from "../src/inbox.ts";
import { client, drain, fundMerchant, ledgerBalance, quiesce, shardedBalance, startStack, type Stack } from "./helpers.ts";

let s: Stack;
let api: ReturnType<typeof client>;

beforeAll(async () => {
  s = await startStack();
  api = client(s);
});
afterAll(() => s.close());
afterEach(() => s.resetFaults());

const available = () => ledgerBalance(s, accountNames.available(s.merchant));
const debt = () => ledgerBalance(s, accountNames.debt(s.merchant));

async function captured(amount = "1000") {
  const a = await api.authorize(amount);
  const c = await api.capture(a.body.id);
  expect(c.status).toBe(201);
  return a.body.id;
}

async function chargeback(paymentId: string, amount: string) {
  const res = await fetch(`${s.netUrl}/admin/chargebacks`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ authRef: `auth:${paymentId}`, amount }),
  });
  expect(res.status).toBe(201);
  return (await res.json()) as { disputeId: string; ref: string };
}

async function resolve(disputeId: string, won: boolean) {
  const res = await fetch(`${s.netUrl}/admin/chargebacks/${disputeId}/resolve`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ won }),
  });
  expect(res.status).toBe(200);
}

describe("refund", () => {
  it("debits available, credits the receivable, and marks the payment", async () => {
    await fundMerchant(s, "5000");
    const id = await captured("1000");
    const before = await available();
    const recvBefore = await shardedBalance(s, "network_receivable");
    const r = await api.refund(id, "300");
    expect(r.status).toBe(201);
    expect(r.body.status).toBe("partially_refunded");
    expect((await available()) - before).toBe(-300n);
    expect((await shardedBalance(s, "network_receivable")) - recvBefore).toBe(300n);
    const full = await api.refund(id);
    expect(full.body.status).toBe("refunded");
    expect(full.body.amountRefunded).toBe("1000");
  });

  it("can't exceed what was captured, and a fully refunded payment is closed", async () => {
    await fundMerchant(s, "5000");
    const id = await captured("1000");
    expect((await api.refund(id, "1001")).body.error).toBe("amount_exceeds_refundable");
    await api.refund(id);
    expect((await api.refund(id, "1")).body.error).toBe("illegal_transition");
  });

  it("refund before capture is illegal", async () => {
    const a = await api.authorize("1000");
    expect((await api.refund(a.body.id)).body.error).toBe("illegal_transition");
  });

  // The ledger doesn't store rejected keys, so its 422 isn't stable. Ours has to be.
  it("a refund rejected for funds stays rejected under its key, even after funds arrive", async () => {
    const drained = await startFresh();
    const id = await captured("1000");
    const key = randomUUID();
    const r1 = await api.refund(id, "1000", key);
    expect(r1.status).toBe(422);
    expect(r1.body.error).toBe("insufficient_merchant_funds");

    await fundMerchant(s, "5000");
    const r2 = await api.refund(id, "1000", key);
    expect(r2.status).toBe(422);
    expect(r2.body).toEqual(r1.body);
    expect(r2.replayed).toBe(true);
    expect((await available()) - drained).toBe(5000n);

    const { rows } = await s.ctx.pool.query<{ state: string; n: number }>(
      "select p.state from ledger_postings p join operations o on o.id = p.operation_id where o.payment_id = $1 and o.kind = 'refund'",
      [id],
    );
    expect(rows[0].state).toBe("rejected");

    // A new key is a new decision, and now it's fine.
    const r3 = await api.refund(id, "1000");
    expect(r3.status).toBe(201);
  });

  it("a declined refund is undone with a ledger reversal, and the merchant is whole", async () => {
    await fundMerchant(s, "5000");
    const id = await captured("1000");
    const before = await available();
    s.faults({ refundDeclineRate: 1 });
    const r = await api.refund(id, "400");
    expect(r.status).toBe(402);
    expect(r.body.error).toBe("refund_declined");
    expect(await available()).toBe(before);
    expect((await api.get(id)).body.amountRefunded).toBe("0");
    s.resetFaults();
    expect((await api.refund(id, "1000")).status).toBe(201);
  });

  it("lost network response: refund applies exactly once", async () => {
    await fundMerchant(s, "5000");
    const id = await captured("1000");
    const before = await available();
    s.faults({ dropResponseRate: 1 });
    const r = await api.refund(id, "250");
    expect(r.status).toBe(202);
    s.resetFaults();
    await quiesce(s);
    expect((await api.get(id)).body.amountRefunded).toBe("250");
    expect((await available()) - before).toBe(-250n);
    const { rows } = await s.netPool.query<{ n: number }>("select count(*)::int as n from messages where auth_ref = $1 and type = 'refund'", [`auth:${id}`]);
    expect(rows[0].n).toBe(1);
  });

  it("20 concurrent refunds never exceed the captured amount", async () => {
    await fundMerchant(s, "50000");
    const id = await captured("1000");
    const before = await available();
    const results = await Promise.all(Array.from({ length: 20 }, () => api.refund(id, "300")));
    await quiesce(s);
    const wins = results.filter((r) => r.status === 201).length;
    expect(wins).toBeGreaterThanOrEqual(1);
    expect(wins).toBeLessThanOrEqual(3);
    expect((await api.get(id)).body.amountRefunded).toBe(String(wins * 300));
    expect((await available()) - before).toBe(BigInt(-wins * 300));
  });
});

async function startFresh(): Promise<bigint> {
  // Spend whatever earlier tests left in the merchant's reserve, so this test starts at zero available.
  const left = await available();
  if (left > 0n) {
    const cash = await s.ctx.chart.id(accountNames.bankCash());
    const acct = await s.ctx.chart.id(accountNames.available(s.merchant));
    const out = await s.ctx.ledger.post(`test:${randomUUID()}`, {
      description: "test: empty the reserve",
      entries: [
        { accountId: acct, amount: (-left).toString() },
        { accountId: cash, amount: left.toString() },
      ],
    });
    expect(out.kind).toBe("posted");
  }
  return available();
}

describe("chargeback", () => {
  it("opens a dispute, pushes debt negative even with zero reserve, and sends a webhook event", async () => {
    await startFresh();
    const id = await captured("1000");
    const before = await debt();
    await chargeback(id, "1000");
    await drain(s);
    expect((await debt()) - before).toBe(-1000n);
    expect((await api.get(id)).body.amountDisputed).toBe("1000");
    const { rows } = await s.ctx.pool.query("select 1 from outbox where payment_id = $1 and type = 'dispute.opened'", [id]);
    expect(rows.length).toBe(1);
  });

  it("winning restores the merchant; losing keeps the debt", async () => {
    const id1 = await captured("1000");
    const before = await debt();
    const d1 = await chargeback(id1, "1000");
    await drain(s);
    await resolve(d1.disputeId, true);
    await drain(s);
    expect(await debt()).toBe(before);
    expect((await api.get(id1)).body.amountDisputed).toBe("0");

    const id2 = await captured("700");
    const d2 = await chargeback(id2, "700");
    await drain(s);
    await resolve(d2.disputeId, false);
    await drain(s);
    expect((await debt()) - before).toBe(-700n);
    const { rows } = await s.ctx.pool.query<{ state: string }>("select state from disputes where id = $1", [d2.disputeId]);
    expect(rows[0].state).toBe("lost");
  });

  it("a disputed amount can no longer be refunded", async () => {
    await fundMerchant(s, "5000");
    const id = await captured("1000");
    await chargeback(id, "800");
    await drain(s);
    expect((await api.refund(id, "201")).body.error).toBe("amount_exceeds_refundable");
    expect((await api.refund(id, "200")).status).toBe(201);
  });

  it("duplicates are applied once", async () => {
    const id = await captured("1000");
    const before = await debt();
    const disputeId = randomUUID();
    const n = { messageId: randomUUID(), type: "chargeback.opened", payload: { disputeId, authRef: `auth:${id}`, amount: "500", seq: 1, ref: `cb:${disputeId}`, appliedAt: new Date().toISOString() } };
    await receiveNotification(s.ctx, n);
    await receiveNotification(s.ctx, n);
    // Same fact, resent under a different message id.
    await receiveNotification(s.ctx, { ...n, messageId: randomUUID() });
    await processInbox(s.ctx);
    await drain(s);
    expect((await debt()) - before).toBe(-500n);
  });

  it("'won' arriving before 'opened' parks until opened has been applied", async () => {
    const id = await captured("1000");
    const before = await debt();
    const disputeId = randomUUID();
    const base = { disputeId, authRef: `auth:${id}`, amount: "500" };
    const won = { messageId: randomUUID(), type: "chargeback.won", payload: { ...base, seq: 2, ref: `cbr:${disputeId}`, appliedAt: new Date().toISOString() } };
    const opened = { messageId: randomUUID(), type: "chargeback.opened", payload: { ...base, seq: 1, ref: `cb:${disputeId}`, appliedAt: new Date().toISOString() } };
    await receiveNotification(s.ctx, won);
    await processInbox(s.ctx);
    expect(await debt()).toBe(before);
    const parked = await s.ctx.pool.query<{ parked_count: number; state: string }>("select parked_count, state from network_inbox where message_id = $1", [won.messageId]);
    expect(parked.rows[0].state).toBe("pending");
    expect(parked.rows[0].parked_count).toBeGreaterThan(0);

    await receiveNotification(s.ctx, opened);
    await drain(s);
    expect(await debt()).toBe(before);
    const { rows } = await s.ctx.pool.query<{ state: string }>("select state from disputes where id = $1", [disputeId]);
    expect(rows[0].state).toBe("won");
  });

  it("a chargeback for a capture we haven't recorded yet waits for it", async () => {
    const a = await api.authorize("1000");
    const before = await debt();
    const disputeId = randomUUID();
    const n = { messageId: randomUUID(), type: "chargeback.opened", payload: { disputeId, authRef: `auth:${a.body.id}`, amount: "1000", seq: 1, ref: `cb:${disputeId}`, appliedAt: new Date().toISOString() } };
    await receiveNotification(s.ctx, n);
    await processInbox(s.ctx);
    expect(await debt()).toBe(before);
    await api.capture(a.body.id);
    await drain(s);
    expect((await debt()) - before).toBe(-1000n);
  });

  it("out-of-order and duplicate notifications through the real dispatcher still converge", async () => {
    s.faults({ duplicateNotificationRate: 1, reorderWindowMs: 200 });
    const before = await debt();
    const ids: string[] = [];
    for (let i = 0; i < 10; i++) {
      const id = await captured("100");
      ids.push(id);
      const d = await chargeback(id, "100");
      await resolve(d.disputeId, i % 2 === 0);
    }
    await new Promise((r) => setTimeout(r, 250));
    await drain(s);
    expect((await debt()) - before).toBe(-500n);
    const { rows } = await s.ctx.pool.query<{ n: number }>("select count(*)::int as n from disputes where payment_id = any($1) and state in ('won','lost')", [ids]);
    expect(rows[0].n).toBe(10);
  });
});
