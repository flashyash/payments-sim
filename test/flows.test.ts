import { randomUUID } from "node:crypto";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { accountNames } from "../src/chart.ts";
import { client, expireAuthDeadline, holdBalance, ledgerBalance, quiesce, shardedBalance, sleep, startStack, type Stack } from "./helpers.ts";

let s: Stack;
let api: ReturnType<typeof client>;

beforeAll(async () => {
  s = await startStack();
  api = client(s);
});
afterAll(() => s.close());
afterEach(() => s.resetFaults());

describe("authorize", () => {
  it("approves, opens the hold in the ledger, and doesn't touch merchant balances", async () => {
    const before = await ledgerBalance(s, accountNames.pending(s.merchant));
    const r = await api.authorize("5000");
    expect(r.status).toBe(201);
    expect(r.body.status).toBe("authorized");
    expect(await holdBalance(s, r.body.id)).toBe(5000n);
    expect(await ledgerBalance(s, accountNames.pending(s.merchant))).toBe(before);
  });

  it("a decline is final and posts nothing", async () => {
    const r = await api.authorize("100", undefined, `tok_nsf_${randomUUID().slice(0, 8)}`);
    expect(r.status).toBe(402);
    expect(r.body.error).toBe("card_declined");
    expect(r.body.payment!.status).toBe("declined");
    const { rows } = await s.ctx.pool.query("select count(*)::int as n from ledger_postings p join operations o on o.id = p.operation_id where o.payment_id = $1", [r.body.payment!.id]);
    expect(rows[0].n).toBe(0);
  });

  it("replays by key: same answer, one payment", async () => {
    const key = randomUUID();
    const first = await api.authorize("700", key);
    const second = await api.authorize("700", key);
    expect(second.status).toBe(first.status);
    expect(second.body).toEqual(first.body);
    expect(second.replayed).toBe(true);
    const { rows } = await s.ctx.pool.query("select count(*)::int as n from idempotency_keys where key = $1", [key]);
    expect(rows[0].n).toBe(1);
  });

  it("same key with a different body is a 409", async () => {
    const key = randomUUID();
    await api.authorize("700", key);
    const r = await api.authorize("701", key);
    expect(r.status).toBe(409);
    expect(r.body.error).toBe("idempotency_key_reused");
  });

  it("the same key fired 20 times concurrently creates one payment", async () => {
    const key = randomUUID();
    const results = await Promise.all(Array.from({ length: 20 }, () => api.authorize("900", key)));
    const ids = new Set(results.filter((r) => r.status === 201).map((r) => r.body.id));
    expect(ids.size).toBe(1);
    await quiesce(s);
    const { rows } = await s.ctx.pool.query(
      "select count(*)::int as n from payments p join idempotency_keys k on true join operations o on o.id = k.operation_id and o.payment_id = p.id where k.key = $1",
      [key],
    );
    expect(rows[0].n).toBe(1);
  });

  it("network approved but the response was lost: returns 202, then resolves to authorized on retransmit", async () => {
    s.faults({ dropResponseRate: 1 });
    const r = await api.authorize("1500");
    expect(r.status).toBe(202);
    const paymentId = r.body.paymentId;
    expect((await api.get(paymentId)).body.status).toBe("authorizing");
    s.resetFaults();
    await quiesce(s);
    const after = await api.get(paymentId);
    expect(after.body.status).toBe("authorized");
    expect(await holdBalance(s, paymentId)).toBe(1500n);
    const { rows } = await s.netPool.query<{ status: string; duplicates: number }>("select status, duplicates from messages where ref = $1", [`auth:${paymentId}`]);
    expect(rows[0].status).toBe("approved");
    expect(rows[0].duplicates).toBeGreaterThan(0);
  });

  it("network unreachable past the deadline: reverses, fails, and leaves no hold at the issuer", async () => {
    s.faults({ dropRequestRate: 1 });
    const card = `tok_low_2000_${randomUUID().slice(0, 8)}`;
    const r = await api.authorize("1500", undefined, card);
    expect(r.status).toBe(202);
    await expireAuthDeadline(s, r.body.paymentId);
    await quiesceOnce(); // the next attempt also gets no answer, so it moves to the reversal step
    const { rows: ops } = await s.ctx.pool.query<{ step: string }>("select step from operations where payment_id = $1", [r.body.paymentId]);
    expect(ops[0].step).toBe("reverse");
    s.resetFaults();
    await quiesce(s);
    expect((await api.get(r.body.paymentId)).body.status).toBe("failed");
    // The reversal arrived first, so it left a tombstone. No hold, and the card row was never touched.
    const { rows } = await s.netPool.query<{ status: string }>("select status from messages where ref = $1", [`auth:${r.body.paymentId}`]);
    expect(rows[0].status).toBe("tombstoned");
  });

  it("approved but we never heard, past the deadline: the reversal releases the issuer's hold", async () => {
    s.faults({ dropResponseRate: 1 }); // applied at the issuer; the response is lost
    const card = `tok_low_2000_${randomUUID().slice(0, 8)}`;
    const r = await api.authorize("1500", undefined, card);
    expect(r.status).toBe(202);
    const held = await s.netPool.query<{ available: bigint }>("select available from cards where token = $1", [card]);
    expect(held.rows[0].available).toBe(500n);
    s.faults({ dropResponseRate: 0, dropRequestRate: 1 }); // the retransmit can't get through either
    await expireAuthDeadline(s, r.body.paymentId);
    await quiesceOnce();
    s.resetFaults();
    await quiesce(s);
    expect((await api.get(r.body.paymentId)).body.status).toBe("failed");
    const { rows } = await s.netPool.query<{ available: bigint }>("select available from cards where token = $1", [card]);
    expect(rows[0].available).toBe(2000n);
  });

  it("a late auth arriving after the reversal is tombstoned, so no orphan hold", async () => {
    s.faults({ lateApplyRate: 1, lateApplyMs: 1500 });
    const card = `tok_low_2000_${randomUUID().slice(0, 8)}`;
    const r = await api.authorize("1500", undefined, card);
    expect(r.status).toBe(202);
    s.faults({ lateApplyRate: 0, dropRequestRate: 1 });
    await expireAuthDeadline(s, r.body.paymentId);
    await quiesceOnce();
    s.resetFaults();
    await quiesce(s);
    expect((await api.get(r.body.paymentId)).body.status).toBe("failed");
    // Wait until the late message has actually been applied, however slowly this machine is running.
    while (s.net.lateInFlight() > 0) await sleep(50);
    const { rows } = await s.netPool.query<{ status: string }>("select status from messages where ref = $1", [`auth:${r.body.paymentId}`]);
    expect(rows[0].status).toBe("tombstoned");
    const { rows: holds } = await s.netPool.query("select 1 from cards where token = $1 and available < 2000", [card]);
    expect(holds).toEqual([]);
  });
});

async function quiesceOnce() {
  const { tick } = await import("../src/worker.ts");
  await tick(s.ctx);
  await sleep(20);
}

describe("capture", () => {
  it("captures in full: moves hold to merchant pending, records a receivable", async () => {
    const pendingBefore = await ledgerBalance(s, accountNames.pending(s.merchant));
    const receivableBefore = await shardedBalance(s, "network_receivable");
    const a = await api.authorize("5000");
    const c = await api.capture(a.body.id);
    expect(c.status).toBe(201);
    expect(c.body.status).toBe("captured");
    expect(await holdBalance(s, a.body.id)).toBe(0n);
    expect((await ledgerBalance(s, accountNames.pending(s.merchant))) - pendingBefore).toBe(5000n);
    expect((await shardedBalance(s, "network_receivable")) - receivableBefore).toBe(-5000n);
  });

  it("partial capture marked final releases the remainder", async () => {
    const a = await api.authorize("5000");
    const c = await api.capture(a.body.id, { amount: "3000" });
    expect(c.body.status).toBe("captured");
    expect(c.body.amountCaptured).toBe("3000");
    expect(c.body.amountReleased).toBe("2000");
    expect(await holdBalance(s, a.body.id)).toBe(0n);
  });

  it("non-final partial captures add up, and the last one completes the payment", async () => {
    const a = await api.authorize("5000");
    const c1 = await api.capture(a.body.id, { amount: "2000", final: false });
    expect(c1.body.status).toBe("partially_captured");
    expect(await holdBalance(s, a.body.id)).toBe(3000n);
    const c2 = await api.capture(a.body.id, { amount: "3000", final: false });
    expect(c2.body.status).toBe("captured");
    expect(await holdBalance(s, a.body.id)).toBe(0n);
  });

  it("over-capture is rejected, and the rejection stays final under the same key", async () => {
    const a = await api.authorize("5000");
    const key = randomUUID();
    const r1 = await api.capture(a.body.id, { amount: "5001" }, key);
    expect(r1.status).toBe(422);
    expect(r1.body.error).toBe("amount_exceeds_authorization");
    const r2 = await api.capture(a.body.id, { amount: "5001" }, key);
    expect(r2.status).toBe(422);
    expect(r2.replayed).toBe(true);
  });

  it("the ledger itself refuses an over-capture, even if local checks were bypassed", async () => {
    const a = await api.authorize("1000");
    const { rows } = await s.ctx.pool.query<{ hold_account_id: string }>("select hold_account_id from payments where id = $1", [a.body.id]);
    const contra = await s.ctx.chart.id(accountNames.holdContra(0));
    const outcome = await s.ctx.ledger.post(`test:${randomUUID()}`, {
      description: "bypass",
      entries: [
        { accountId: rows[0].hold_account_id, amount: "-1001" },
        { accountId: contra, amount: "1001" },
      ],
    });
    expect(outcome).toMatchObject({ kind: "rejected", code: "insufficient_funds" });
  });

  it("20 concurrent captures with different keys capture at most the authorized amount", async () => {
    const a = await api.authorize("1000");
    const results = await Promise.all(Array.from({ length: 20 }, () => api.capture(a.body.id, { amount: "300", final: false })));
    await quiesce(s);
    const wins = results.filter((r) => r.status === 201).length;
    expect(wins).toBeGreaterThanOrEqual(1);
    expect(wins).toBeLessThanOrEqual(3);
    // Everyone else was told why: an op was in flight, the amount no longer fits, or the payment is now fully captured.
    for (const r of results) expect([201, 409, 422]).toContain(r.status);
    const after = await api.get(a.body.id);
    expect(BigInt(after.body.amountCaptured)).toBe(BigInt(wins) * 300n);
    expect(await holdBalance(s, a.body.id)).toBe(1000n - BigInt(wins) * 300n);
  });

  it("the same capture key fired 20 times concurrently captures once", async () => {
    const a = await api.authorize("1000");
    const key = randomUUID();
    await Promise.all(Array.from({ length: 20 }, () => api.capture(a.body.id, { amount: "400" }, key)));
    await quiesce(s);
    const after = await api.get(a.body.id);
    expect(after.body.amountCaptured).toBe("400");
    const { rows } = await s.netPool.query<{ n: number }>("select count(*)::int as n from messages where auth_ref = $1 and type = 'capture'", [`auth:${a.body.id}`]);
    expect(rows[0].n).toBe(1);
  });

  it("network approved the capture but the response was lost: exactly one capture after retries", async () => {
    const a = await api.authorize("1000");
    s.faults({ dropResponseRate: 1 });
    const c = await api.capture(a.body.id, { amount: "400" });
    expect(c.status).toBe(202);
    s.resetFaults();
    await quiesce(s);
    const after = await api.get(a.body.id);
    expect(after.body.amountCaptured).toBe("400");
    const { rows } = await s.netPool.query<{ n: number; d: number }>(
      "select count(*)::int as n, sum(duplicates)::int as d from messages where auth_ref = $1 and type = 'capture'",
      [`auth:${a.body.id}`],
    );
    expect(rows[0].n).toBe(1);
    expect(rows[0].d).toBeGreaterThan(0);
    expect(await holdBalance(s, a.body.id)).toBe(0n);
  });

  it("a network-declined capture frees the reservation so a later one can succeed", async () => {
    const a = await api.authorize("1000");
    s.faults({ captureDeclineRate: 1 });
    const c1 = await api.capture(a.body.id, { amount: "400" });
    expect(c1.status).toBe(402);
    expect(c1.body.error).toBe("capture_declined");
    s.resetFaults();
    const c2 = await api.capture(a.body.id, { amount: "1000" });
    expect(c2.status).toBe(201);
  });

  it("a second op while one is in flight is told to retry, and that answer isn't final", async () => {
    const a = await api.authorize("1000");
    s.faults({ dropResponseRate: 1 });
    await api.capture(a.body.id, { amount: "100", final: false });
    s.resetFaults();
    const key = randomUUID();
    const blocked = await api.capture(a.body.id, { amount: "100", final: false }, key);
    expect(blocked.status).toBe(409);
    expect(blocked.body.error).toBe("operation_in_progress");
    await quiesce(s);
    const retried = await api.capture(a.body.id, { amount: "100", final: false }, key);
    expect(retried.status).toBe(201);
  });
});

describe("void and illegal transitions", () => {
  it("void releases the hold and frees the issuer's credit", async () => {
    const card = `tok_low_1000_${randomUUID().slice(0, 8)}`;
    const a = await api.authorize("1000", undefined, card);
    const v = await api.void(a.body.id);
    expect(v.status).toBe(200);
    expect(v.body.status).toBe("voided");
    expect(await holdBalance(s, a.body.id)).toBe(0n);
    const { rows } = await s.netPool.query<{ available: bigint }>("select available from cards where token = $1", [card]);
    expect(rows[0].available).toBe(1000n);
  });

  it("void after a non-final partial capture closes it as captured", async () => {
    const a = await api.authorize("1000");
    await api.capture(a.body.id, { amount: "400", final: false });
    const v = await api.void(a.body.id);
    expect(v.body.status).toBe("captured");
    expect(v.body.amountCaptured).toBe("400");
    expect(v.body.amountReleased).toBe("600");
    expect(await holdBalance(s, a.body.id)).toBe(0n);
  });

  it("every illegal command returns a stored 409 and changes nothing", async () => {
    const a = await api.authorize("1000");
    await api.void(a.body.id);
    expect((await api.capture(a.body.id)).body.error).toBe("illegal_transition");
    expect((await api.void(a.body.id)).body.error).toBe("illegal_transition");
    const b = await api.authorize("1000");
    await api.capture(b.body.id);
    expect((await api.void(b.body.id)).body.error).toBe("illegal_transition");
    expect((await api.capture(b.body.id)).body.error).toBe("illegal_transition");
    const key = randomUUID();
    const first = await api.void(b.body.id, key);
    const second = await api.void(b.body.id, key);
    expect(second.replayed).toBe(true);
    expect(second.body).toEqual(first.body);
  });

  it("capture races void: exactly one outcome, books agree", async () => {
    for (let i = 0; i < 10; i++) {
      const a = await api.authorize("1000");
      const [c, v] = await Promise.all([api.capture(a.body.id), api.void(a.body.id)]);
      await quiesce(s);
      const after = await api.get(a.body.id);
      expect(["captured", "voided"]).toContain(after.body.status);
      expect(after.body.status === "captured").toBe(c.status === 201);
      expect(after.body.status === "voided").toBe(v.status === 200);
      expect(await holdBalance(s, a.body.id)).toBe(0n);
    }
  });

  it("an expired auth can't be captured, and the sweeper releases it", async () => {
    const a = await api.authorize("1000");
    await s.ctx.pool.query("update payments set expires_at = now() - interval '1 second' where id = $1", [a.body.id]);
    const c = await api.capture(a.body.id);
    expect(c.status).toBe(422);
    expect(c.body.error).toBe("authorization_expired");
    await quiesceOnce();
    await quiesce(s);
    expect((await api.get(a.body.id)).body.status).toBe("expired");
    expect(await holdBalance(s, a.body.id)).toBe(0n);
  });
});

describe("requests", () => {
  it("rejects a missing key, numeric amounts and non-simulator tokens", async () => {
    expect((await api.call("POST", "/v1/payments", { merchantId: s.merchant, amount: "100", cardToken: "tok_ok" }, null)).status).toBe(400);
    expect((await api.call("POST", "/v1/payments", { merchantId: s.merchant, amount: 100, cardToken: "tok_ok" })).status).toBe(400);
    expect((await api.call("POST", "/v1/payments", { merchantId: s.merchant, amount: "100", cardToken: "4111111111111111" })).status).toBe(400);
    expect((await api.call("POST", "/v1/payments", { merchantId: s.merchant, amount: "0", cardToken: "tok_ok" })).status).toBe(400);
  });

  it("a validation failure doesn't burn the key", async () => {
    const key = randomUUID();
    expect((await api.call("POST", "/v1/payments", { merchantId: s.merchant, amount: "abc", cardToken: "tok_ok" }, key)).status).toBe(400);
    expect((await api.authorize("100", key)).status).toBe(201);
  });

  it("the database refuses a capture beyond the authorization, even if the app were wrong", async () => {
    const a = await api.authorize("1000");
    await expect(s.ctx.pool.query("update payments set amount_captured = 1001 where id = $1", [a.body.id])).rejects.toThrow(/check/);
  });
});
