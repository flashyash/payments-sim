import { randomUUID } from "node:crypto";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { accountNames } from "../src/chart.ts";
import { drive, sweep } from "../src/engine.ts";
import { checkInvariants } from "../src/invariants.ts";
import { client, fundMerchant, holdBalance, ledgerBalance, quiesce, sleep, startStack, type Stack } from "./helpers.ts";

let s: Stack;
let api: ReturnType<typeof client>;
let realLedgerUrl: string;

beforeAll(async () => {
  s = await startStack({ leaseMs: 100, authDeadlineMs: 30_000 }); // lease churn is the point here, not the deadline path
  api = client(s);
  realLedgerUrl = s.ctx.ledger.url;
});
afterAll(() => s.close());
afterEach(() => {
  s.resetFaults();
  s.ctx.ledger.url = realLedgerUrl;
});

const opOf = async (paymentId: string, kind: string) =>
  (await s.ctx.pool.query<{ id: string; state: string; step: string; error_code: string | null }>("select id, state, step, error_code from operations where payment_id = $1 and kind = $2", [paymentId, kind])).rows[0];

describe("recovery", () => {
  it("a driver that lost its lease can't finish the op; the new owner does, and it happens once", async () => {
    const a = await api.authorize("1000");
    // Every network call outlives the 100 ms lease, so drivers keep taking the op from each other.
    s.faults({ latencyMs: [250, 250] });
    const capturing = api.capture(a.body.id, { amount: "1000" });
    await sleep(120);
    // The first driver's lease has run out while its network call is in flight. Make the op due, so a sweep takes it.
    await s.ctx.pool.query("update operations set next_attempt_at = now() where payment_id = $1 and kind = 'capture'", [a.body.id]);
    await Promise.all([sweep(s.ctx), sweep(s.ctx)]);
    await capturing;
    s.resetFaults();
    await quiesce(s);
    const after = await api.get(a.body.id);
    expect(after.body.amountCaptured).toBe("1000");
    expect(await holdBalance(s, a.body.id)).toBe(0n);
    const { rows } = await s.netPool.query<{ n: number }>("select count(*)::int as n from messages where auth_ref = $1 and type = 'capture'", [`auth:${a.body.id}`]);
    expect(rows[0].n).toBe(1);
    const lease = await s.ctx.pool.query<{ lease_token: bigint }>("select lease_token from operations where payment_id = $1 and kind = 'capture'", [a.body.id]);
    expect(lease.rows[0].lease_token).toBeGreaterThan(1n);
  });

  it("10 drivers on the same op: one authorization, one hold posting", async () => {
    s.faults({ dropResponseRate: 1 });
    const r = await api.authorize("1000");
    expect(r.status).toBe(202);
    s.resetFaults();
    const op = await opOf(r.body.paymentId, "authorize");
    await sleep(120);
    await Promise.all(Array.from({ length: 10 }, () => drive(s.ctx, op.id)));
    await quiesce(s);
    expect((await api.get(r.body.paymentId)).body.status).toBe("authorized");
    expect(await holdBalance(s, r.body.paymentId)).toBe(1000n);
  });

  it("ledger down after the network took the capture: answers 202, retries, books it once when it returns", async () => {
    const a = await api.authorize("1000");
    const before = await ledgerBalance(s, accountNames.pending(s.merchant));
    s.ctx.ledger.url = "http://127.0.0.1:9"; // nothing listens here
    const c = await api.capture(a.body.id, { amount: "1000" });
    expect(c.status).toBe(202);
    const op = await opOf(a.body.id, "capture");
    expect(op.step).toBe("post"); // the network has it; only the books are behind
    s.ctx.ledger.url = realLedgerUrl;
    await quiesce(s);
    expect((await api.get(a.body.id)).body.status).toBe("captured");
    expect((await ledgerBalance(s, accountNames.pending(s.merchant))) - before).toBe(1000n);
  });

  it("if the ledger refuses a capture the network already took, the op sticks for a human instead of guessing", async () => {
    const a = await api.authorize("1000");
    const { rows } = await s.ctx.pool.query<{ hold_account_id: string }>("select hold_account_id from payments where id = $1", [a.body.id]);
    const contra = await s.ctx.chart.id(accountNames.holdContra(0));
    await s.ctx.ledger.post(`test:${randomUUID()}`, {
      description: "test: drain the hold behind the processor's back",
      entries: [
        { accountId: rows[0].hold_account_id, amount: "-1000" },
        { accountId: contra, amount: "1000" },
      ],
    });
    const c = await api.capture(a.body.id, { amount: "1000" });
    expect(c.status).toBe(202);
    const op = await opOf(a.body.id, "capture");
    expect(op.state).toBe("stuck");
    expect(op.error_code).toMatch(/insufficient_funds/);
    const pools = { processor: s.ctx.pool, network: s.netPool, sink: s.netPool, ledger: s.ledgerPool };
    const report = await checkInvariants(pools, { requireSink: false });
    expect(report.violations.some((v) => v.startsWith("nothing_left_running") && v.includes("stuck"))).toBe(true);
    await s.ctx.pool.query("update operations set state = 'failed' where id = $1", [op.id]); // clean up for later tests
    await s.ctx.pool.query("update payments set reserved_capture = 0 where id = $1", [a.body.id]);
  });

  it("a retry resends the stored posting verbatim, so the ledger never sees a changed body", async () => {
    await fundMerchant(s, "5000");
    const a = await api.authorize("1000");
    await api.capture(a.body.id);
    const { rows } = await s.ctx.pool.query<{ key: string; body: unknown }>(
      "select p.key, p.body from ledger_postings p join operations o on o.id = p.operation_id where o.payment_id = $1 and o.kind = 'capture'",
      [a.body.id],
    );
    // Re-sending the exact stored body is a replay. Anything else would have been a 409.
    const again = await s.ctx.ledger.post(rows[0].key, rows[0].body as never);
    expect(again.kind).toBe("posted");
    const changed = await s.ctx.ledger.post(rows[0].key, { ...(rows[0].body as object), description: "different" } as never);
    expect(changed).toMatchObject({ kind: "rejected", code: "idempotency_key_reused" });
  });

  it("a retry with the same fields in a different order is the same request", async () => {
    const key = randomUUID();
    const body1 = { merchantId: s.merchant, amount: "100", cardToken: "tok_ok" };
    const body2 = { cardToken: "tok_ok", amount: "100", merchantId: s.merchant };
    const r1 = await api.call("POST", "/v1/payments", body1, key);
    const r2 = await api.call("POST", "/v1/payments", body2, key);
    expect(r2.status).toBe(r1.status);
    expect(r2.body).toEqual(r1.body);
  });
});
