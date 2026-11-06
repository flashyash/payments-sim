import { randomBytes, randomUUID } from "node:crypto";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { dbUrl } from "../src/config.ts";
import { createPool, type Pool } from "../src/db.ts";
import { buildSink } from "../src/sink/app.ts";
import { claimBatch, relayOnce, sign, verify } from "../src/webhooks.ts";
import { TEST_DBS } from "./global-setup.ts";
import { client, quiesce, sleep, startStack, type Stack } from "./helpers.ts";

let s: Stack;
let api: ReturnType<typeof client>;
let sinkPool: Pool;
let sink: ReturnType<typeof buildSink>;
let merchant: string;
let secret: string;

async function authorize(amount = "1000") {
  return api.call("POST", "/v1/payments", { merchantId: merchant, amount, cardToken: "tok_ok" });
}

async function flush(timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    await relayOnce(s.ctx);
    const { rows } = await s.ctx.pool.query<{ n: number }>("select count(*)::int as n from outbox where delivered_at is null");
    if (rows[0].n === 0) return;
    if (Date.now() > deadline) throw new Error(`${rows[0].n} webhooks undelivered`);
    await sleep(10);
  }
}

beforeAll(async () => {
  s = await startStack();
  api = client(s);
  sinkPool = createPool(dbUrl(TEST_DBS.sink), 5);
  await sinkPool.query("truncate deliveries, secrets, processed_events");
  sink = buildSink(sinkPool);
  const sinkUrl = await sink.app.listen({ port: 0, host: "127.0.0.1" });
  merchant = `wh_${randomUUID().slice(0, 8)}`;
  const res = await api.call("POST", "/v1/merchants", { id: merchant, webhookUrl: `${sinkUrl}/hooks/${merchant}` }, null);
  secret = res.body.webhookSecret;
  await fetch(`${sinkUrl}/admin/merchants`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ id: merchant, secret }),
  });
});
afterAll(async () => {
  await sink.app.close();
  await sinkPool.end();
  await s.close();
});
afterEach(() => {
  Object.assign(sink.faults, { failRate: 0, dropAfterProcessRate: 0, latencyMs: 0, down: false });
  s.resetFaults();
});

describe("signatures", () => {
  const key = randomBytes(16).toString("hex");
  const now = 1_800_000_000;
  const body = '{"a":1}';

  it("accepts a good signature", () => {
    expect(verify(key, sign(key, now, body), body, 300, now)).toBe(true);
  });
  it("rejects a changed body, a wrong key, a stale timestamp and garbage", () => {
    expect(verify(key, sign(key, now, body), '{"a":2}', 300, now)).toBe(false);
    expect(verify("other", sign(key, now, body), body, 300, now)).toBe(false);
    expect(verify(key, sign(key, now - 301, body), body, 300, now)).toBe(false);
    expect(verify(key, sign(key, now + 301, body), body, 300, now)).toBe(false);
    expect(verify(key, "nonsense", body, 300, now)).toBe(false);
    expect(verify(key, undefined, body, 300, now)).toBe(false);
  });
  it("binds the timestamp: moving it invalidates the signature", () => {
    const moved = sign(key, now, body).replace(`t=${now}`, `t=${now + 1}`);
    expect(verify(key, moved, body, 300, now)).toBe(false);
  });
});

describe("outbox delivery", () => {
  it("delivers a signed event for each state change, and the receiver verifies it", async () => {
    const a = await authorize();
    await api.capture(a.body.id);
    await flush();
    const { rows: events } = await s.ctx.pool.query<{ event_id: string; type: string }>("select event_id, type from outbox where payment_id = $1 order by id", [a.body.id]);
    expect(events.map((e) => e.type)).toEqual(["payment.authorized", "payment.captured"]);
    for (const e of events) {
      const { rows } = await sinkPool.query("select valid_sig, response from deliveries where event_id = $1", [e.event_id]);
      expect(rows).toEqual([{ valid_sig: true, response: 200 }]);
    }
  });

  it("the payment change and its event are one transaction: no event, no change", async () => {
    await s.ctx.pool.query(`
      create function fail_outbox() returns trigger language plpgsql as $$ begin raise exception 'outbox unavailable'; end $$;
      create trigger outbox_fail before insert on outbox for each row execute function fail_outbox();`);
    try {
      const a = await authorize();
      expect(a.status).toBe(202);
      const after = await api.get(a.body.paymentId);
      expect(after.body.status).toBe("authorizing"); // state didn't move, because the event couldn't be written
    } finally {
      await s.ctx.pool.query("drop trigger outbox_fail on outbox; drop function fail_outbox();");
    }
    await quiesce(s);
    await flush();
    const { rows } = await s.ctx.pool.query<{ status: string; type: string }>(
      "select p.status, o.type from payments p join outbox o on o.payment_id = p.id where p.merchant_id = $1 and o.type = 'payment.authorized' order by o.id desc limit 1",
      [merchant],
    );
    expect(rows[0].status).toBe("authorized");
  });

  it("keeps retrying through an outage, then delivers", async () => {
    sink.faults.down = true;
    const a = await authorize();
    for (let i = 0; i < 5; i++) {
      await relayOnce(s.ctx);
      await sleep(30);
    }
    const stuck = await s.ctx.pool.query<{ attempts: number; delivered_at: Date | null; last_error: string }>(
      "select attempts, delivered_at, last_error from outbox where payment_id = $1",
      [a.body.id],
    );
    expect(stuck.rows[0].delivered_at).toBeNull();
    expect(stuck.rows[0].attempts).toBeGreaterThan(1);
    expect(stuck.rows[0].last_error).toBe("http 503");
    sink.faults.down = false;
    await flush();
    const { rows } = await s.ctx.pool.query<{ delivered_at: Date | null }>("select delivered_at from outbox where payment_id = $1", [a.body.id]);
    expect(rows[0].delivered_at).not.toBeNull();
  });

  it("a lost response means a duplicate delivery, and the merchant still applies it once", async () => {
    sink.faults.dropAfterProcessRate = 1;
    const a = await authorize();
    await relayOnce(s.ctx);
    await sleep(120);
    await relayOnce(s.ctx);
    sink.faults.dropAfterProcessRate = 0;
    await sleep(120);
    await flush();
    const { rows } = await s.ctx.pool.query<{ event_id: string }>("select event_id from outbox where payment_id = $1", [a.body.id]);
    const deliveries = await sinkPool.query<{ n: number }>("select count(*)::int as n from deliveries where event_id = $1 and response = 200", [rows[0].event_id]);
    const processed = await sinkPool.query<{ n: number }>("select count(*)::int as n from processed_events where event_id = $1", [rows[0].event_id]);
    expect(deliveries.rows[0].n).toBeGreaterThan(1);
    expect(processed.rows[0].n).toBe(1);
  });

  it("two relays never claim the same event", async () => {
    await s.ctx.pool.query("update outbox set delivered_at = now() where delivered_at is null");
    for (let i = 0; i < 10; i++) await authorize("100");
    const [a, b] = await Promise.all([claimBatch(s.ctx, 6), claimBatch(s.ctx, 6)]);
    const ids = [...a, ...b].map((r) => r.event_id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids.length).toBe(10);
    await s.ctx.pool.query("update outbox set lease_until = null where delivered_at is null");
    await flush();
  });

  it("a relay that died mid-batch loses its lease and the events still go out", async () => {
    const a = await authorize();
    await claimBatch(s.ctx, 10, 50); // claimed, then the "relay" vanishes
    await relayOnce(s.ctx);
    const before = await s.ctx.pool.query<{ delivered_at: Date | null }>("select delivered_at from outbox where payment_id = $1", [a.body.id]);
    expect(before.rows[0].delivered_at).toBeNull();
    await sleep(80);
    await flush();
    const after = await s.ctx.pool.query<{ delivered_at: Date | null }>("select delivered_at from outbox where payment_id = $1", [a.body.id]);
    expect(after.rows[0].delivered_at).not.toBeNull();
  });

  it("each event carries a per-payment sequence so merchants can order them", async () => {
    const a = await authorize();
    await api.capture(a.body.id);
    const { rows } = await s.ctx.pool.query<{ seq: number }>("select (payload->>'seq')::int as seq from outbox where payment_id = $1 order by id", [a.body.id]);
    expect(rows.map((r) => r.seq)).toEqual([1, 2]);
    await flush();
  });
});
