import { randomUUID } from "node:crypto";
import type { Stack } from "./stack.ts";

// A well-behaved merchant integration: same key on every retry, follows 202s to a final answer, keeps going through
// outages. It also remembers what it was told, because an acknowledged capture has to still be true at the end.

export interface Answer {
  status: number;
  body: Record<string, string> & { error?: string; payment?: Record<string, string> };
}

export interface Acks {
  captured: Map<string, bigint>;
  refunded: Map<string, bigint>;
}

export const newAcks = (): Acks => ({ captured: new Map(), refunded: new Map() });

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function send(stack: Stack, method: string, path: string, body: object | undefined, key: string, timeoutMs = 90_000): Promise<Answer> {
  const deadline = Date.now() + timeoutMs;
  let backoff = 25;
  for (;;) {
    if (Date.now() > deadline) throw new Error(`gave up on ${method} ${path} (key ${key})`);
    try {
      const res = await fetch(stack.apiUrl + path, {
        method,
        headers: { "content-type": "application/json", "idempotency-key": key },
        body: JSON.stringify(body ?? {}),
        signal: AbortSignal.timeout(8000),
      });
      const parsed = (await res.json()) as Answer["body"];
      const inFlight = res.status === 409 && parsed.error === "operation_in_progress";
      if (res.status === 202 || res.status >= 500 || inFlight) {
        await sleep(backoff);
        backoff = Math.min(backoff * 2, 500);
        continue;
      }
      return { status: res.status, body: parsed };
    } catch {
      await sleep(backoff);
      backoff = Math.min(backoff * 2, 500);
    }
  }
}

const add = (m: Map<string, bigint>, k: string, a: bigint) => m.set(k, (m.get(k) ?? 0n) + a);

export class Merchant {
  stack: Stack;
  id: string;
  acks: Acks;

  constructor(stack: Stack, id: string, acks: Acks) {
    this.stack = stack;
    this.id = id;
    this.acks = acks;
  }

  authorize(amount: string, card = "tok_ok") {
    return send(this.stack, "POST", "/v1/payments", { merchantId: this.id, amount, cardToken: card }, randomUUID());
  }

  async capture(id: string, amount: string, final = true, key: string = randomUUID()) {
    const r = await send(this.stack, "POST", `/v1/payments/${id}/capture`, { amount, final }, key);
    return r;
  }

  void(id: string) {
    return send(this.stack, "POST", `/v1/payments/${id}/void`, {}, randomUUID());
  }

  refund(id: string, amount: string) {
    return send(this.stack, "POST", `/v1/payments/${id}/refund`, { amount }, randomUUID());
  }

  async chargeback(id: string, amount: string): Promise<string | null> {
    for (let i = 0; i < 200; i++) {
      try {
        const res = await fetch(`${this.stack.networkUrl}/admin/chargebacks`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ authRef: `auth:${id}`, amount }),
          signal: AbortSignal.timeout(3000),
        });
        if (res.status === 201) return ((await res.json()) as { disputeId: string }).disputeId;
        if (res.status === 404) return null;
      } catch {
        // network process down, or a lost response; a retry may open a second dispute, which is still legal
      }
      await sleep(100);
    }
    throw new Error("could not open chargeback");
  }

  async resolve(disputeId: string, won: boolean): Promise<void> {
    for (let i = 0; i < 200; i++) {
      try {
        const res = await fetch(`${this.stack.networkUrl}/admin/chargebacks/${disputeId}/resolve`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ won }),
          signal: AbortSignal.timeout(3000),
        });
        if (res.status === 200 || res.status === 404) return;
      } catch {
        // retry
      }
      await sleep(100);
    }
  }
}

// One tour of every flow. Every merchant-visible success gets written down for the end-of-run audit.
export async function fullFlow(m: Merchant): Promise<void> {
  const ack = (r: Answer, map: Map<string, bigint>, id: string, amount: bigint) => {
    if (r.status === 201) add(map, id, amount);
  };

  // Partial capture, then the rest, then a partial refund.
  const a = await m.authorize("10000");
  if (a.status === 201) {
    const c1 = await m.capture(a.body.id, "4000", false);
    ack(c1, m.acks.captured, a.body.id, 4000n);
    const c2 = await m.capture(a.body.id, "6000", true);
    ack(c2, m.acks.captured, a.body.id, 6000n);
    if (c1.status === 201 || c2.status === 201) {
      ack(await m.refund(a.body.id, "1500"), m.acks.refunded, a.body.id, 1500n);
    }
  }

  // Void.
  const b = await m.authorize("2500");
  if (b.status === 201) await m.void(b.body.id);

  // Partial capture, closed by a void.
  const c = await m.authorize("3000");
  if (c.status === 201) {
    ack(await m.capture(c.body.id, "1000", false), m.acks.captured, c.body.id, 1000n);
    await m.void(c.body.id);
  }

  // A hostile retry pattern: same key twice at once, plus an over-capture and a racing capture under other keys.
  const d = await m.authorize("5000");
  if (d.status === 201) {
    const key = randomUUID();
    const [p, q, over, race] = await Promise.all([
      m.capture(d.body.id, "2000", false, key),
      m.capture(d.body.id, "2000", false, key),
      m.capture(d.body.id, "5001", true),
      m.capture(d.body.id, "4000", false),
    ]);
    if (p.status === 201) add(m.acks.captured, d.body.id, 2000n);
    if (q.status !== p.status) throw new Error(`same key, different answers: ${p.status} and ${q.status}`);
    if (over.status === 201) throw new Error("over-capture was accepted");
    if (race.status === 201) add(m.acks.captured, d.body.id, 4000n);
    await m.void(d.body.id);
  }

  // Decline.
  await m.authorize("1000", `tok_nsf_${randomUUID().slice(0, 8)}`);

  // Chargebacks: one won, one lost.
  for (const won of [true, false]) {
    const e = await m.authorize("2000");
    if (e.status !== 201) continue;
    const cap = await m.capture(e.body.id, "2000", true);
    ack(cap, m.acks.captured, e.body.id, 2000n);
    if (cap.status !== 201) continue;
    const dispute = await m.chargeback(e.body.id, "2000");
    if (dispute) await m.resolve(dispute, won);
  }
}

// Network builds a file and the processor reconciles it, retrying through kills. A retried file is a no-op.
export async function settleOnce(stack: Stack): Promise<{ fileId: string; report: { lines: number; matched: number; exceptions: { ref: string; type: string }[] } }> {
  let fileId: string | undefined;
  for (let i = 0; i < 400 && !fileId; i++) {
    try {
      const res = await fetch(`${stack.networkUrl}/admin/settlement-files`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: "{}",
        signal: AbortSignal.timeout(5000),
      });
      if (res.status === 201) fileId = ((await res.json()) as { id: string }).id;
    } catch {
      await sleep(100);
    }
  }
  if (!fileId) throw new Error("could not get a settlement file");
  for (let i = 0; i < 600; i++) {
    try {
      const res = await fetch(`${stack.apiUrl}/admin/reconcile/${fileId}`, { method: "POST", signal: AbortSignal.timeout(20_000) });
      if (res.status === 200) return { fileId, report: (await res.json()) as { lines: number; matched: number; exceptions: { ref: string; type: string }[] } };
    } catch {
      // API killed mid-run: go round again.
    }
    await sleep(100);
  }
  throw new Error("could not reconcile");
}

export async function setupMerchants(stack: Stack, n: number, prefix: string): Promise<string[]> {
  const ids: string[] = [];
  for (let i = 0; i < n; i++) {
    const id = `${prefix}${i}`;
    const res = await fetch(`${stack.apiUrl}/v1/merchants`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id, webhookUrl: `${stack.sinkUrl}/hooks/${id}` }),
    });
    if (res.status !== 201) throw new Error(`merchant ${id}: ${res.status}`);
    const { webhookSecret } = (await res.json()) as { webhookSecret: string };
    await fetch(`${stack.sinkUrl}/admin/merchants`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id, secret: webhookSecret }),
    });
    await send(stack, "POST", `/v1/merchants/${id}/reserve`, { amount: "100000000" }, randomUUID());
    ids.push(id);
  }
  return ids;
}

// Waits for all work to finish. Returns how long it took, or null if it never did.
export async function quiesce(stack: Stack, timeoutMs = 60_000): Promise<number | null> {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    try {
      const local = await stack.pools.processor.query<{ n: number }>(
        `select (select count(*) from operations where state = 'in_progress')::int
              + (select count(*) from network_inbox where state = 'pending')::int
              + (select count(*) from outbox where delivered_at is null)::int as n`,
      );
      const net = await stack.pools.network.query<{ n: number }>("select count(*)::int as n from notifications where delivered_at is null");
      if (local.rows[0].n === 0 && net.rows[0].n === 0) return Date.now() - started;
    } catch {
      // database restarting
    }
    await sleep(100);
  }
  return null;
}

// What merchants were told has to match the final record, exactly.
export async function auditAcks(stack: Stack, acks: Acks): Promise<string[]> {
  const out: string[] = [];
  const { rows } = await stack.pools.processor.query<{ id: string; amount_captured: bigint; amount_refunded: bigint }>(
    "select id, amount_captured, amount_refunded from payments",
  );
  for (const p of rows) {
    const c = acks.captured.get(p.id) ?? 0n;
    if (c !== p.amount_captured) out.push(`acknowledged_ops: payment ${p.id}: merchant was told ${c} captured, record says ${p.amount_captured}`);
    const r = acks.refunded.get(p.id) ?? 0n;
    if (r !== p.amount_refunded) out.push(`acknowledged_ops: payment ${p.id}: merchant was told ${r} refunded, record says ${p.amount_refunded}`);
  }
  return out;
}
