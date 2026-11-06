import { createHmac, timingSafeEqual } from "node:crypto";
import type { Ctx } from "./engine.ts";
import { failpoint } from "./failpoints.ts";

// Signature over "<unix seconds>.<raw body>", so a captured request can't be replayed later or have its body changed.
// A retry signs afresh with a new timestamp; the event id stays the same and is what merchants dedupe on.
export const SIGNATURE_HEADER = "x-sim-signature";

export function sign(secret: string, timestamp: number, body: string): string {
  const mac = createHmac("sha256", secret).update(`${timestamp}.${body}`).digest("hex");
  return `t=${timestamp},v1=${mac}`;
}

export function verify(secret: string, header: string | undefined, body: string, toleranceSec = 300, now = Date.now() / 1000): boolean {
  if (!header) return false;
  const match = /^t=(\d+),v1=([0-9a-f]{64})$/.exec(header);
  if (!match) return false;
  const timestamp = Number(match[1]);
  if (Math.abs(now - timestamp) > toleranceSec) return false;
  const expected = createHmac("sha256", secret).update(`${timestamp}.${body}`).digest();
  return timingSafeEqual(expected, Buffer.from(match[2], "hex"));
}

interface Row {
  id: bigint;
  event_id: string;
  merchant_id: string;
  type: string;
  payload: unknown;
  attempts: number;
  webhook_url: string | null;
  webhook_secret: string;
}

// Takes a batch out of the outbox. SKIP LOCKED plus a lease: relays never block each other, and a batch whose relay
// died comes back when the lease runs out. Delivered is only recorded after a 2xx, so a crash between the two means
// a duplicate delivery, never a lost one. That's the "at least once".
export async function claimBatch(ctx: Pick<Ctx, "pool">, limit: number, leaseMs = 15_000): Promise<Row[]> {
  const { rows } = await ctx.pool.query<Row>(
    `with due as (
       select id from outbox
        where delivered_at is null and next_attempt_at <= now() and (lease_until is null or lease_until < now())
        order by id limit $1 for update skip locked
     )
     update outbox o set lease_until = now() + ($2 || ' milliseconds')::interval, attempts = o.attempts + 1
       from due, merchants m
      where o.id = due.id and m.id = o.merchant_id
     returning o.id, o.event_id, o.merchant_id, o.type, o.payload, o.attempts, m.webhook_url, m.webhook_secret`,
    [limit, String(leaseMs)],
  );
  return rows;
}

async function deliver(ctx: Ctx, row: Row): Promise<void> {
  if (!row.webhook_url) {
    await ctx.pool.query("update outbox set delivered_at = now(), last_error = 'merchant has no webhook endpoint' where id = $1", [row.id.toString()]);
    return;
  }
  let failure: string | null = null;
  try {
    const body = JSON.stringify(row.payload);
    failpoint("outbox.send.before");
    const res = await fetch(row.webhook_url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        [SIGNATURE_HEADER]: sign(row.webhook_secret, Math.floor(Date.now() / 1000), body),
        "x-sim-event-id": row.event_id,
      },
      body,
      signal: AbortSignal.timeout(ctx.cfg.webhookTimeoutMs),
    });
    await res.body?.cancel();
    if (!res.ok) failure = `http ${res.status}`;
  } catch (err) {
    failure = (err as Error).name;
  }
  failpoint("outbox.send.after");
  if (failure === null) {
    await ctx.pool.query("update outbox set delivered_at = now(), last_error = null where id = $1", [row.id.toString()]);
    return;
  }
  // Never gives up. The age of the oldest undelivered event is the alarm, not a dead-letter queue nobody reads.
  const delay = Math.min(ctx.cfg.webhookRetryMaxMs, ctx.cfg.webhookRetryBaseMs * 2 ** Math.min(row.attempts, 16)) * (0.5 + Math.random() / 2);
  await ctx.pool.query(
    `update outbox set last_error = $2, lease_until = null, next_attempt_at = now() + ($3 || ' milliseconds')::interval where id = $1`,
    [row.id.toString(), failure, String(Math.floor(delay))],
  );
}

export async function relayOnce(ctx: Ctx, limit = 50): Promise<number> {
  const batch = await claimBatch(ctx, limit);
  await Promise.all(batch.map((row) => deliver(ctx, row)));
  return batch.length;
}
