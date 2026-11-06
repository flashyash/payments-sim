import { createOp } from "./api.ts";
import { tx } from "./db.ts";
import { drive, sweep, type Ctx } from "./engine.ts";
import { processInbox } from "./inbox.ts";

// Auths nobody captured or voided in time. The hold must not sit on the cardholder's credit forever.
export async function expireDue(ctx: Ctx, limit = 100): Promise<number> {
  const { rows } = await ctx.pool.query<{ id: string; merchant_id: string }>(
    `select p.id, p.merchant_id from payments p
      where p.status in ('authorized','partially_captured') and p.expires_at <= now()
        and not exists (select 1 from operations o where o.payment_id = p.id and o.state = 'in_progress' and o.kind in ('authorize','capture','void','expire','refund'))
      limit $1`,
    [limit],
  );
  let n = 0;
  for (const r of rows) {
    try {
      const op = await tx(ctx.pool, (c) =>
        createOp(c, { paymentId: r.id, merchantId: r.merchant_id, kind: "expire", step: "network", graceless: true }),
      );
      await drive(ctx, op);
      n++;
    } catch (err) {
      if ((err as { constraint?: string }).constraint !== "operations_one_active") throw err;
    }
  }
  return n;
}

// One pass over all background work. Exposed so tests can step time deterministically.
export async function tick(ctx: Ctx): Promise<void> {
  await processInbox(ctx);
  await sweep(ctx);
  await expireDue(ctx);
}

export function startWorker(ctx: Ctx, intervalMs = 100): () => Promise<void> {
  let stopped = false;
  const loop = (async () => {
    while (!stopped) {
      try {
        await tick(ctx);
      } catch (err) {
        // Postgres or a peer is down. Keep trying; that's the worker's whole job.
        const msg = (err as Error).message;
        if (!/ECONN|terminat|connection/i.test(msg)) console.error("[worker]", err);
      }
      await new Promise((r) => setTimeout(r, intervalMs));
    }
  })();
  return async () => {
    stopped = true;
    await loop;
  };
}
