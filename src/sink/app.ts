import Fastify from "fastify";
import type { Pool } from "../db.ts";
import { SIGNATURE_HEADER, verify } from "../webhooks.ts";

// PLUMBING: a fake merchant's webhook endpoint. It checks signatures and dedupes on event id, as a correct merchant
// should, and records every attempt so the harness can audit delivery from the receiving side.

export interface SinkFaults {
  /** Answer 500 without processing. */
  failRate: number;
  /** Process, then break the connection: the sender must retry, and we must not process it twice. */
  dropAfterProcessRate: number;
  latencyMs: number;
  down: boolean;
}

export function buildSink(pool: Pool, initial: Partial<SinkFaults> = {}) {
  const faults: SinkFaults = { failRate: 0, dropAfterProcessRate: 0, latencyMs: 0, down: false, ...initial };
  const app = Fastify({ logger: false });
  app.addContentTypeParser("application/json", { parseAs: "string" }, (_req, body, done) => done(null, body));

  app.get("/health", async () => {
    await pool.query("select 1");
    return { ok: true };
  });

  app.put<{ Body: string }>("/admin/faults", async (req) => {
    Object.assign(faults, JSON.parse(req.body));
    return faults;
  });

  app.post<{ Body: string }>("/admin/merchants", async (req, reply) => {
    const { id, secret } = JSON.parse(req.body) as { id: string; secret: string };
    await pool.query("insert into secrets (merchant_id, secret) values ($1, $2) on conflict (merchant_id) do update set secret = excluded.secret", [id, secret]);
    return reply.status(201).send({ ok: true });
  });

  app.post<{ Params: { merchant: string }; Body: string }>("/hooks/:merchant", async (req, reply) => {
    const merchant = req.params.merchant;
    const eventId = String(req.headers["x-sim-event-id"] ?? "unknown");
    if (faults.latencyMs) await new Promise((r) => setTimeout(r, faults.latencyMs));
    if (faults.down || Math.random() < faults.failRate) {
      await pool.query("insert into deliveries (event_id, merchant_id, valid_sig, response) values ($1,$2,false,503)", [eventId, merchant]);
      return reply.status(503).send({ error: "unavailable" });
    }
    const { rows } = await pool.query<{ secret: string }>("select secret from secrets where merchant_id = $1", [merchant]);
    const signature = req.headers[SIGNATURE_HEADER] as string | undefined;
    const valid = rows.length > 0 && verify(rows[0].secret, signature, req.body);
    if (!valid) {
      await pool.query("insert into deliveries (event_id, merchant_id, valid_sig, response) values ($1,$2,false,401)", [eventId, merchant]);
      return reply.status(401).send({ error: "bad_signature" });
    }
    const event = JSON.parse(req.body) as { type: string };
    await pool.query("insert into processed_events (event_id, merchant_id, type) values ($1,$2,$3) on conflict do nothing", [eventId, merchant, event.type]);
    await pool.query("insert into deliveries (event_id, merchant_id, type, valid_sig, response) values ($1,$2,$3,true,200)", [eventId, merchant, event.type]);
    if (Math.random() < faults.dropAfterProcessRate) {
      reply.hijack();
      req.raw.socket.destroy();
      return;
    }
    return reply.status(200).send({ ok: true });
  });

  return { app, faults };
}
