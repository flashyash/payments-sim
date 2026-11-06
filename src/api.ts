import { createHash, randomBytes, randomUUID } from "node:crypto";
import Fastify, { type FastifyError } from "fastify";
import { Unknown } from "./clients.ts";
import { tx, type Client } from "./db.ts";
import { processNotification, receiveNotification } from "./inbox.ts";
import { drive, loadOp, paymentJson, refundable, remaining, type Ctx, type Op, type Payment } from "./engine.ts";
import { failpoint } from "./failpoints.ts";
import { reconcileFile } from "./recon.ts";
import { canAdmit, type Command } from "./stateMachine.ts";

const AMOUNT = { type: "string", pattern: "^[1-9][0-9]{0,17}$" } as const;
const NEW_OP_GRACE = "1 second"; // the worker leaves a fresh op alone for this long, so the API can drive it inline

class HttpError extends Error {
  status: number;
  code: string;
  constructor(status: number, code: string, message: string) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

type Admission = { op: string; fresh: boolean } | { stored: { status: number; body: unknown }; replayed: boolean };

// Admit a merchant request exactly once. Key insert, state check and op creation are one local transaction. A retry
// blocks on the key row until the first attempt commits, then sees its operation or its stored rejection. Business
// rejections are stored and stay final, which is the guarantee the ledger's own idempotency doesn't give us.
async function admit(
  ctx: Ctx,
  merchantId: string,
  key: string,
  hash: string,
  decide: (c: Client) => Promise<string | { status: number; body: unknown }>,
): Promise<Admission> {
  return tx(ctx.pool, async (c) => {
    const claimed = await c.query(
      "insert into idempotency_keys (merchant_id, key, request_hash) values ($1, $2, $3) on conflict do nothing",
      [merchantId, key, hash],
    );
    if (!claimed.rowCount) {
      const { rows } = await c.query<{ request_hash: string; operation_id: string | null; response_status: number | null; response_body: unknown }>(
        "select request_hash, operation_id, response_status, response_body from idempotency_keys where merchant_id = $1 and key = $2",
        [merchantId, key],
      );
      if (rows[0].request_hash !== hash) throw new HttpError(409, "idempotency_key_reused", "this key was used with a different request");
      if (rows[0].response_status) return { stored: { status: rows[0].response_status, body: rows[0].response_body }, replayed: true };
      return { op: rows[0].operation_id!, fresh: false };
    }
    const decision = await decide(c);
    if (typeof decision === "string") {
      await c.query("update idempotency_keys set operation_id = $3 where merchant_id = $1 and key = $2", [merchantId, key, decision]);
      return { op: decision, fresh: true };
    }
    await c.query("update idempotency_keys set response_status = $3, response_body = $4 where merchant_id = $1 and key = $2", [
      merchantId,
      key,
      decision.status,
      JSON.stringify(decision.body),
    ]);
    return { stored: decision, replayed: false };
  });
}

// Request bodies are flat, so sorting top-level keys is enough for a retry with reordered fields to hash the same.
const requestHash = (route: string, body: unknown) =>
  createHash("sha256").update(route + JSON.stringify(body, Object.keys(body as object).sort())).digest("hex");

export async function createOp(
  c: Client,
  o: {
    paymentId: string | null;
    merchantId: string;
    kind: string;
    step: string;
    amount?: bigint;
    finalCapture?: boolean;
    deadlineMs?: number;
    networkRef?: boolean | string; // false = none, a string = the network's own ref (chargebacks)
    networkAt?: string;
    disputeId?: string;
    graceless?: boolean;
  },
): Promise<string> {
  const id = randomUUID();
  await c.query(
    `insert into operations (id, payment_id, merchant_id, kind, step, state, amount, final_capture, network_ref, deadline_at, dispute_id, next_attempt_at, network_at)
     values ($1,$2,$3,$4,$5,'in_progress',$6,$7,$8, now() + ($9 || ' milliseconds')::interval, $10, now() + ($11)::interval, $12)`,
    [
      id,
      o.paymentId,
      o.merchantId,
      o.kind,
      o.step,
      o.amount?.toString() ?? null,
      o.finalCapture ?? false,
      o.networkRef === false ? null : typeof o.networkRef === "string" ? o.networkRef : `${o.kind}:${id}`,
      o.deadlineMs ?? null,
      o.disputeId ?? null,
      o.graceless ? "0 seconds" : NEW_OP_GRACE,
      o.networkAt ?? null,
    ],
  );
  return id;
}

export function buildApi(ctx: Ctx, opts: { syncWaitMs?: number } = {}) {
  const app = Fastify({ logger: false, ajv: { customOptions: { coerceTypes: false, removeAdditional: false } } });
  const syncWaitMs = opts.syncWaitMs ?? 3000;

  app.setErrorHandler((err: FastifyError | HttpError, _req, reply) => {
    if (err instanceof HttpError) return reply.status(err.status).send({ error: err.code, message: err.message });
    if (err instanceof Unknown) return reply.status(503).send({ error: "dependency_unavailable", message: "retry" });
    const e = err as FastifyError & { constraint?: string };
    if (e.validation) return reply.status(400).send({ error: "validation_error", message: e.message });
    if (e.constraint === "operations_one_active") {
      return reply.status(409).send({ error: "operation_in_progress", message: "another operation is in flight for this payment; retry" });
    }
    if (e.statusCode && e.statusCode < 500) return reply.status(e.statusCode).send({ error: e.code ?? "bad_request", message: e.message });
    console.error("[api] unhandled", err);
    return reply.status(500).send({ error: "internal_error", message: "internal error" });
  });

  function idempotencyKey(headers: Record<string, string | string[] | undefined>): string {
    const key = headers["idempotency-key"];
    if (typeof key !== "string" || key.length === 0 || key.length > 255) {
      throw new HttpError(400, "validation_error", "Idempotency-Key header is required (1–255 characters)");
    }
    return key;
  }

  // Drive inline for low latency. If someone else holds the lease, or the network is being unhelpful, wait briefly and
  // answer 202: the worker finishes it and the merchant hears by webhook.
  async function respond(
    reply: { status(n: number): { send(b: unknown): unknown }; header(k: string, v: string): unknown },
    admission: Admission,
  ) {
    if ("stored" in admission) {
      if (admission.replayed) reply.header("Idempotent-Replayed", "true");
      return reply.status(admission.stored.status).send(admission.stored.body);
    }
    if (!admission.fresh) reply.header("Idempotent-Replayed", "true");
    failpoint("api.admitted");
    let op: Op;
    const started = Date.now();
    for (;;) {
      const outcome = await drive(ctx, admission.op);
      op = await loadOp(ctx, admission.op);
      if (op.state === "succeeded" || op.state === "failed") return reply.status(op.result!.status).send(op.result!.body);
      if (outcome === "retry" || op.state === "stuck" || Date.now() - started > syncWaitMs) break;
      await new Promise((r) => setTimeout(r, 25));
    }
    return reply.status(202).send({ status: "processing", operationId: op.id, paymentId: op.payment_id });
  }

  app.get("/health", async () => {
    await ctx.pool.query("select 1");
    return { ok: true };
  });

  app.post<{ Body: { id: string; webhookUrl?: string } }>(
    "/v1/merchants",
    {
      schema: {
        body: {
          type: "object",
          required: ["id"],
          additionalProperties: false,
          properties: { id: { type: "string", pattern: "^[a-zA-Z0-9_-]{1,64}$" }, webhookUrl: { type: "string" } },
        },
      },
    },
    async (req, reply) => {
      const secret = `whsec_${randomBytes(24).toString("hex")}`;
      const res = await ctx.pool.query(
        "insert into merchants (id, webhook_url, webhook_secret) values ($1, $2, $3) on conflict (id) do nothing",
        [req.body.id, req.body.webhookUrl ?? null, secret],
      );
      if (!res.rowCount) throw new HttpError(409, "merchant_exists", "merchant already exists");
      return reply.status(201).send({ id: req.body.id, webhookSecret: secret });
    },
  );

  app.post<{ Body: { merchantId: string; amount: string; cardToken: string; currency?: string } }>(
    "/v1/payments",
    {
      schema: {
        body: {
          type: "object",
          required: ["merchantId", "amount", "cardToken"],
          additionalProperties: false,
          properties: {
            merchantId: { type: "string", minLength: 1 },
            amount: AMOUNT,
            cardToken: { type: "string", pattern: "^tok_[a-zA-Z0-9_-]{1,64}$" }, // simulator tokens only; never a card number
            currency: { type: "string", enum: ["USD"] },
          },
        },
      },
    },
    async (req, reply) => {
      const { merchantId, amount, cardToken } = req.body;
      const key = idempotencyKey(req.headers);
      const admission = await admit(ctx, merchantId, key, requestHash("authorize", req.body), async (c) => {
        const known = await c.query("select 1 from merchants where id = $1", [merchantId]);
        if (!known.rowCount) throw new HttpError(404, "merchant_not_found", `merchant ${merchantId} not found`);
        const paymentId = randomUUID();
        await c.query(
          `insert into payments (id, merchant_id, currency, card_token, status, amount_authorized, auth_ref, expires_at)
           values ($1, $2, 'USD', $3, 'authorizing', $4, $5, now() + ($6 || ' milliseconds')::interval)`,
          [paymentId, merchantId, cardToken, amount, `auth:${paymentId}`, String(ctx.cfg.authTtlMs)],
        );
        return createOp(c, { paymentId, merchantId, kind: "authorize", step: "network", deadlineMs: ctx.cfg.authDeadlineMs, networkRef: false });
      });
      return respond(reply, admission);
    },
  );

  // Shared admission for commands on an existing payment: row lock, state machine, amount rules, then the op.
  async function command(
    paymentId: string,
    key: string,
    body: unknown,
    cmd: Command,
    build: (c: Client, p: Payment) => Promise<string | { status: number; body: unknown }>,
  ): Promise<Admission> {
    const { rows } = await ctx.pool.query<{ merchant_id: string }>("select merchant_id from payments where id = $1", [paymentId]);
    if (!rows.length) throw new HttpError(404, "payment_not_found", `payment ${paymentId} not found`);
    return admit(ctx, rows[0].merchant_id, key, requestHash(`${cmd}:${paymentId}`, body), async (c) => {
      const p = (await c.query<Payment>("select * from payments where id = $1 for update", [paymentId])).rows[0];
      const active = await c.query("select 1 from operations where payment_id = $1 and state = 'in_progress' and kind in ('authorize','capture','void','expire','refund')", [paymentId]);
      if (active.rowCount) throw new HttpError(409, "operation_in_progress", "another operation is in flight for this payment; retry");
      if (!canAdmit(p.status, cmd)) {
        return { status: 409, body: { error: "illegal_transition", message: `${cmd} is not allowed from ${p.status}`, payment: paymentJson(p) } };
      }
      return build(c, p);
    });
  }

  const uuidParams = { type: "object", required: ["id"], properties: { id: { type: "string", format: "uuid" } } } as const;

  app.post<{ Params: { id: string }; Body: { amount?: string; final?: boolean } }>(
    "/v1/payments/:id/capture",
    {
      schema: {
        params: uuidParams,
        body: { type: "object", additionalProperties: false, properties: { amount: AMOUNT, final: { type: "boolean" } } },
      },
      preValidation: async (req) => {
        req.body ??= {};
      },
    },
    async (req, reply) => {
      const key = idempotencyKey(req.headers);
      const admission = await command(req.params.id, key, req.body, "capture", async (c, p) => {
        if (p.expires_at.getTime() <= Date.now()) {
          return { status: 422, body: { error: "authorization_expired", message: "this authorization has expired", payment: paymentJson(p) } };
        }
        const left = remaining(p);
        const amount = req.body.amount ? BigInt(req.body.amount) : left;
        if (amount > left) {
          return { status: 422, body: { error: "amount_exceeds_authorization", message: `only ${left} is left to capture`, payment: paymentJson(p) } };
        }
        await c.query("update payments set reserved_capture = reserved_capture + $2 where id = $1", [p.id, amount.toString()]);
        return createOp(c, { paymentId: p.id, merchantId: p.merchant_id, kind: "capture", step: "network", amount, finalCapture: req.body.final ?? true });
      });
      return respond(reply, admission);
    },
  );

  app.post<{ Params: { id: string } }>(
    "/v1/payments/:id/void",
    {
      schema: { params: uuidParams },
      preValidation: async (req) => {
        req.body ??= {};
      },
    },
    async (req, reply) => {
      const key = idempotencyKey(req.headers);
      const admission = await command(req.params.id, key, {}, "void", async (c, p) =>
        createOp(c, { paymentId: p.id, merchantId: p.merchant_id, kind: "void", step: "network" }),
      );
      return respond(reply, admission);
    },
  );

  app.post<{ Params: { id: string }; Body: { amount?: string } }>(
    "/v1/payments/:id/refund",
    {
      schema: { params: uuidParams, body: { type: "object", additionalProperties: false, properties: { amount: AMOUNT } } },
      preValidation: async (req) => {
        req.body ??= {};
      },
    },
    async (req, reply) => {
      const key = idempotencyKey(req.headers);
      const admission = await command(req.params.id, key, req.body, "refund", async (c, p) => {
        const left = refundable(p);
        const amount = req.body.amount ? BigInt(req.body.amount) : left;
        if (amount <= 0n || amount > left) {
          return { status: 422, body: { error: "amount_exceeds_refundable", message: `only ${left} is refundable`, payment: paymentJson(p) } };
        }
        await c.query("update payments set reserved_refund = reserved_refund + $2 where id = $1", [p.id, amount.toString()]);
        return createOp(c, { paymentId: p.id, merchantId: p.merchant_id, kind: "refund", step: "post", amount });
      });
      return respond(reply, admission);
    },
  );

  app.post<{ Params: { id: string }; Body: { amount: string } }>(
    "/v1/merchants/:id/reserve",
    { schema: { body: { type: "object", required: ["amount"], additionalProperties: false, properties: { amount: AMOUNT } } } },
    async (req, reply) => {
      const key = idempotencyKey(req.headers);
      const merchantId = req.params.id;
      const admission = await admit(ctx, merchantId, key, requestHash("reserve", req.body), async (c) => {
        const known = await c.query("select 1 from merchants where id = $1", [merchantId]);
        if (!known.rowCount) throw new HttpError(404, "merchant_not_found", `merchant ${merchantId} not found`);
        return createOp(c, { paymentId: null, merchantId, kind: "reserve_topup", step: "post", amount: BigInt(req.body.amount), networkRef: false });
      });
      return respond(reply, admission);
    },
  );

  // The network's channel to us. Store first, answer 200, process later: at-least-once and unordered on their side,
  // deduped and resequenced on ours.
  app.post<{ Body: { messageId: string; type: string; payload: Record<string, unknown> } }>(
    "/network/notifications",
    async (req, reply) => {
      await receiveNotification(ctx, req.body);
      failpoint("inbox.received");
      await processNotification(ctx, req.body.messageId).catch(() => {}); // the worker picks it up if this fails
      return reply.status(200).send({ ok: true });
    },
  );

  // Simulated "daily" run. The harness calls it with each new file; replays are no-ops.
  app.post<{ Params: { fileId: string } }>("/admin/reconcile/:fileId", async (req) => {
    const report = await reconcileFile(ctx, req.params.fileId);
    return { ...report, exceptions: report.exceptions.map((e) => ({ ref: e.ref, type: e.type })) };
  });

  app.get<{ Params: { id: string } }>("/v1/payments/:id", { schema: { params: uuidParams } }, async (req) => {
    const { rows } = await ctx.pool.query<Payment>("select * from payments where id = $1", [req.params.id]);
    if (!rows.length) throw new HttpError(404, "payment_not_found", `payment ${req.params.id} not found`);
    return paymentJson(rows[0]);
  });

  return app;
}

