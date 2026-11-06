import { createHash, randomUUID } from "node:crypto";
import Fastify from "fastify";
import { tx, type Client, type Pool } from "../db.ts";

// PLUMBING: a fake card network and issuer bank. Not a model of ISO 8583, just the behaviours a processor has to
// survive: latency, declines, lost requests, lost responses, late arrivals, duplicate and out-of-order notifications.

export interface Faults {
  latencyMs: [number, number];
  /** Authorizations declined at random with do_not_honor. */
  declineRate: number;
  captureDeclineRate: number;
  refundDeclineRate: number;
  /** Request dropped: never applied, the caller sees a broken connection. */
  dropRequestRate: number;
  /** Applied, then the response is lost: "the network approved but we timed out". */
  dropResponseRate: number;
  /** Caller sees a broken connection now; the message lands later. Races with any reversal the caller sends. */
  lateApplyRate: number;
  lateApplyMs: number;
  /** Send auth/capture advice notifications at all (extra channel that can overtake the response). */
  advice: boolean;
  duplicateNotificationRate: number;
  /** Notifications are delayed by a random amount up to this, which reorders them. */
  reorderWindowMs: number;
}

export const NO_FAULTS: Faults = {
  latencyMs: [0, 0],
  declineRate: 0,
  captureDeclineRate: 0,
  refundDeclineRate: 0,
  dropRequestRate: 0,
  dropResponseRate: 0,
  lateApplyRate: 0,
  lateApplyMs: 0,
  advice: false,
  duplicateNotificationRate: 0,
  reorderWindowMs: 0,
};

export interface NetworkOptions {
  pool: Pool;
  faults?: Partial<Faults>;
  processorUrl?: string;
  feeBps?: number;
  seed?: number;
}

export interface Injections {
  drop?: number;
  shift?: number;
  alterAmount?: number;
  alterFee?: number;
  duplicate?: number;
  phantom?: number;
}

export interface ManifestEntry {
  kind: keyof Injections;
  ref: string;
}

interface Reply {
  status: "approved" | "declined";
  reason?: string;
  appliedAt: string;
}

function mulberry32(seed: number) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const paramsHash = (o: unknown) => createHash("sha256").update(JSON.stringify(o)).digest("hex");

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function openingBalance(token: string): bigint {
  if (token.startsWith("tok_nsf")) return 0n;
  const low = /^tok_low_(\d+)/.exec(token);
  if (low) return BigInt(low[1]);
  return 1_000_000_000_000n;
}

const SETTLED_TYPES = ["capture", "refund", "chargeback", "chargeback_reversal"];

export function feeFor(type: string, gross: bigint, feeBps: number): bigint {
  return type === "capture" ? (gross * BigInt(feeBps)) / 10_000n : 0n;
}

export function buildNetwork(opts: NetworkOptions) {
  const { pool } = opts;
  const faults: Faults = { ...NO_FAULTS, ...opts.faults };
  const feeBps = opts.feeBps ?? 200;
  const rng = mulberry32(opts.seed ?? Date.now() & 0xffffffff);
  let processorUrl = opts.processorUrl;
  const app = Fastify({ logger: false });
  const lateTimers = new Set<NodeJS.Timeout>();

  async function notify(c: Client, type: string, payload: object) {
    const messageId = randomUUID();
    const copies = rng() < faults.duplicateNotificationRate ? 2 : 1;
    for (let i = 0; i < copies; i++) {
      const delay = Math.floor(rng() * faults.reorderWindowMs);
      await c.query(
        `insert into notifications (message_id, type, payload, deliver_after)
         values ($1, $2, $3, now() + ($4 || ' milliseconds')::interval)`,
        [messageId, type, JSON.stringify(payload), String(delay)],
      );
    }
  }

  // Replays by ref: a retransmit gets the original answer and is counted, never applied twice.
  async function withRef(
    ref: string,
    lockOn: string,
    params: object,
    apply: (c: Client) => Promise<Reply>,
  ): Promise<Reply> {
    return tx(pool, async (c) => {
      await c.query("select pg_advisory_xact_lock(hashtext($1))", [lockOn]);
      const hash = paramsHash(params);
      const seen = await c.query<{ status: string; reason: string | null; applied_at: Date; params_hash: string }>(
        "select status, reason, applied_at, params_hash from messages where ref = $1",
        [ref],
      );
      if (seen.rowCount) {
        const m = seen.rows[0];
        if (m.status === "tombstoned") {
          return { status: "declined", reason: "reversed", appliedAt: m.applied_at.toISOString() };
        }
        if (m.params_hash !== hash) throw Object.assign(new Error(`ref ${ref} reused`), { statusCode: 409 });
        await c.query("update messages set duplicates = duplicates + 1 where ref = $1", [ref]);
        return {
          status: m.status as Reply["status"],
          reason: m.reason ?? undefined,
          appliedAt: m.applied_at.toISOString(),
        };
      }
      return apply(c);
    });
  }

  async function insertMessage(
    c: Client,
    m: {
      ref: string;
      type: string;
      authRef?: string;
      merchantId?: string;
      cardToken?: string;
      amount: bigint;
      status: string;
      reason?: string;
      remaining?: bigint;
      params: object;
    },
  ): Promise<Reply> {
    const { rows } = await c.query<{ applied_at: Date }>(
      `insert into messages (ref, type, auth_ref, merchant_id, card_token, amount, status, reason, remaining, params_hash)
       values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) returning applied_at`,
      [
        m.ref,
        m.type,
        m.authRef ?? null,
        m.merchantId ?? null,
        m.cardToken ?? null,
        m.amount.toString(),
        m.status,
        m.reason ?? null,
        m.remaining?.toString() ?? null,
        paramsHash(m.params),
      ],
    );
    return { status: m.status as Reply["status"], reason: m.reason, appliedAt: rows[0].applied_at.toISOString() };
  }

  async function loadAuth(c: Client, authRef: string) {
    const { rows } = await c.query<{
      merchant_id: string;
      card_token: string;
      status: string;
      remaining: bigint;
      amount: bigint;
    }>("select merchant_id, card_token, status, remaining, amount from messages where ref = $1 and type = 'auth'", [
      authRef,
    ]);
    return rows[0];
  }

  function applyAuth(b: { ref: string; merchantId: string; cardToken: string; amount: string }) {
    const amount = BigInt(b.amount);
    const params = { t: "auth", m: b.merchantId, c: b.cardToken, a: b.amount };
    return withRef(b.ref, b.ref, params, async (c) => {
      await c.query("insert into cards (token, available) values ($1, $2) on conflict (token) do nothing", [
        b.cardToken,
        openingBalance(b.cardToken).toString(),
      ]);
      const base = { ref: b.ref, type: "auth", merchantId: b.merchantId, cardToken: b.cardToken, amount, params };
      let reply: Reply;
      if (rng() < faults.declineRate) {
        reply = await insertMessage(c, { ...base, status: "declined", reason: "do_not_honor" });
      } else {
        const held = await c.query("update cards set available = available - $2 where token = $1 and available >= $2", [
          b.cardToken,
          b.amount,
        ]);
        reply = held.rowCount
          ? await insertMessage(c, { ...base, status: "approved", remaining: amount })
          : await insertMessage(c, { ...base, status: "declined", reason: "insufficient_funds" });
      }
      if (faults.advice) await notify(c, "auth.advice", { ref: b.ref, status: reply.status });
      return reply;
    });
  }

  function applyCapture(b: { ref: string; authRef: string; amount: string; final: boolean }) {
    const amount = BigInt(b.amount);
    const params = { t: "capture", r: b.authRef, a: b.amount, f: b.final };
    return withRef(b.ref, b.authRef, params, async (c) => {
      const auth = await loadAuth(c, b.authRef);
      const base = { ref: b.ref, type: "capture", authRef: b.authRef, amount, params, merchantId: auth?.merchant_id };
      if (!auth || auth.status !== "approved") {
        return insertMessage(c, { ...base, status: "declined", reason: "no_such_authorization" });
      }
      if (amount > auth.remaining) return insertMessage(c, { ...base, status: "declined", reason: "exceeds_authorization" });
      if (rng() < faults.captureDeclineRate) return insertMessage(c, { ...base, status: "declined", reason: "network_declined" });
      const left = auth.remaining - amount;
      await c.query("update messages set remaining = $2 where ref = $1", [b.authRef, b.final ? "0" : left.toString()]);
      if (b.final && left > 0n) {
        await c.query("update cards set available = available + $2 where token = $1", [auth.card_token, left.toString()]);
      }
      const reply = await insertMessage(c, { ...base, status: "approved" });
      if (faults.advice) await notify(c, "capture.advice", { ref: b.ref, status: reply.status });
      return reply;
    });
  }

  // A void of an authorization we never saw leaves a tombstone, so if the auth is still in flight it can't land.
  function applyVoid(b: { ref: string; authRef: string; merchantId: string; amount: string }) {
    const params = { t: "void", r: b.authRef };
    return withRef(b.ref, b.authRef, params, async (c) => {
      const base = { ref: b.ref, type: "void", authRef: b.authRef, amount: 0n, params, merchantId: b.merchantId };
      const auth = await loadAuth(c, b.authRef);
      if (!auth) {
        await c.query(
          `insert into messages (ref, type, merchant_id, amount, status, reason, params_hash)
           values ($1, 'auth', $2, $3, 'tombstoned', 'reversed before arrival', 'tombstone')`,
          [b.authRef, b.merchantId, b.amount],
        );
        return insertMessage(c, { ...base, status: "approved", reason: "tombstoned" });
      }
      if (auth.status === "approved" && auth.remaining > 0n) {
        await c.query("update cards set available = available + $2 where token = $1", [
          auth.card_token,
          auth.remaining.toString(),
        ]);
        await c.query("update messages set remaining = 0 where ref = $1", [b.authRef]);
      }
      return insertMessage(c, { ...base, status: "approved" });
    });
  }

  function applyRefund(b: { ref: string; authRef: string; amount: string }) {
    const amount = BigInt(b.amount);
    const params = { t: "refund", r: b.authRef, a: b.amount };
    return withRef(b.ref, b.authRef, params, async (c) => {
      const auth = await loadAuth(c, b.authRef);
      const base = { ref: b.ref, type: "refund", authRef: b.authRef, amount, params, merchantId: auth?.merchant_id };
      if (!auth) return insertMessage(c, { ...base, status: "declined", reason: "no_such_authorization" });
      const { rows } = await c.query<{ captured: bigint; refunded: bigint }>(
        `select coalesce(sum(amount) filter (where type = 'capture'), 0)::bigint as captured,
                coalesce(sum(amount) filter (where type = 'refund'), 0)::bigint as refunded
           from messages where auth_ref = $1 and status = 'approved'`,
        [b.authRef],
      );
      if (rows[0].refunded + amount > rows[0].captured) {
        return insertMessage(c, { ...base, status: "declined", reason: "exceeds_captured" });
      }
      if (rng() < faults.refundDeclineRate) return insertMessage(c, { ...base, status: "declined", reason: "network_declined" });
      await c.query("update cards set available = available + $2 where token = $1", [auth.card_token, b.amount]);
      return insertMessage(c, { ...base, status: "approved" });
    });
  }

  type Apply = () => Promise<Reply>;

  // The fault wrapper every message goes through.
  async function handle(req: { raw: { socket: { destroy(): void } } }, reply: { hijack(): void }, apply: Apply) {
    const [lo, hi] = faults.latencyMs;
    if (hi > 0) await sleep(lo + rng() * (hi - lo));
    const breakConnection = () => {
      reply.hijack();
      req.raw.socket.destroy();
    };
    const roll = rng();
    if (roll < faults.dropRequestRate) return breakConnection();
    if (roll < faults.dropRequestRate + faults.lateApplyRate) {
      const timer = setTimeout(() => {
        lateTimers.delete(timer);
        apply().catch(() => {});
      }, faults.lateApplyMs);
      lateTimers.add(timer);
      return breakConnection();
    }
    const result = await apply();
    if (roll < faults.dropRequestRate + faults.lateApplyRate + faults.dropResponseRate) return breakConnection();
    return result;
  }

  app.get("/health", async () => {
    await pool.query("select 1");
    return { ok: true };
  });

  app.post<{ Body: { ref: string; merchantId: string; cardToken: string; amount: string } }>("/messages/auth", async (req, reply) =>
    handle(req, reply, () => applyAuth(req.body)),
  );
  app.post<{ Body: { ref: string; authRef: string; amount: string; final: boolean } }>("/messages/capture", async (req, reply) =>
    handle(req, reply, () => applyCapture(req.body)),
  );
  app.post<{ Body: { ref: string; authRef: string; merchantId: string; amount: string } }>("/messages/void", async (req, reply) =>
    handle(req, reply, () => applyVoid(req.body)),
  );
  app.post<{ Body: { ref: string; authRef: string; amount: string } }>("/messages/refund", async (req, reply) =>
    handle(req, reply, () => applyRefund(req.body)),
  );

  app.put<{ Body: Partial<Faults> }>("/admin/faults", async (req) => {
    Object.assign(faults, req.body);
    return faults;
  });

  // A cardholder disputes a charge. Always goes through: the processor doesn't get a vote.
  app.post<{ Body: { authRef: string; amount: string } }>("/admin/chargebacks", async (req, reply) => {
    const { authRef, amount } = req.body;
    const out = await tx(pool, async (c) => {
      const auth = await loadAuth(c, authRef);
      if (!auth) return null;
      const id = randomUUID();
      const ref = `cb:${id}`;
      await c.query("insert into disputes (id, auth_ref, amount, state) values ($1,$2,$3,'open')", [id, authRef, amount]);
      const m = await insertMessage(c, {
        ref,
        type: "chargeback",
        authRef,
        merchantId: auth.merchant_id,
        amount: BigInt(amount),
        status: "approved",
        params: { ref },
      });
      await notify(c, "chargeback.opened", { disputeId: id, authRef, amount, ref, seq: 1, appliedAt: m.appliedAt });
      return { disputeId: id, ref };
    });
    if (!out) return reply.status(404).send({ error: "no_such_authorization" });
    return reply.status(201).send(out);
  });

  app.post<{ Params: { id: string }; Body: { won: boolean } }>("/admin/chargebacks/:id/resolve", async (req, reply) => {
    const out = await tx(pool, async (c) => {
      const { rows } = await c.query<{ auth_ref: string; amount: bigint; state: string }>(
        "select auth_ref, amount, state from disputes where id = $1 for update",
        [req.params.id],
      );
      if (rows.length === 0 || rows[0].state !== "open") return null;
      const d = rows[0];
      const auth = await loadAuth(c, d.auth_ref);
      await c.query("update disputes set state = $2 where id = $1", [req.params.id, req.body.won ? "won" : "lost"]);
      const base = { disputeId: req.params.id, authRef: d.auth_ref, amount: d.amount.toString(), seq: 2 };
      if (!req.body.won) {
        await notify(c, "chargeback.lost", base);
        return { state: "lost" };
      }
      const ref = `cbr:${req.params.id}`;
      const m = await insertMessage(c, {
        ref,
        type: "chargeback_reversal",
        authRef: d.auth_ref,
        merchantId: auth.merchant_id,
        amount: d.amount,
        status: "approved",
        params: { ref },
      });
      await notify(c, "chargeback.won", { ...base, ref, appliedAt: m.appliedAt });
      return { state: "won", ref };
    });
    if (!out) return reply.status(404).send({ error: "no_open_dispute" });
    return out;
  });

  // Builds the day's settlement file, with deliberately injected mismatches recorded in a manifest.
  app.post<{ Body: { cutoff?: string; inject?: Injections } }>("/admin/settlement-files", async (req, reply) => {
    const cutoff = req.body.cutoff ? new Date(req.body.cutoff) : new Date();
    const inject = req.body.inject ?? {};
    const file = await tx(pool, async (c) => {
      await c.query("select pg_advisory_xact_lock(hashtext('settlement'))");
      const { rows } = await c.query<{ ref: string; type: string; merchant_id: string; amount: bigint }>(
        `select ref, type, merchant_id, amount from messages
          where settlement_file_id is null and status = 'approved' and applied_at <= $1 and type = any($2)
          order by applied_at, ref for update`,
        [cutoff, SETTLED_TYPES],
      );
      const id = randomUUID();
      const lines = rows.map((m) => ({
        ref: m.ref,
        type: m.type,
        merchant: m.merchant_id,
        gross: m.amount,
        fee: feeFor(m.type, m.amount, feeBps),
      }));

      // Pick distinct victims from a shuffled list, so one line never carries two injected faults.
      const pool_ = [...lines];
      for (let i = pool_.length - 1; i > 0; i--) {
        const j = Math.floor(rng() * (i + 1));
        [pool_[i], pool_[j]] = [pool_[j], pool_[i]];
      }
      const manifest: ManifestEntry[] = [];
      const take = (kind: keyof Injections, candidates = pool_) => {
        const picked: typeof lines = [];
        for (let n = inject[kind] ?? 0; n > 0; n--) {
          const idx = candidates.findIndex(() => true);
          if (idx < 0) break;
          const [line] = candidates.splice(idx, 1);
          picked.push(line);
          manifest.push({ kind, ref: line.ref });
        }
        return picked;
      };
      const dropped = new Set(take("drop").map((l) => l.ref));
      const shifted = new Set(take("shift").map((l) => l.ref));
      for (const l of take("alterAmount")) l.gross += 1n;
      for (const l of take("alterFee")) l.fee += 1n;
      const dupes = take("duplicate");
      const output = lines.filter((l) => !dropped.has(l.ref) && !shifted.has(l.ref));
      output.push(...dupes);
      const template = lines[0];
      for (let n = inject.phantom ?? 0; n > 0; n--) {
        const ref = `phantom:${randomUUID()}`;
        output.push({ ref, type: "capture", merchant: template?.merchant ?? "unknown", gross: 1234n, fee: feeFor("capture", 1234n, feeBps) });
        manifest.push({ kind: "phantom", ref });
      }

      const csv = ["ref,type,merchant_id,gross,fee,net,currency"];
      for (const l of output) {
        const net = l.type === "capture" || l.type === "chargeback_reversal" ? l.gross - l.fee : -l.gross;
        csv.push([l.ref, l.type, l.merchant, l.gross, l.fee, net, "USD"].join(","));
      }
      const text = csv.join("\n") + "\n";
      await c.query("insert into settlement_files (id, cutoff, csv, manifest) values ($1,$2,$3,$4)", [
        id,
        cutoff,
        text,
        JSON.stringify(manifest),
      ]);
      // A shifted line stays unsettled, so it lands in the next file (a timing difference). A dropped line is gone.
      await c.query("update messages set settlement_file_id = $1 where ref = any($2)", [
        id,
        rows.map((r) => r.ref).filter((r) => !shifted.has(r)),
      ]);
      return { id, cutoff: cutoff.toISOString(), csv: text, manifest };
    });
    return reply.status(201).send(file);
  });

  app.get<{ Params: { id: string } }>("/settlement-files/:id", async (req, reply) => {
    const { rows } = await pool.query<{ id: string; cutoff: Date; csv: string }>(
      "select id, cutoff, csv from settlement_files where id = $1",
      [req.params.id],
    );
    if (rows.length === 0) return reply.status(404).send({ error: "not_found" });
    return { id: rows[0].id, cutoff: rows[0].cutoff.toISOString(), csv: rows[0].csv };
  });

  // Pushes due notifications. At-least-once and unordered, by design: a failed POST stays due.
  async function dispatchOnce(limit = 50): Promise<number> {
    if (!processorUrl) return 0;
    const { rows } = await pool.query<{ id: bigint; message_id: string; type: string; payload: unknown }>(
      `select id, message_id, type, payload from notifications
        where delivered_at is null and deliver_after <= now() order by deliver_after limit $1`,
      [limit],
    );
    let sent = 0;
    await Promise.all(
      rows.map(async (n) => {
        try {
          const res = await fetch(`${processorUrl}/network/notifications`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({ messageId: n.message_id, type: n.type, payload: n.payload }),
            signal: AbortSignal.timeout(2000),
          });
          if (res.ok) {
            await pool.query("update notifications set delivered_at = now() where id = $1", [n.id.toString()]);
            sent++;
          }
        } catch {
          // Processor down or slow. Stays due.
        }
      }),
    );
    return sent;
  }

  let timer: NodeJS.Timeout | undefined;
  let running = false;
  function startDispatcher(intervalMs = 50) {
    timer = setInterval(async () => {
      if (running) return;
      running = true;
      await dispatchOnce().catch(() => {});
      running = false;
    }, intervalMs);
  }
  app.addHook("onClose", async () => {
    clearInterval(timer);
    for (const t of lateTimers) clearTimeout(t);
  });

  return {
    app,
    faults,
    dispatchOnce,
    startDispatcher,
    setProcessorUrl: (url: string) => {
      processorUrl = url;
    },
  };
}
