import { randomUUID } from "node:crypto";
import { loadProcessorConfig, dbUrl, type ProcessorConfig } from "../src/config.ts";
import { buildApi } from "../src/api.ts";
import { buildContext } from "../src/context.ts";
import { createPool, type Pool } from "../src/db.ts";
import type { Ctx } from "../src/engine.ts";
import { buildNetwork, NO_FAULTS, type Faults } from "../src/network/app.ts";
import { tick } from "../src/worker.ts";
import { TEST_DBS, TEST_LEDGER_PORT } from "./global-setup.ts";

export interface Stack {
  ctx: Ctx;
  api: ReturnType<typeof buildApi>;
  apiUrl: string;
  net: ReturnType<typeof buildNetwork>;
  netUrl: string;
  netPool: Pool;
  ledgerPool: Pool;
  merchant: string;
  faults: (f: Partial<Faults>) => void;
  resetFaults: () => void;
  close: () => Promise<void>;
}

// Everything but the ledger runs in-process. Each file gets clean processor/network tables and a fresh chart of
// accounts, so aggregate balances start at zero even though the ledger database is shared for the run.
export async function startStack(cfgOver: Partial<ProcessorConfig> = {}, netFaults: Partial<Faults> = {}): Promise<Stack> {
  const netPool = createPool(dbUrl(TEST_DBS.network), 10);
  const ledgerPool = createPool(dbUrl(TEST_DBS.ledger), 3);
  const pool = createPool(dbUrl(TEST_DBS.processor), 20);
  await netPool.query("truncate cards, messages, disputes, notifications, settlement_files");
  await pool.query(`truncate merchants, ledger_accounts, payments, disputes, operations, idempotency_keys, ledger_postings,
                    outbox, network_inbox, settlement_items, recon_runs, recon_exceptions cascade`);

  const net = buildNetwork({ pool: netPool, faults: netFaults, seed: 42 });
  const netUrl = await net.app.listen({ port: 0, host: "127.0.0.1" });
  const cfg = loadProcessorConfig({
    databaseUrl: dbUrl(TEST_DBS.processor),
    ledgerUrl: `http://127.0.0.1:${TEST_LEDGER_PORT}`,
    networkUrl: netUrl,
    // Generous on purpose: a busy machine must not trip these. Tests that need a deadline to pass move it in the database.
    leaseMs: 5000,
    httpTimeoutMs: 3000,
    authDeadlineMs: 60_000,
    retryBaseMs: 10,
    retryMaxMs: 100,
    webhookRetryBaseMs: 10,
    webhookRetryMaxMs: 100,
    ...cfgOver,
  });
  const ctx = buildContext(cfg, pool);
  const api = buildApi(ctx, { syncWaitMs: 2000 });
  const apiUrl = await api.listen({ port: 0, host: "127.0.0.1" });
  net.setProcessorUrl(apiUrl);
  const merchant = `m_${randomUUID().slice(0, 8)}`;
  const res = await fetch(`${apiUrl}/v1/merchants`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ id: merchant }),
  });
  if (res.status !== 201) throw new Error("could not create merchant");
  return {
    ctx,
    api,
    apiUrl,
    net,
    netUrl,
    netPool,
    ledgerPool,
    merchant,
    faults: (f) => Object.assign(net.faults, f),
    resetFaults: () => Object.assign(net.faults, NO_FAULTS),
    close: async () => {
      await api.close();
      await net.app.close();
      await pool.end();
      await netPool.end();
      await ledgerPool.end();
    },
  };
}

export interface ApiResult<T = Record<string, string> & { error?: string; payment?: Record<string, string> }> {
  status: number;
  body: T;
  replayed: boolean;
}

export function client(s: Stack) {
  async function call(method: string, path: string, body?: object, key: string | null = randomUUID()): Promise<ApiResult> {
    const res = await fetch(s.apiUrl + path, {
      method,
      headers: { ...(body ? { "content-type": "application/json" } : {}), ...(key ? { "idempotency-key": key } : {}) },
      body: body ? JSON.stringify(body) : undefined,
    });
    return { status: res.status, body: (await res.json()) as ApiResult["body"], replayed: res.headers.get("idempotent-replayed") === "true" };
  }
  return {
    call,
    authorize: (amount = "10000", key?: string, cardToken = "tok_ok") =>
      call("POST", "/v1/payments", { merchantId: s.merchant, amount, cardToken }, key),
    capture: (id: string, body: { amount?: string; final?: boolean } = {}, key?: string) => call("POST", `/v1/payments/${id}/capture`, body, key),
    void: (id: string, key?: string) => call("POST", `/v1/payments/${id}/void`, {}, key),
    refund: (id: string, amount?: string, key?: string) => call("POST", `/v1/payments/${id}/refund`, amount ? { amount } : {}, key),
    get: (id: string) => call("GET", `/v1/payments/${id}`, undefined, null),
  };
}

// Runs background work until an op-free state, or fails loudly.
export async function quiesce(s: Stack, opts: { timeoutMs?: number } = {}): Promise<void> {
  const deadline = Date.now() + (opts.timeoutMs ?? 20_000);
  for (;;) {
    await tick(s.ctx);
    const { rows } = await s.ctx.pool.query<{ n: bigint }>(
      "select count(*)::bigint as n from operations where state = 'in_progress'",
    );
    if (rows[0].n === 0n) return;
    if (Date.now() > deadline) throw new Error(`still ${rows[0].n} ops in progress`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

export async function ledgerBalance(s: Stack, name: string): Promise<bigint> {
  const { rows } = await s.ctx.pool.query<{ id: string }>("select id from ledger_accounts where name = $1", [name]);
  return rows.length ? s.ctx.ledger.balance(rows[0].id) : 0n;
}

export async function shardedBalance(s: Stack, prefix: string): Promise<bigint> {
  const { rows } = await s.ctx.pool.query<{ id: string }>("select id from ledger_accounts where name like $1", [`${prefix}:%`]);
  let total = 0n;
  for (const r of rows) total += await s.ctx.ledger.balance(r.id);
  return total;
}

export async function holdBalance(s: Stack, paymentId: string): Promise<bigint> {
  const { rows } = await s.ctx.pool.query<{ hold_account_id: string }>("select hold_account_id from payments where id = $1", [paymentId]);
  return s.ctx.ledger.balance(rows[0].hold_account_id);
}

// Pushes notifications through the real HTTP path, then waits for ops, inbox and network queue to be empty.
export async function drain(s: Stack, opts: { timeoutMs?: number } = {}): Promise<void> {
  const deadline = Date.now() + (opts.timeoutMs ?? 20_000);
  for (;;) {
    await s.net.dispatchOnce();
    await tick(s.ctx);
    const ops = await s.ctx.pool.query<{ n: number }>(
      `select (select count(*) from operations where state = 'in_progress')::int
            + (select count(*) from network_inbox where state = 'pending')::int as n`,
    );
    const net = await s.netPool.query<{ n: number }>("select count(*)::int as n from notifications where delivered_at is null");
    if (ops.rows[0].n === 0 && net.rows[0].n === 0) return;
    if (Date.now() > deadline) throw new Error(`not drained: ${ops.rows[0].n} local, ${net.rows[0].n} at network`);
    await new Promise((r) => setTimeout(r, 20));
  }
}

export async function fundMerchant(s: Stack, amount: string): Promise<void> {
  const r = await client(s).call("POST", `/v1/merchants/${s.merchant}/reserve`, { amount });
  if (r.status !== 201) throw new Error(`top-up failed: ${r.status}`);
}

// Puts an authorization past its deadline without waiting for the clock.
export async function expireAuthDeadline(s: Stack, paymentId: string): Promise<void> {
  await s.ctx.pool.query("update operations set deadline_at = now() - interval '1 second', next_attempt_at = now(), lease_until = null where payment_id = $1 and kind = 'authorize'", [paymentId]);
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
