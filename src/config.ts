const env = (name: string, fallback: string) => process.env[name] ?? fallback;
const num = (name: string, fallback: number) => Number(process.env[name] ?? fallback);

export const PG_BASE = env("PG_BASE_URL", "postgres://payments:payments@localhost:5434");

export function dbUrl(db: string): string {
  return `${PG_BASE}/${db}`;
}

export interface ProcessorConfig {
  databaseUrl: string;
  ledgerUrl: string;
  networkUrl: string;
  leaseMs: number;
  httpTimeoutMs: number;
  /** How long an authorization may stay unresolved before we give up and reverse it. */
  authDeadlineMs: number;
  retryBaseMs: number;
  retryMaxMs: number;
  /** Hot ledger accounts (receivable, hold contra) are split into this many sub-accounts. */
  shards: number;
  authTtlMs: number;
  feeBps: number;
  webhookRetryBaseMs: number;
  webhookRetryMaxMs: number;
  webhookTimeoutMs: number;
}

export function loadProcessorConfig(overrides: Partial<ProcessorConfig> = {}): ProcessorConfig {
  return {
    databaseUrl: env("DATABASE_URL", dbUrl("processor")),
    ledgerUrl: env("LEDGER_URL", "http://127.0.0.1:3101"),
    networkUrl: env("NETWORK_URL", "http://127.0.0.1:3102"),
    leaseMs: num("LEASE_MS", 10_000),
    httpTimeoutMs: num("HTTP_TIMEOUT_MS", 2_000),
    authDeadlineMs: num("AUTH_DEADLINE_MS", 15_000),
    retryBaseMs: num("RETRY_BASE_MS", 100),
    retryMaxMs: num("RETRY_MAX_MS", 5_000),
    shards: num("LEDGER_SHARDS", 16),
    authTtlMs: num("AUTH_TTL_MS", 7 * 24 * 3600 * 1000),
    feeBps: num("FEE_BPS", 200),
    webhookRetryBaseMs: num("WEBHOOK_RETRY_BASE_MS", 200),
    webhookRetryMaxMs: num("WEBHOOK_RETRY_MAX_MS", 10_000),
    webhookTimeoutMs: num("WEBHOOK_TIMEOUT_MS", 2_000),
    ...overrides,
  };
}
