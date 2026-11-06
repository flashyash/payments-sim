import { Chart } from "./chart.ts";
import { LedgerClient, NetworkClient } from "./clients.ts";
import type { ProcessorConfig } from "./config.ts";
import { createPool, type Pool } from "./db.ts";
import type { Ctx } from "./engine.ts";

export function buildContext(cfg: ProcessorConfig, pool?: Pool): Ctx {
  const p = pool ?? createPool(cfg.databaseUrl, Number(process.env.DB_POOL_SIZE ?? 20));
  const ledger = new LedgerClient(cfg.ledgerUrl, cfg.httpTimeoutMs);
  return { pool: p, cfg, ledger, network: new NetworkClient(cfg.networkUrl, cfg.httpTimeoutMs), chart: new Chart(p, ledger) };
}
