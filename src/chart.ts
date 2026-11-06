import type { LedgerClient } from "./clients.ts";
import type { Pool } from "./db.ts";

// The chart of accounts: stable names → ledger ids. Positive balance = credit (what we owe). See DESIGN.md.

export const accountNames = {
  holdContra: (shard: number) => `hold_contra:${shard}`,
  receivable: (shard: number) => `network_receivable:${shard}`,
  bankCash: () => "bank_cash",
  pending: (m: string) => `merchant:${m}:pending`,
  available: (m: string) => `merchant:${m}:available`,
  debt: (m: string) => `merchant:${m}:dispute_debt`,
};

const ALLOW_NEGATIVE = (name: string) =>
  name.startsWith("hold_contra:") || name.startsWith("network_receivable:") || name === "bank_cash" || name.endsWith(":dispute_debt");

export class Chart {
  pool: Pool;
  ledger: LedgerClient;
  cache = new Map<string, string>();

  constructor(pool: Pool, ledger: LedgerClient) {
    this.pool = pool;
    this.ledger = ledger;
  }

  async id(name: string): Promise<string> {
    const cached = this.cache.get(name);
    if (cached) return cached;
    const found = await this.pool.query<{ id: string }>("select id from ledger_accounts where name = $1", [name]);
    if (found.rowCount) {
      this.cache.set(name, found.rows[0].id);
      return found.rows[0].id;
    }
    // Two processes can race here. Both create an account; the first insert wins and the loser's stays empty forever.
    const created = await this.ledger.createAccount(name, ALLOW_NEGATIVE(name));
    await this.pool.query("insert into ledger_accounts (name, id) values ($1, $2) on conflict (name) do nothing", [name, created]);
    const { rows } = await this.pool.query<{ id: string }>("select id from ledger_accounts where name = $1", [name]);
    this.cache.set(name, rows[0].id);
    return rows[0].id;
  }
}

export function shardOf(id: string, shards: number): number {
  return parseInt(id.slice(0, 8), 16) % shards;
}
