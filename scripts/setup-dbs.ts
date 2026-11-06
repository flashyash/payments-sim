// Creates and migrates the processor, network, sink and ledger databases. `--fresh` wipes them first.
import { spawn } from "node:child_process";
import path from "node:path";
import pg from "pg";
import { PG_BASE, dbUrl } from "../src/config.ts";
import { createPool } from "../src/db.ts";
import { migrate } from "../src/migrate.ts";

export const LEDGER_DIR = path.resolve(import.meta.dirname, "..", process.env.LEDGER_DIR ?? "../ledger");

export interface DbNames {
  processor: string;
  network: string;
  sink: string;
  ledger: string;
}

export const DEFAULT_DBS: DbNames = { processor: "processor", network: "network", sink: "sink", ledger: "ledger_ps" };

export async function setupDbs(names: DbNames, fresh: boolean): Promise<void> {
  const admin = new pg.Client({ connectionString: `${PG_BASE}/postgres` });
  await admin.connect();
  for (const db of Object.values(names)) {
    if (fresh) {
      await admin.query(`select pg_terminate_backend(pid) from pg_stat_activity where datname = $1`, [db]);
      await admin.query(`drop database if exists "${db}"`);
    }
    const exists = await admin.query("select 1 from pg_database where datname = $1", [db]);
    if (exists.rowCount === 0) await admin.query(`create database "${db}"`);
  }
  await admin.end();

  for (const service of ["processor", "network", "sink"] as const) {
    const pool = createPool(dbUrl(names[service]), 1);
    await migrate(pool, service);
    await pool.end();
  }

  // The ledger owns its schema: run its own migrator, against our database, without touching its code.
  await new Promise<void>((resolve, reject) => {
    const child = spawn("node", ["src/migrate.ts"], {
      cwd: LEDGER_DIR,
      env: { ...process.env, DATABASE_URL: dbUrl(names.ledger) },
      stdio: "ignore",
    });
    child.on("exit", (code) => (code === 0 ? resolve() : reject(new Error(`ledger migrate exited ${code}`))));
    child.on("error", reject);
  });
}

if (import.meta.main) {
  await setupDbs(DEFAULT_DBS, process.argv.includes("--fresh"));
  console.log("databases ready:", Object.values(DEFAULT_DBS).join(", "));
}
