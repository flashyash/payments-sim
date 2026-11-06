import { spawn, type ChildProcess } from "node:child_process";
import { dbUrl } from "../src/config.ts";
import { LEDGER_DIR, setupDbs } from "../scripts/setup-dbs.ts";

export const TEST_DBS = { processor: "t_processor", network: "t_network", sink: "t_sink", ledger: "t_ledger" };
export const TEST_LEDGER_PORT = 3191;

let ledger: ChildProcess | undefined;

// A real ledger process on its own database: integration tests go through the same HTTP API production would.
export default async function setup() {
  await setupDbs(TEST_DBS, true);
  ledger = spawn("node", ["src/server.ts"], {
    cwd: LEDGER_DIR,
    env: { ...process.env, DATABASE_URL: dbUrl(TEST_DBS.ledger), PORT: String(TEST_LEDGER_PORT), LOG_LEVEL: "error" },
    stdio: ["ignore", "ignore", "inherit"],
  });
  for (let i = 0; i < 100; i++) {
    const up = await fetch(`http://127.0.0.1:${TEST_LEDGER_PORT}/health`).then((r) => r.ok, () => false);
    if (up) break;
    if (i === 99) throw new Error("ledger did not start");
    await new Promise((r) => setTimeout(r, 100));
  }
  return async () => {
    ledger?.kill("SIGKILL");
  };
}
