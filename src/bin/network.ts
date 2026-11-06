import { dbUrl } from "../config.ts";
import { createPool } from "../db.ts";
import { buildNetwork, type Faults } from "../network/app.ts";

const pool = createPool(process.env.NETWORK_DATABASE_URL ?? dbUrl("network"), Number(process.env.DB_POOL_SIZE ?? 20));
const faults = process.env.NETWORK_FAULTS ? (JSON.parse(process.env.NETWORK_FAULTS) as Partial<Faults>) : {};
const { app, startDispatcher } = buildNetwork({
  pool,
  faults,
  processorUrl: process.env.PROCESSOR_URL ?? "http://127.0.0.1:3100",
  feeBps: Number(process.env.FEE_BPS ?? 200),
  seed: process.env.SEED ? Number(process.env.SEED) : undefined,
});
startDispatcher();
await app.listen({ port: Number(process.env.PORT ?? 3102), host: "127.0.0.1" });
