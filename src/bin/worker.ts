import { loadProcessorConfig } from "../config.ts";
import { buildContext } from "../context.ts";
import { startWorker } from "../worker.ts";

const ctx = buildContext(loadProcessorConfig());
startWorker(ctx, Number(process.env.WORKER_INTERVAL_MS ?? 50));
process.on("SIGTERM", () => process.exit(0));
