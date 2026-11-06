// Runs everything until Ctrl-C: API on :3100, ledger :3101, network :3102, merchant sink :3103.
import { Stack } from "./lib/stack.ts";

const stack = new Stack({ fresh: process.argv.includes("--fresh") });
await stack.start();
console.log(`api ${stack.apiUrl}  ledger ${stack.ledgerUrl}  network ${stack.networkUrl}  sink ${stack.sinkUrl}`);
for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, async () => {
    await stack.stop();
    process.exit(0);
  });
}
