import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globalSetup: ["test/global-setup.ts"],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    // Files share one ledger process and three databases; each file resets them in beforeAll.
    fileParallelism: false,
  },
});
