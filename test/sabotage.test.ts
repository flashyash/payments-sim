import { readdirSync, readFileSync } from "node:fs";
import http from "node:http";
import type { AddressInfo } from "node:net";
import path from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { unstableLedgerKey, unstableNetworkRef } from "../scripts/lib/sabotage.ts";
import { startProxy } from "../src/faultProxy.ts";

// `npm run chaos` part 4 proves each control gets caught. These prove the seam itself: requests really change on the
// wire, only the ones meant to, and the services have no way to be sabotaged from their environment.
describe("sabotage seam", () => {
  const seen: { url: string; key?: string; body: string; length?: string }[] = [];
  const upstream = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (d) => chunks.push(d));
    req.on("end", () => {
      seen.push({ url: req.url!, key: req.headers["idempotency-key"] as string, body: Buffer.concat(chunks).toString(), length: req.headers["content-length"] });
      res.end("{}");
    });
  });
  let proxy: Awaited<ReturnType<typeof startProxy>>;
  const proxyPort = 3491;
  const send = (url: string, body: object, key?: string) =>
    fetch(`http://127.0.0.1:${proxyPort}${url}`, {
      method: "POST",
      headers: { "content-type": "application/json", ...(key ? { "idempotency-key": key } : {}) },
      body: JSON.stringify(body),
    });

  beforeAll(async () => {
    await new Promise<void>((r) => upstream.listen(0, "127.0.0.1", r));
    proxy = await startProxy((upstream.address() as AddressInfo).port, proxyPort);
  });
  afterAll(async () => {
    await proxy.close();
    upstream.closeAllConnections();
    await new Promise((r) => upstream.close(r));
  });

  it("unstable ledger key: each attempt at a posting gets a new key, reversals keep theirs", async () => {
    proxy.set({ rewrite: unstableLedgerKey });
    seen.length = 0;
    await send("/transactions", {}, "k");
    await send("/transactions", {}, "k");
    await send("/transactions/t/reverse", {}, "r");
    expect(seen[0].key).toMatch(/^k:/);
    expect(seen[1].key).toMatch(/^k:/);
    expect(seen[0].key).not.toBe(seen[1].key);
    expect(seen[2].key).toBe("r");
  });

  it("unstable network ref: captures and refunds get a new ref per attempt, auths keep theirs, length stays honest", async () => {
    proxy.set({ rewrite: unstableNetworkRef });
    seen.length = 0;
    await send("/messages/capture", { ref: "c" });
    await send("/messages/refund", { ref: "f" });
    await send("/messages/auth", { ref: "a" });
    expect(JSON.parse(seen[0].body).ref).toMatch(/^c:/);
    expect(JSON.parse(seen[1].body).ref).toMatch(/^f:/);
    expect(JSON.parse(seen[2].body).ref).toBe("a");
    expect(Number(seen[0].length)).toBe(seen[0].body.length);
  });

  it("no service reads a sabotage switch", () => {
    const root = path.resolve(import.meta.dirname, "../src");
    const files = readdirSync(root, { recursive: true, encoding: "utf8" }).filter((f) => f.endsWith(".ts"));
    expect(files.length).toBeGreaterThan(10);
    for (const f of files) expect(readFileSync(path.join(root, f), "utf8"), f).not.toMatch(/SABOTAGE|sabotage/);
  });
});
