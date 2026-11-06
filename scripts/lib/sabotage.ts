import type { ProxiedRequest } from "../../src/faultProxy.ts";
import type { Stack } from "./stack.ts";

// Negative controls for the chaos harness. Each breaks one guarantee from the outside, on the wire or in the database, so
// the services run the same code as in production. A stack started with `withProxies` is required for the first two.

export function unstableLedgerKey(req: ProxiedRequest): void {
  if (req.method === "POST" && req.url === "/transactions") req.headers["idempotency-key"] = `${req.headers["idempotency-key"]}:${Math.random()}`;
}

export function unstableNetworkRef(req: ProxiedRequest): void {
  if (req.method !== "POST" || (req.url !== "/messages/capture" && req.url !== "/messages/refund")) return;
  const message = JSON.parse(req.body.toString()) as { ref: string };
  req.body = Buffer.from(JSON.stringify({ ...message, ref: `${message.ref}:${Math.random()}` }));
}

export const SABOTAGE = {
  // A fresh key per attempt turns retries into double postings.
  unstable_ledger_key: async (stack: Stack) => stack.setProxy("ledger", { rewrite: unstableLedgerKey }),
  // A fresh ref per attempt makes the network apply retries twice.
  unstable_network_ref: async (stack: Stack) => stack.setProxy("network", { rewrite: unstableNetworkRef }),
  // The payment commits and its webhook silently doesn't. Goes away with the database when the next stack starts fresh.
  skip_outbox: async (stack: Stack) => {
    await stack.pools.processor.query("create rule sabotage_skip_outbox as on insert to outbox where new.payment_id is not null do instead nothing");
  },
};

export type SabotageName = keyof typeof SABOTAGE;
