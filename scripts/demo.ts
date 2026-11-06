// A guided tour. Starts everything, walks one payment through its life, lands an unknown network outcome and injects a
// settlement mismatch, printing what each step did. Uses its own databases, wiped on every run.
import { randomUUID } from "node:crypto";
import { checkInvariants } from "../src/invariants.ts";
import { Merchant, newAcks, quiesce, send, setupMerchants } from "./lib/driver.ts";
import { Stack } from "./lib/stack.ts";

const stack = new Stack({ dbs: { processor: "d_processor", network: "d_network", sink: "d_sink", ledger: "d_ledger" }, basePort: 3500 });
await stack.start();
const say = (title: string, value?: unknown) => console.log(value === undefined ? `\n== ${title}` : `${title}: ${JSON.stringify(value)}`);
try {
  const [merchantId] = await setupMerchants(stack, 1, "demo");
  const merchant = new Merchant(stack, merchantId, newAcks());

  say("Authorize $100.00, capture $60.00 (partial, final), refund $10.00");
  const a = await merchant.authorize("10000");
  say("authorize", { status: a.status, payment: a.body.status });
  const c = await merchant.capture(a.body.id, "6000");
  say("capture", { status: c.status, payment: c.body.status, captured: c.body.amountCaptured, released: c.body.amountReleased });
  const over = await merchant.capture(a.body.id, "1");
  say("capture again (illegal)", { status: over.status, error: over.body.error });
  const r = await merchant.refund(a.body.id, "1000");
  say("refund", { status: r.status, payment: r.body.status, refunded: r.body.amountRefunded });

  say("The network approves, but the response is lost");
  await stack.setNetworkFaults({ dropResponseRate: 1 });
  const key = randomUUID();
  const res = await fetch(`${stack.apiUrl}/v1/payments`, {
    method: "POST",
    headers: { "content-type": "application/json", "idempotency-key": key },
    body: JSON.stringify({ merchantId, amount: "2500", cardToken: "tok_ok" }),
  });
  const pending = (await res.json()) as { status: string; paymentId: string };
  say("first answer", { http: res.status, ...pending });
  await stack.setNetworkFaults({ dropResponseRate: 0 });
  const resolved = await send(stack, "POST", "/v1/payments", { merchantId, amount: "2500", cardToken: "tok_ok" }, key);
  say("after retransmit", { http: resolved.status, payment: resolved.body.status });

  say("Chargeback, then reconciliation with an injected amount mismatch");
  const b = await merchant.authorize("4000");
  await merchant.capture(b.body.id, "4000");
  const dispute = await merchant.chargeback(b.body.id, "4000");
  say("chargeback opened", { dispute });
  await quiesce(stack);
  const fileRes = await fetch(`${stack.networkUrl}/admin/settlement-files`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ inject: { alterAmount: 1 } }),
  });
  const file = (await fileRes.json()) as { id: string; csv: string; manifest: unknown };
  console.log(file.csv);
  say("injected", file.manifest);
  const rec = await fetch(`${stack.apiUrl}/admin/reconcile/${file.id}`, { method: "POST" });
  const report = (await rec.json()) as { lines: number; matched: number; exceptions: unknown };
  say("reconciliation", { lines: report.lines, matched: report.matched, exceptions: report.exceptions });
  await quiesce(stack);

  say("Webhooks the merchant received");
  const { rows } = await stack.pools.sink.query<{ type: string; n: number }>("select type, count(*)::int as n from processed_events group by type order by type");
  for (const row of rows) console.log(`  ${row.type} × ${row.n}`);

  const open = await stack.pools.processor.query<{ n: number }>("select count(*)::int as n from recon_exceptions where state = 'open'");
  const inv = await checkInvariants(stack.pools);
  say("\nInvariants", { violations: inv.violations, openReconExceptions: open.rows[0].n, checked: inv.checked });
} finally {
  await stack.stop();
}
process.exit(0);
