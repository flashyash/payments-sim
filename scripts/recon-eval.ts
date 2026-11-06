// How good is reconciliation? Injects random mismatches into real settlement files, and compares what it reports with
// the injector's manifest. Precision and recall here are measured against ground truth the matcher can't see.
import { randomUUID } from "node:crypto";
import { writeFileSync } from "node:fs";
import path from "node:path";
import { Merchant, newAcks, quiesce, send, setupMerchants } from "./lib/driver.ts";
import { Stack } from "./lib/stack.ts";

const DBS = { processor: "r_processor", network: "r_network", sink: "r_sink", ledger: "r_ledger" };
const DAYS = Number(process.env.RECON_DAYS ?? 5);
const PER_DAY = Number(process.env.RECON_PER_DAY ?? 200);

const EXPECTED: Record<string, string> = {
  drop: "missing_in_file",
  shift: "missing_in_file",
  alterAmount: "amount_mismatch",
  alterFee: "fee_mismatch",
  duplicate: "duplicate_in_file",
  phantom: "missing_in_books",
};

const stack = new Stack({ dbs: DBS, basePort: 3400 });
await stack.start();
let tp = 0;
let fp = 0;
let fn = 0;
let alreadyOpen = 0;
let lines = 0;
let matched = 0;
let injected = 0;
let shiftsResolved = 0;
const byKind: Record<string, { injected: number; detected: number }> = {};
try {
  const [id] = await setupMerchants(stack, 1, "r");
  const merchant = new Merchant(stack, id, newAcks());
  let previousShifts: string[] = [];
  for (let day = 0; day < DAYS; day++) {
    const workers = Array.from({ length: 10 }, async () => {
      for (let i = 0; i < PER_DAY / 10; i++) {
        const a = await merchant.authorize(String(1000 + Math.floor(Math.random() * 90000)));
        await send(stack, "POST", `/v1/payments/${a.body.id}/capture`, {}, randomUUID());
      }
    });
    await Promise.all(workers);
    await quiesce(stack);

    const inject = Object.fromEntries(Object.keys(EXPECTED).map((k) => [k, 1 + Math.floor(Math.random() * 3)]));
    const fileRes = await fetch(`${stack.networkUrl}/admin/settlement-files`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ inject }),
    });
    const file = (await fileRes.json()) as { id: string; manifest: { kind: string; ref: string }[] };
    const rec = await fetch(`${stack.apiUrl}/admin/reconcile/${file.id}`, { method: "POST" });
    const report = (await rec.json()) as { lines: number; matched: number; exceptions: { ref: string; type: string }[]; resolved: string[] };
    await quiesce(stack);

    // The injector can pick a line that's already missing from an earlier day (shifted twice in a row). Reconciliation
    // deliberately doesn't report those again, because the exception is still open. Counted separately, and it only
    // counts if an open exception of the right type really exists for that line.
    const stillOpen = await stack.pools.processor.query<{ ref: string; type: string }>(
      "select ref, type from recon_exceptions where state = 'open' and file_id <> $1",
      [file.id],
    );
    const open = new Set(stillOpen.rows.map((e) => `${e.ref}/${e.type}`));
    const got = new Set(report.exceptions.map((e) => `${e.ref}/${e.type}`));
    const want = new Set<string>();
    for (const m of file.manifest) {
      const key = `${m.ref}/${EXPECTED[m.kind]}`;
      byKind[m.kind] ??= { injected: 0, detected: 0 };
      byKind[m.kind].injected++;
      if (!got.has(key) && open.has(key)) {
        alreadyOpen++;
        continue;
      }
      want.add(key);
      if (got.has(key)) byKind[m.kind].detected++;
    }
    for (const w of want) (got.has(w) ? tp++ : fn++);
    for (const g of got) if (!want.has(g)) fp++;
    injected += file.manifest.length;
    lines += report.lines;
    matched += report.matched;
    shiftsResolved += previousShifts.filter((r) => report.resolved.includes(r)).length;
    previousShifts = file.manifest.filter((m) => m.kind === "shift").map((m) => m.ref);
    console.log(`day ${day + 1}: ${report.lines} lines, ${report.matched} matched, injected ${file.manifest.length}, reported ${report.exceptions.length}`);
  }
  const result = {
    days: DAYS,
    linesInFiles: lines,
    matched,
    injected,
    truePositives: tp,
    falsePositives: fp,
    falseNegatives: fn,
    alreadyCoveredByOpenException: alreadyOpen,
    precision: tp / (tp + fp),
    recall: tp / (tp + fn),
    shiftsResolvedNextDay: shiftsResolved,
    byKind,
  };
  console.log(JSON.stringify(result, null, 2));
  writeFileSync(path.resolve(import.meta.dirname, "../results/recon-eval.json"), JSON.stringify(result, null, 2));
} finally {
  await stack.stop();
}
process.exit(0);
