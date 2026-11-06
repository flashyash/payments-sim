import { randomUUID } from "node:crypto";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { accountNames } from "../src/chart.ts";
import type { Injections, ManifestEntry } from "../src/network/app.ts";
import { parseSettlementFile, reconcileFile, settlementEntries } from "../src/recon.ts";
import { client, drain, fundMerchant, ledgerBalance, quiesce, shardedBalance, startStack, type Stack } from "./helpers.ts";

let s: Stack;
let api: ReturnType<typeof client>;

beforeAll(async () => {
  s = await startStack();
  api = client(s);
});
afterAll(() => s.close());
afterEach(() => s.resetFaults());

const pending = () => ledgerBalance(s, accountNames.pending(s.merchant));
const available = () => ledgerBalance(s, accountNames.available(s.merchant));

async function captures(n: number, amount = "1000") {
  const ids: string[] = [];
  for (let i = 0; i < n; i++) {
    const a = await api.authorize(amount);
    expect((await api.capture(a.body.id)).status).toBe(201);
    ids.push(a.body.id);
  }
  return ids;
}

async function newFile(inject: Injections = {}) {
  const res = await fetch(`${s.netUrl}/admin/settlement-files`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ inject }),
  });
  expect(res.status).toBe(201);
  return (await res.json()) as { id: string; csv: string; manifest: ManifestEntry[] };
}

// Clears anything earlier tests left unsettled, so each test's expectations are exact.
async function settleEverything() {
  await quiesce(s);
  const f = await newFile();
  await reconcileFile(s.ctx, f.id);
  await quiesce(s);
  await s.ctx.pool.query("update recon_exceptions set state = 'resolved', resolution = 'test cleanup' where state = 'open'");
  await s.ctx.pool.query("update settlement_items set state = 'settled' where state = 'unsettled'");
}

describe("settlement arithmetic", () => {
  it("balances for every line type", () => {
    const e = settlementEntries([
      { type: "capture", amount: 1000n, fee: 20n, shard: 0 },
      { type: "refund", amount: 100n, fee: 0n, shard: 0 },
      { type: "chargeback", amount: 50n, fee: 0n, shard: 1 },
      { type: "chargeback_reversal", amount: 50n, fee: 0n, shard: 1 },
    ]);
    const total = e.pending + e.available + e.cash + [...e.receivable.values()].reduce((a, b) => a + b, 0n);
    expect(total).toBe(0n);
    expect(e.pending).toBe(-1000n);
    expect(e.available).toBe(980n);
    expect(e.cash).toBe(-(980n - 100n - 50n + 50n));
    expect(e.receivable.get(0)).toBe(900n);
    expect(e.receivable.get(1)).toBe(0n);
  });
});

describe("clean reconciliation", () => {
  it("settles matched captures: pending → available, net of fees, receivable cleared", async () => {
    await settleEverything();
    const pendingBefore = await pending();
    const availableBefore = await available();
    const receivableBefore = await shardedBalance(s, "network_receivable");
    await captures(5);
    expect((await pending()) - pendingBefore).toBe(5000n);
    const report = await reconcileFile(s.ctx, (await newFile()).id);
    await quiesce(s);
    expect(report.exceptions).toEqual([]);
    expect(report.matched).toBe(5);
    expect(await pending()).toBe(pendingBefore);
    expect((await available()) - availableBefore).toBe(5000n - 5n * 20n);
    expect(await shardedBalance(s, "network_receivable")).toBe(receivableBefore);
    const { rows } = await s.ctx.pool.query<{ n: number }>("select count(*)::int as n from settlement_items where state = 'settling'");
    expect(rows[0].n).toBe(0);
  });

  it("settles refunds and chargebacks too, and clears their receivable", async () => {
    await settleEverything();
    await fundMerchant(s, "100000");
    const ids = await captures(3);
    const before = await shardedBalance(s, "network_receivable");
    expect((await api.refund(ids[0], "1000")).status).toBe(201);
    const res = await fetch(`${s.netUrl}/admin/chargebacks`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ authRef: `auth:${ids[1]}`, amount: "1000" }),
    });
    expect(res.status).toBe(201);
    await drain(s);
    const f = await newFile();
    const report = await reconcileFile(s.ctx, f.id);
    await quiesce(s);
    expect(report.exceptions).toEqual([]);
    expect(report.matched).toBe(5);
    expect(await shardedBalance(s, "network_receivable")).toBe(before + 3000n); // all 3 captures cleared (they were −3000)
  });

  it("replaying a file is a no-op, and doesn't move money twice", async () => {
    await settleEverything();
    await captures(2);
    const f = await newFile();
    await reconcileFile(s.ctx, f.id);
    await quiesce(s);
    const before = await available();
    const again = await reconcileFile(s.ctx, f.id);
    await quiesce(s);
    expect(again.replayed).toBe(true);
    expect(await available()).toBe(before);
  });

  it("concurrent runs of the same file settle once", async () => {
    await settleEverything();
    await captures(4);
    const before = await available();
    const f = await newFile();
    const reports = await Promise.all([reconcileFile(s.ctx, f.id), reconcileFile(s.ctx, f.id), reconcileFile(s.ctx, f.id)]);
    await quiesce(s);
    expect(reports.filter((r) => !r.replayed)).toHaveLength(1);
    expect((await available()) - before).toBe(4000n - 80n);
  });

  it("refuses a file whose contents changed since it was reconciled", async () => {
    await settleEverything();
    await captures(1);
    const f = await newFile();
    await reconcileFile(s.ctx, f.id);
    await s.netPool.query("update settlement_files set csv = csv || 'x,capture,m,1,0,1,USD\n' where id = $1", [f.id]);
    await expect(reconcileFile(s.ctx, f.id)).rejects.toThrow(/changed/);
  });
});

describe("injected mismatches", () => {
  it("catches each kind of injected mismatch exactly, with no false positives, and settles the clean lines", async () => {
    await settleEverything();
    await captures(30);
    const availableBefore = await available();
    const pendingBefore = await pending();
    const f = await newFile({ drop: 2, shift: 2, alterAmount: 2, alterFee: 2, duplicate: 2, phantom: 2 });
    const report = await reconcileFile(s.ctx, f.id);
    await quiesce(s);

    const expected = new Map<string, string>([
      ["drop", "missing_in_file"],
      ["shift", "missing_in_file"],
      ["alterAmount", "amount_mismatch"],
      ["alterFee", "fee_mismatch"],
      ["duplicate", "duplicate_in_file"],
      ["phantom", "missing_in_books"],
    ]);
    const want = f.manifest.map((m) => `${m.ref}/${expected.get(m.kind)}`).sort();
    const got = report.exceptions.map((e) => `${e.ref}/${e.type}`).sort();
    expect(got).toEqual(want);

    // 30 lines: 8 lost to drop/shift/alterAmount/alterFee, 2 to duplicate. Exactly the other 20 settle.
    expect(report.matched).toBe(20);
    expect((await available()) - availableBefore).toBe(20n * 980n);
    expect(pendingBefore - (await pending())).toBe(20n * 1000n);
  });

  it("a shifted line clears itself when it arrives in the next file", async () => {
    await settleEverything();
    await captures(5);
    const f1 = await newFile({ shift: 1 });
    const r1 = await reconcileFile(s.ctx, f1.id);
    await quiesce(s);
    const shifted = f1.manifest[0].ref;
    expect(r1.exceptions).toMatchObject([{ ref: shifted, type: "missing_in_file" }]);

    const f2 = await newFile();
    const r2 = await reconcileFile(s.ctx, f2.id);
    await quiesce(s);
    expect(r2.exceptions).toEqual([]);
    expect(r2.resolved).toEqual([shifted]);
    const { rows } = await s.ctx.pool.query<{ state: string; resolution: string }>("select state, resolution from recon_exceptions where ref = $1", [shifted]);
    expect(rows[0].state).toBe("resolved");
    expect(rows[0].resolution).toMatch(/timing/);
  });

  it("a dropped line stays open and isn't reported again every day", async () => {
    await settleEverything();
    await captures(3);
    const f1 = await newFile({ drop: 1 });
    await reconcileFile(s.ctx, f1.id);
    await quiesce(s);
    const r2 = await reconcileFile(s.ctx, (await newFile()).id);
    expect(r2.exceptions).toEqual([]);
    const { rows } = await s.ctx.pool.query<{ state: string }>("select state from recon_exceptions where ref = $1", [f1.manifest[0].ref]);
    expect(rows).toEqual([{ state: "open" }]);
  });

  it("a line in the file before our op committed is a timing difference, resolved on the next run", async () => {
    await settleEverything();
    const a = await api.authorize("1000");
    s.faults({ dropResponseRate: 1 });
    const c = await api.capture(a.body.id);
    expect(c.status).toBe(202); // the network has it, we don't know yet
    s.resetFaults();
    const f1 = await newFile();
    const r1 = await reconcileFile(s.ctx, f1.id);
    expect(r1.exceptions.map((e) => e.type)).toEqual(["missing_in_books"]);
    await quiesce(s);
    const before = await available();
    const r2 = await reconcileFile(s.ctx, (await newFile()).id);
    await quiesce(s);
    expect(r2.exceptions).toEqual([]);
    expect(r2.resolved).toHaveLength(1);
    expect((await available()) - before).toBe(980n);
  });

  it("catches a ledger transaction that doesn't match, even though the file and our item agree", async () => {
    await settleEverything();
    const [good, tampered] = await captures(2);
    const { rows } = await s.ctx.pool.query<{ ledger_txn_id: string }>("select ledger_txn_id from settlement_items where payment_id = $1", [good]);
    await s.ctx.pool.query("update settlement_items set ledger_txn_id = $1 where payment_id = $2", [randomUUID(), tampered]);
    const r = await reconcileFile(s.ctx, (await newFile()).id);
    await quiesce(s);
    expect(r.exceptions.map((e) => e.type)).toEqual(["ledger_mismatch"]);
    expect(rows.length).toBe(1);
    expect(r.matched).toBe(1);
  });

  it("flags a line for a merchant other than the one on our books", async () => {
    await settleEverything();
    await captures(1);
    const f = await newFile();
    const line = parseSettlementFile(f.csv)[0];
    await s.netPool.query("update settlement_files set csv = replace(csv, $1, 'someone_else') where id = $2", [line.merchantId, f.id]);
    const r = await reconcileFile(s.ctx, f.id);
    expect(r.exceptions.map((e) => e.type)).toEqual(["merchant_mismatch"]);
  });

  it("a line that already settled and appears again is flagged, not paid twice", async () => {
    await settleEverything();
    await captures(1);
    const f1 = await newFile();
    await reconcileFile(s.ctx, f1.id);
    await quiesce(s);
    const line = f1.csv.trim().split("\n")[1];
    const before = await available();
    const { rows } = await s.netPool.query<{ id: string }>(
      `insert into settlement_files (cutoff, csv, manifest) values (now(), $1, '[]') returning id`,
      [`ref,type,merchant_id,gross,fee,net,currency\n${line}\n`],
    );
    const r = await reconcileFile(s.ctx, rows[0].id);
    expect(r.exceptions.map((e) => e.type)).toEqual(["already_settled"]);
    expect(await available()).toBe(before);
  });
});
