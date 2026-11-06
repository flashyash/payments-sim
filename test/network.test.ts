import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { dbUrl } from "../src/config.ts";
import { createPool, type Pool } from "../src/db.ts";
import { buildNetwork, NO_FAULTS } from "../src/network/app.ts";
import { TEST_DBS } from "./global-setup.ts";

let pool: Pool;
let net: ReturnType<typeof buildNetwork>;
let url: string;
let n = 0;

const post = (path: string, body: object) =>
  fetch(url + path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
const ok = async (path: string, body: object) => (await post(path, body)).json() as Promise<{ status: string; reason?: string }>;
const ref = () => `r${Date.now()}-${n++}`;
const auth = (r: string, amount = "1000", cardToken = "tok_ok") => ok("/messages/auth", { ref: r, merchantId: "m", cardToken, amount });
const available = async (token: string) =>
  (await pool.query<{ available: bigint }>("select available from cards where token = $1", [token])).rows[0].available;

beforeAll(async () => {
  pool = createPool(dbUrl(TEST_DBS.network), 5);
  await pool.query("truncate cards, messages, disputes, notifications, settlement_files");
  net = buildNetwork({ pool, seed: 7 });
  url = await net.app.listen({ port: 0, host: "127.0.0.1" });
});
afterAll(async () => {
  await net.app.close();
  await pool.end();
});
beforeEach(() => Object.assign(net.faults, NO_FAULTS));

describe("fake network", () => {
  it("retransmitting a ref returns the original answer and applies once", async () => {
    const card = `tok_ok_${ref()}`;
    const r = ref();
    expect((await auth(r, "1000", card)).status).toBe("approved");
    const before = await available(card);
    expect((await auth(r, "1000", card)).status).toBe("approved");
    expect(await available(card)).toBe(before);
    const { rows } = await pool.query("select duplicates from messages where ref = $1", [r]);
    expect(rows[0].duplicates).toBe(1);
  });

  it("rejects a ref reused with different parameters", async () => {
    const r = ref();
    await auth(r);
    expect((await post("/messages/auth", { ref: r, merchantId: "m", cardToken: "tok_ok", amount: "2" })).status).toBe(409);
  });

  it("declines insufficient funds, and holds reduce availability", async () => {
    expect((await auth(ref(), "1", `tok_nsf_${ref()}`)).reason).toBe("insufficient_funds");
    const low = `tok_low_100_${ref()}`;
    expect((await auth(ref(), "80", low)).status).toBe("approved");
    expect((await auth(ref(), "30", low)).reason).toBe("insufficient_funds");
  });

  it("enforces captures against the authorization and releases the rest on a final capture", async () => {
    const card = `tok_low_100_${ref()}`;
    const a = ref();
    await auth(a, "100", card);
    expect((await ok("/messages/capture", { ref: ref(), authRef: a, amount: "101", final: false })).reason).toBe("exceeds_authorization");
    expect((await ok("/messages/capture", { ref: ref(), authRef: a, amount: "60", final: true })).status).toBe("approved");
    expect(await available(card)).toBe(40n);
    expect((await ok("/messages/capture", { ref: ref(), authRef: a, amount: "1", final: false })).reason).toBe("exceeds_authorization");
  });

  it("a dropped response still applies; a dropped request doesn't", async () => {
    const card = `tok_low_100_${ref()}`;
    net.faults.dropResponseRate = 1;
    await expect(auth(ref(), "10", card)).rejects.toThrow();
    net.faults.dropResponseRate = 0;
    expect(await available(card)).toBe(90n);
    net.faults.dropRequestRate = 1;
    await expect(auth(ref(), "10", card)).rejects.toThrow();
    net.faults.dropRequestRate = 0;
    expect(await available(card)).toBe(90n);
  });

  it("a reversal that overtakes its auth leaves a tombstone, so the late auth is declined", async () => {
    const card = `tok_low_100_${ref()}`;
    const a = ref();
    net.faults.lateApplyRate = 1;
    net.faults.lateApplyMs = 100;
    await expect(auth(a, "100", card)).rejects.toThrow();
    net.faults.lateApplyRate = 0;
    expect((await ok("/messages/void", { ref: ref(), authRef: a, merchantId: "m", amount: "100" })).status).toBe("approved");
    await new Promise((r) => setTimeout(r, 200));
    const seed = await auth(ref(), "100", card); // creates the card row if the late auth never did
    expect(seed.status).toBe("approved");
    expect((await auth(a, "100", card)).reason).toBe("reversed");
  });

  it("refunds can't exceed captures", async () => {
    const a = ref();
    await auth(a);
    await ok("/messages/capture", { ref: ref(), authRef: a, amount: "100", final: true });
    expect((await ok("/messages/refund", { ref: ref(), authRef: a, amount: "60" })).status).toBe("approved");
    expect((await ok("/messages/refund", { ref: ref(), authRef: a, amount: "41" })).reason).toBe("exceeds_captured");
  });

  it("settlement files contain each line once and record every injected fault", async () => {
    for (let i = 0; i < 12; i++) {
      const a = ref();
      await auth(a);
      await ok("/messages/capture", { ref: ref(), authRef: a, amount: "1000", final: true });
    }
    const res = await post("/admin/settlement-files", { inject: { drop: 1, shift: 1, alterAmount: 1, alterFee: 1, duplicate: 1, phantom: 1 } });
    const file = (await res.json()) as { csv: string; manifest: { kind: string; ref: string }[] };
    expect(file.manifest.map((m) => m.kind).sort()).toEqual(["alterAmount", "alterFee", "drop", "duplicate", "phantom", "shift"]);
    const refs = file.csv.trim().split("\n").slice(1).map((l) => l.split(",")[0]);
    const byKind = Object.fromEntries(file.manifest.map((m) => [m.kind, m.ref]));
    expect(refs).not.toContain(byKind.drop);
    expect(refs).not.toContain(byKind.shift);
    expect(refs.filter((r) => r === byKind.duplicate)).toHaveLength(2);
    expect(refs).toContain(byKind.phantom);
    // The shifted line lands in the next file; the dropped one is gone.
    const next = (await (await post("/admin/settlement-files", {})).json()) as { csv: string };
    expect(next.csv).toContain(byKind.shift);
    expect(next.csv).not.toContain(byKind.drop);
  });
});
