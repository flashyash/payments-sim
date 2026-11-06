// The chaos harness. Real OS processes, real SIGKILLs, real lost responses; afterwards the oracle reads every database.
// Part 1: crash at every named step of every flow. Part 2: lose the request or response at every external call.
// Part 3: everything at once, with random kills. Part 4: sabotage, to prove the oracle can see the bugs it claims to.
//
// npm run chaos                 full run
// npm run chaos -- --profile ci small enough for CI
import { writeFileSync } from "node:fs";
import path from "node:path";
import { ALL_FAILPOINTS } from "../src/failpoints.ts";
import { checkInvariants } from "../src/invariants.ts";
import { auditAcks, fullFlow, Merchant, newAcks, quiesce, settleOnce, setupMerchants, type Acks } from "./lib/driver.ts";
import { SABOTAGE, type SabotageName } from "./lib/sabotage.ts";
import { FAST_RECOVERY, Stack, type ServiceName } from "./lib/stack.ts";

const profile = process.argv.includes("--profile") ? process.argv[process.argv.indexOf("--profile") + 1] : "full";
const CI = profile === "ci";
const only = process.argv.includes("--part") ? process.argv[process.argv.indexOf("--part") + 1] : "all";

const DBS = { processor: "c_processor", network: "c_network", sink: "c_sink", ledger: "c_ledger" };
const BASE = 3200;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface Result {
  name: string;
  fired: boolean;
  quiesceMs: number | null;
  violations: string[];
  checked?: Record<string, number>;
}

function log(line: string) {
  console.log(`[${new Date().toISOString().slice(11, 19)}] ${line}`);
}

// After faults stop: wait for quiet, run two settlement days to flush timing differences, wait again, audit.
async function audit(stack: Stack, acks: Acks): Promise<{ quiesceMs: number | null; violations: string[]; checked: Record<string, number> }> {
  const first = await quiesce(stack);
  await settleOnce(stack);
  await quiesce(stack);
  await settleOnce(stack);
  const last = await quiesce(stack);
  const report = await checkInvariants(stack.pools);
  const violations = [...report.violations, ...(await auditAcks(stack, acks))];
  if (first === null || last === null) violations.push("quiesced: work still outstanding after the 60 s bound");
  const open = await stack.pools.processor.query<{ ref: string; type: string }>("select ref, type from recon_exceptions where state = 'open'");
  for (const e of open.rows) violations.push(`reconciliation_clean: ${e.type} still open for ${e.ref}`);
  return { quiesceMs: first, violations, checked: report.checked };
}

const CALM = { latencyMs: [0, 0], declineRate: 0, captureDeclineRate: 0, refundDeclineRate: 0, dropRequestRate: 0, dropResponseRate: 0, lateApplyRate: 0, lateApplyMs: 0, advice: false, duplicateNotificationRate: 0, reorderWindowMs: 0 };

// Failpoints that need specific network weather before their line of code can run at all.
const WEATHER: Partial<Record<(typeof ALL_FAILPOINTS)[number], object>> = {
  "refund.reverse_post.after": { refundDeclineRate: 1 },
};

async function part1(results: Result[]): Promise<void> {
  log(`part 1: SIGKILL at each of ${ALL_FAILPOINTS.length} failpoints`);
  const stack = new Stack({ dbs: DBS, basePort: BASE, withProxies: true, env: FAST_RECOVERY });
  await stack.start();
  const acks = newAcks();
  try {
    const merchants = await setupMerchants(stack, 1, "fp");
    const merchant = new Merchant(stack, merchants[0], acks);
    const points = CI ? ALL_FAILPOINTS.filter((_, i) => i % 3 === 0) : ALL_FAILPOINTS;
    for (const fp of points) {
      const killsBefore = stack.killedAt().length;
      await stack.respawn("api", { FAILPOINTS: `${fp}:kill` });
      await stack.respawn("worker", { FAILPOINTS: `${fp}:kill` });
      await stack.setNetworkFaults({ ...CALM, ...WEATHER[fp] });
      if (fp === "authorize.reverse.after") {
        // Only reachable if the network stays silent past the auth deadline.
        await stack.setNetworkFaults({ ...CALM, dropRequestRate: 1 });
        const stuck = merchant.authorize("1234");
        await sleep(4000);
        await stack.setNetworkFaults({ ...CALM });
        await stuck;
      }
      await fullFlow(merchant);
      await settleOnce(stack);
      await stack.setNetworkFaults({ ...CALM });
      const { quiesceMs, violations, checked } = await audit(stack, acks);
      const fired = stack.killedAt().length > killsBefore;
      results.push({ name: `kill@${fp}`, fired, quiesceMs, violations, checked });
      log(`  ${fp.padEnd(30)} ${fired ? "killed" : "NOT HIT"}  quiesce ${quiesceMs} ms  violations ${violations.length}`);
      for (const x of violations.slice(0, 5)) log(`      ${x}`);
      // Anything the failpoint didn't get to fire on must not stay armed into the next round.
      await stack.respawn("api");
      await stack.respawn("worker");
    }
  } finally {
    await stack.stop();
  }
}

const DROPS: { name: string; which: "ledger" | "network"; path: string }[] = [
  { name: "ledger /transactions", which: "ledger", path: "/transactions" },
  { name: "ledger /accounts", which: "ledger", path: "/accounts" },
  { name: "ledger reverse", which: "ledger", path: "/reverse" },
  { name: "network auth", which: "network", path: "/messages/auth" },
  { name: "network capture", which: "network", path: "/messages/capture" },
  { name: "network void", which: "network", path: "/messages/void" },
  { name: "network refund", which: "network", path: "/messages/refund" },
];

async function part2(results: Result[]): Promise<void> {
  log("part 2: drop requests and responses at each external call");
  const stack = new Stack({ dbs: DBS, basePort: BASE, withProxies: true, env: FAST_RECOVERY });
  await stack.start();
  const acks = newAcks();
  try {
    const merchants = await setupMerchants(stack, 1, "dp");
    const merchant = new Merchant(stack, merchants[0], acks);
    for (const d of CI ? DROPS.slice(0, 2) : DROPS) {
      for (const mode of ["dropResponseRate", "dropRequestRate"] as const) {
        const before = { ...(d.which === "ledger" ? stack.ledgerProxy! : stack.networkProxy!).stats };
        await stack.setNetworkFaults(d.path === "/reverse" ? { ...CALM, refundDeclineRate: 1 } : { ...CALM });
        // Half of matching calls are lost, so a flow can't just stall until the fault ends: it has to get through retries.
        stack.setProxy(d.which, { [mode]: 0.5, pathContains: d.path });
        // A flow makes few calls of some kinds, so keep going until this fault has actually bitten a few times.
        const proxy = d.which === "ledger" ? stack.ledgerProxy! : stack.networkProxy!;
        const droppedSoFar = () => proxy.stats.droppedRequests + proxy.stats.droppedResponses - before.droppedRequests - before.droppedResponses;
        for (let i = 0; i < 10 && droppedSoFar() < 3; i++) await fullFlow(merchant);
        stack.setProxy(d.which, { dropRequestRate: 0, dropResponseRate: 0, pathContains: "" });
        await stack.setNetworkFaults({ ...CALM });
        const after = (d.which === "ledger" ? stack.ledgerProxy! : stack.networkProxy!).stats;
        const dropped = after.droppedRequests + after.droppedResponses - before.droppedRequests - before.droppedResponses;
        const { quiesceMs, violations, checked } = await audit(stack, acks);
        const name = `${mode === "dropResponseRate" ? "lost response" : "lost request"}: ${d.name}`;
        results.push({ name, fired: dropped > 0, quiesceMs, violations, checked });
        log(`  ${name.padEnd(42)} dropped ${String(dropped).padStart(4)}  quiesce ${quiesceMs} ms  violations ${violations.length}`);
        for (const x of violations.slice(0, 5)) log(`      ${x}`);
      }
    }
  } finally {
    await stack.stop();
  }
}

async function part3(results: Result[]): Promise<void> {
  const seconds = CI ? 15 : 90;
  log(`part 3: ${seconds} s of everything at once, random SIGKILLs`);
  const weather = {
    ...CALM,
    latencyMs: [0, 20],
    declineRate: 0.05,
    captureDeclineRate: 0.02,
    refundDeclineRate: 0.05,
    dropRequestRate: 0.03,
    dropResponseRate: 0.05,
    lateApplyRate: 0.03,
    lateApplyMs: 1500,
    advice: true,
    duplicateNotificationRate: 0.3,
    reorderWindowMs: 500,
  };
  const stack = new Stack({
    dbs: DBS,
    basePort: BASE,
    withProxies: true,
    env: FAST_RECOVERY,
    networkFaults: weather,
    sinkFaults: { failRate: 0.1, dropAfterProcessRate: 0.1 },
  });
  await stack.start();
  const acks = newAcks();
  let flows = 0;
  const kills: Record<string, number> = {};
  try {
    const merchants = await setupMerchants(stack, 4, "ch");
    stack.setProxy("ledger", { dropRequestRate: 0.03, dropResponseRate: 0.05 });
    stack.setProxy("network", { dropRequestRate: 0.02, dropResponseRate: 0.03 });
    const stopAt = Date.now() + seconds * 1000;

    const traffic = merchants.flatMap((id) =>
      [0, 1].map(async () => {
        const m = new Merchant(stack, id, acks);
        while (Date.now() < stopAt) {
          await fullFlow(m);
          flows++;
        }
      }),
    );
    const killer = (async () => {
      const victims: ServiceName[] = ["api", "worker", "ledger", "network", "sink"];
      while (Date.now() < stopAt) {
        await sleep(500 + Math.random() * 1500);
        const v = victims[Math.floor(Math.random() * victims.length)];
        stack.kill(v);
        kills[v] = (kills[v] ?? 0) + 1;
      }
    })();
    const settler = (async () => {
      while (Date.now() < stopAt) {
        await sleep(5000);
        await settleOnce(stack);
      }
    })();
    await Promise.all([...traffic, killer, settler]);

    stack.setProxy("ledger", { dropRequestRate: 0, dropResponseRate: 0 });
    stack.setProxy("network", { dropRequestRate: 0, dropResponseRate: 0 });
    await stack.setNetworkFaults({ ...CALM });
    await stack.setSinkFaults({ failRate: 0, dropAfterProcessRate: 0 });
    const { quiesceMs, violations, checked } = await audit(stack, acks);
    const seen = await faultsSeen(stack);
    results.push({ name: `all faults for ${seconds} s`, fired: true, quiesceMs, violations, checked: { ...checked, flows, ...prefixed(kills), ...seen } });
    log(`  flows ${flows}, kills ${JSON.stringify(kills)}, checked ${JSON.stringify(checked)}`);
    log(`  faults actually seen: ${JSON.stringify(seen)}`);
    log(`  quiesce ${quiesceMs} ms, violations ${violations.length}`);
    for (const x of violations.slice(0, 20)) log(`      ${x}`);
  } finally {
    await stack.stop();
  }
}

// Evidence the storm really happened: a green result means little if the system never had to recover from anything.
async function faultsSeen(stack: Stack): Promise<Record<string, number>> {
  const one = async (pool: "processor" | "network" | "sink", sql: string) => (await stack.pools[pool].query<{ n: number }>(sql)).rows[0].n;
  return {
    networkRetransmits: await one("network", "select coalesce(sum(duplicates),0)::int as n from messages"),
    authsTombstoned: await one("network", "select count(*)::int as n from messages where status = 'tombstoned'"),
    paymentsFailedByTimeout: await one("processor", "select count(*)::int as n from payments where status = 'failed'"),
    opsRetried: await one("processor", "select count(*)::int as n from operations where attempts > 1"),
    refundsReversed: await one("processor", "select count(*)::int as n from ledger_postings where purpose = 'refund_reversal' and state = 'posted'"),
    refundsRejectedFinal: await one("processor", "select count(*)::int as n from ledger_postings where state = 'rejected'"),
    notificationsParked: await one("processor", "select count(*)::int as n from network_inbox where parked_count > 0"),
    disputes: await one("processor", "select count(*)::int as n from disputes"),
    webhookAttempts: await one("sink", "select count(*)::int as n from deliveries"),
    webhookDuplicates: await one("sink", "select (count(*) filter (where response = 200) - count(distinct event_id) filter (where response = 200))::int as n from deliveries"),
    settlementFiles: await one("processor", "select count(*)::int as n from recon_runs"),
    transientReconExceptions: await one("processor", "select count(*)::int as n from recon_exceptions where state = 'resolved'"),
  };
}

const prefixed = (o: Record<string, number>) => Object.fromEntries(Object.entries(o).map(([k, v]) => [`kills_${k}`, v]));

// Negative controls. Each breaks one guarantee on purpose; a run that stays green here means the oracle is blind.
async function part4(results: Result[]): Promise<void> {
  const controls: { sabotage: SabotageName; which: "ledger" | "network"; expect: string }[] = [
    { sabotage: "unstable_ledger_key", which: "ledger", expect: "postings_exact" },
    { sabotage: "unstable_network_ref", which: "network", expect: "nothing_captured_twice" },
    { sabotage: "skip_outbox", which: "ledger", expect: "webhooks_delivered" },
  ];
  for (const c of controls) {
    log(`part 4: sabotage ${c.sabotage}`);
    const stack = new Stack({ dbs: DBS, basePort: BASE, withProxies: true, env: FAST_RECOVERY });
    await stack.start();
    await SABOTAGE[c.sabotage](stack);
    const acks = newAcks();
    try {
      const merchants = await setupMerchants(stack, 1, "sb");
      const m = new Merchant(stack, merchants[0], acks);
      stack.setProxy(c.which, { dropResponseRate: 0.3 });
      const flows = Promise.all([0, 1, 2].map(() => fullFlow(m).catch(() => {})));
      await Promise.race([flows, sleep(20_000)]);
      stack.setProxy(c.which, { dropResponseRate: 0 });
      await quiesce(stack, 8000);
      const report = await checkInvariants(stack.pools);
      const caught = report.violations.some((v) => v.startsWith(c.expect));
      results.push({ name: `sabotage ${c.sabotage} caught as ${c.expect}`, fired: caught, quiesceMs: null, violations: caught ? [] : [`oracle missed ${c.sabotage}: expected ${c.expect}`], checked: { violationsSeen: report.violations.length } });
      log(`  saw ${report.violations.length} violations; expected kind ${c.expect}: ${caught ? "caught" : "MISSED"}`);
      log(`  e.g. ${report.violations[0]}`);
    } finally {
      await stack.stop();
    }
  }
}

const results: Result[] = [];
const started = Date.now();
if (only === "all" || only === "1") await part1(results);
if (only === "all" || only === "2") await part2(results);
if (only === "all" || only === "3") await part3(results);
if (only === "all" || only === "4") await part4(results);

const failing = results.filter((r) => r.violations.length > 0);
const notFired = results.filter((r) => !r.fired);
const summary = {
  profile,
  durationSeconds: Math.round((Date.now() - started) / 1000),
  scenarios: results.length,
  scenariosWithViolations: failing.length,
  scenariosNotExercised: notFired.map((r) => r.name),
  maxQuiesceMs: Math.max(0, ...results.map((r) => r.quiesceMs ?? 0)),
  results,
};
writeFileSync(path.resolve(import.meta.dirname, `../results/chaos-${profile}${only === "all" ? "" : `-part${only}`}.json`), JSON.stringify(summary, null, 2));
log(`done: ${results.length} scenarios, ${failing.length} with violations, ${notFired.length} not exercised, ${summary.durationSeconds} s`);
for (const r of failing) log(`FAILED ${r.name}: ${r.violations.length} violations`);
for (const r of notFired) log(`NOT EXERCISED ${r.name}`);
process.exit(failing.length || notFired.length ? 1 : 0);
