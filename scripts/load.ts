// Closed-loop load test: N clients each run authorize → capture as fast as they can, against the real processes with no
// injected faults. Reports what was measured, then verifies the invariants on what the run left behind.
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { checkInvariants } from "../src/invariants.ts";
import { quiesce, settleOnce, setupMerchants } from "./lib/driver.ts";
import { Stack } from "./lib/stack.ts";

const seconds = Number(process.env.LOAD_SECONDS ?? 15);
const runs = Number(process.env.LOAD_RUNS ?? 2);
const concurrency = Number(process.env.LOAD_CONCURRENCY ?? 20);
const DBS = { processor: "l_processor", network: "l_network", sink: "l_sink", ledger: "l_ledger" };

interface Scenario {
  name: string;
  shards: number;
  merchants: number;
  networkLatency: [number, number];
}

const SCENARIOS: Scenario[] = [
  { name: "16 shards, 20 merchants", shards: 16, merchants: 20, networkLatency: [0, 0] },
  { name: "1 shard (hot accounts), 20 merchants", shards: 1, merchants: 20, networkLatency: [0, 0] },
  { name: "16 shards, 1 merchant (hot pending)", shards: 16, merchants: 1, networkLatency: [0, 0] },
  { name: "16 shards, 20 merchants, network 20–50 ms", shards: 16, merchants: 20, networkLatency: [20, 50] },
];

function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return NaN;
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}
const stats = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  return { n: s.length, p50: +pct(s, 50).toFixed(1), p99: +pct(s, 99).toFixed(1) };
};

async function timed(url: string, body: object, ok: number[]): Promise<{ ms: number; json: { id?: string }; failed: boolean }> {
  const t0 = performance.now();
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json", "idempotency-key": randomUUID() },
    body: JSON.stringify(body),
  });
  const json = (await res.json()) as { id?: string };
  return { ms: performance.now() - t0, json, failed: !ok.includes(res.status) };
}

// The Docker VM's disk occasionally stalls when the host is busy. Postgres logs how long each checkpoint's sync took, so
// a run hit by a stall can be identified from evidence rather than by its numbers looking bad.
function maxCheckpointSyncSeconds(sinceIso: string): Promise<number | null> {
  return new Promise((resolve) => {
    execFile("docker", ["logs", "--since", sinceIso, "payments-sim-db-1"], { maxBuffer: 50 * 1024 * 1024 }, (err, stdout, stderr) => {
      if (err) return resolve(null);
      const syncs = [...(stdout + stderr).matchAll(/checkpoint complete:.*sync=([0-9.]+) s/g)].map((m) => Number(m[1]));
      resolve(syncs.length ? Math.max(...syncs) : 0);
    });
  });
}

async function runScenario(sc: Scenario) {
  const windowStart = new Date().toISOString();
  // A 100 ms timer that fires seconds late means this whole machine paused (host sleep, VM freeze). Such a run measures
  // the laptop rather than the system, so it's labelled invalid. It stays in the results file.
  let lastTick = performance.now();
  let maxPauseMs = 0;
  const pauseWatch = setInterval(() => {
    const now = performance.now();
    maxPauseMs = Math.max(maxPauseMs, now - lastTick - 100);
    lastTick = now;
  }, 100);
  const loadBefore = os.loadavg()[0];
  const stack = new Stack({
    dbs: DBS,
    basePort: 3300,
    env: { LEDGER_SHARDS: String(sc.shards) },
    networkFaults: { latencyMs: sc.networkLatency },
  });
  await stack.start();
  try {
    const merchants = await setupMerchants(stack, sc.merchants, "l");
    const auth: number[] = [];
    const capture: number[] = [];
    const flow: number[] = [];
    let failed = 0;
    let flows = 0;
    // Commits across all four databases: every durable step, ledger posting and webhook record is one.
    const commits = async () => {
      const { rows } = await stack.pools.processor.query<{ n: bigint }>(
        "select sum(xact_commit)::bigint as n from pg_stat_database where datname = any($1)",
        [Object.values(DBS)],
      );
      return rows[0].n;
    };
    await new Promise((r) => setTimeout(r, 1000)); // stats are published with a small delay
    const commitsBefore = await commits();
    const cpu: number[] = [];
    const procCpu: Record<string, number[]> = {};
    const sampler = setInterval(() => {
      execFile("docker", ["stats", "--no-stream", "--format", "{{.CPUPerc}}", "payments-sim-db-1"], (err, out) => {
        if (!err) cpu.push(parseFloat(out));
      });
      for (const [name, svc] of stack.services) {
        if (!svc.child?.pid) continue;
        execFile("ps", ["-o", "pcpu=", "-p", String(svc.child.pid)], (err, out) => {
          if (!err) (procCpu[name] ??= []).push(parseFloat(out));
        });
      }
    }, 3000);
    // docker stats sees only the Postgres container. /proc/stat sees the whole VM, sshd and docker-proxy included.
    const vmStat = () => new Promise<number[] | null>((resolve) => {
      execFile("docker", ["exec", "payments-sim-db-1", "head", "-1", "/proc/stat"], (err, out) => resolve(err ? null : out.trim().split(/\s+/).slice(1, 9).map(Number)));
    });
    const vm0 = await vmStat();
    let hostIdlePercent: number | null = null;
    setTimeout(() => {
      execFile("top", ["-l", "2", "-n", "0", "-s", "3"], (err, out) => {
        const m = [...(err ? "" : out).matchAll(/([0-9.]+)% idle/g)].pop();
        if (m) hostIdlePercent = Number(m[1]);
      });
    }, 4000);
    const started = performance.now();
    const wallStart = Date.now();
    const stopAt = started + seconds * 1000;
    await Promise.all(
      Array.from({ length: concurrency }, async (_, i) => {
        const merchantId = merchants[i % merchants.length];
        while (performance.now() < stopAt) {
          const t0 = performance.now();
          const a = await timed(`${stack.apiUrl}/v1/payments`, { merchantId, amount: "5000", cardToken: "tok_ok" }, [201]);
          auth.push(a.ms);
          if (a.failed) {
            failed++;
            continue;
          }
          const c = await timed(`${stack.apiUrl}/v1/payments/${a.json.id}/capture`, {}, [201]);
          capture.push(c.ms);
          if (c.failed) {
            failed++;
            continue;
          }
          flow.push(performance.now() - t0);
          flows++;
        }
      }),
    );
    const elapsed = (performance.now() - started) / 1000;
    clearInterval(sampler);
    const vm1 = await vmStat();
    const vmIdlePercent = vm0 && vm1 ? +((100 * (vm1[3] - vm0[3])) / vm1.reduce((a, v, i) => a + v - vm0[i], 0)).toFixed(1) : null;
    const quiesceMs = await quiesce(stack, 120_000);
    await new Promise((r) => setTimeout(r, 1000));
    const commitsAfter = await commits();

    const t0 = performance.now();
    const settled = await settleOnce(stack);
    await quiesce(stack, 120_000);
    const reconMs = performance.now() - t0;

    const delivery = await stack.pools.processor.query<{ p50: number; p99: number; n: number }>(
      `select percentile_cont(0.5) within group (order by extract(epoch from delivered_at - created_at) * 1000) as p50,
              percentile_cont(0.99) within group (order by extract(epoch from delivered_at - created_at) * 1000) as p99,
              count(*)::int as n from outbox where type like 'payment.%'`,
    );
    const report = await checkInvariants(stack.pools);
    return {
      scenario: sc.name,
      concurrency,
      seconds: +elapsed.toFixed(1),
      flows,
      flowsPerSec: +(flows / elapsed).toFixed(0),
      requestsPerSec: +((auth.length + capture.length) / elapsed).toFixed(0),
      failed,
      commitsPerFlow: +(Number(commitsAfter - commitsBefore) / Math.max(flows, 1)).toFixed(1),
      vmIdlePercent,
      hostIdlePercent,
      postgresCpuPercent: +(cpu.reduce((x, y) => x + y, 0) / Math.max(cpu.length, 1)).toFixed(0),
      // 100 = one core. Node runs each service on one thread, so a service near 100 is saturated.
      processCpuPercent: Object.fromEntries(Object.entries(procCpu).map(([k, v]) => [k, +(v.reduce((x, y) => x + y, 0) / v.length).toFixed(0)])),
      authorizeMs: stats(auth),
      captureMs: stats(capture),
      flowMs: stats(flow),
      webhookDeliveryMs: { n: delivery.rows[0].n, p50: +Number(delivery.rows[0].p50).toFixed(0), p99: +Number(delivery.rows[0].p99).toFixed(0) },
      backlogDrainMs: quiesceMs,
      recon: { lines: settled.report.lines, matched: settled.report.matched, exceptions: settled.report.exceptions.length, ms: +reconMs.toFixed(0) },
      maxPauseMs: +maxPauseMs.toFixed(0),
      valid: maxPauseMs < 2000 && wallClockGapOk(wallStart, elapsed),
      hostLoadAverage: { before: +loadBefore.toFixed(1), after: +os.loadavg()[0].toFixed(1) },
      maxCheckpointSyncSeconds: await maxCheckpointSyncSeconds(windowStart),
      violations: report.violations.length,
      violationExamples: report.violations.slice(0, 5),
    };
  } finally {
    clearInterval(pauseWatch);
    await stack.stop();
  }
}

// Darwin's monotonic clock can stop during sleep, so also compare it with the wall clock over the measured window.
function wallClockGapOk(wallStart: number, elapsedSeconds: number): boolean {
  return Math.abs((Date.now() - wallStart) / 1000 - elapsedSeconds) < 30;
}

// Every run is kept, including failures, and written as it finishes. Nothing is dropped for looking bad.
const out: object[] = [];
const filter = process.env.LOAD_ONLY;
const save = () => {
  if (!filter) writeFileSync(path.resolve(import.meta.dirname, "../results/load.json"), JSON.stringify({ seconds, runs, concurrency, out }, null, 2));
};
for (const sc of SCENARIOS.filter((x) => !filter || x.name.includes(filter))) {
  for (let run = 1; run <= runs; run++) {
    try {
      const r = await runScenario(sc);
      out.push({ run, ...r });
      console.log(
        `${sc.name} (run ${run}): ${r.flowsPerSec} flows/s, ${r.requestsPerSec} req/s, auth p50 ${r.authorizeMs.p50} p99 ${r.authorizeMs.p99}, capture p50 ${r.captureMs.p50} p99 ${r.captureMs.p99}, flow p50 ${r.flowMs.p50} p99 ${r.flowMs.p99}, failed ${r.failed}, commits/flow ${r.commitsPerFlow}, pg cpu ${r.postgresCpuPercent}%, webhook p50 ${r.webhookDeliveryMs.p50} p99 ${r.webhookDeliveryMs.p99}, recon ${r.recon.lines} lines ${r.recon.ms} ms (${r.recon.exceptions} exc), drain ${r.backlogDrainMs} ms, violations ${r.violations}, vm idle ${r.vmIdlePercent}%, host idle ${r.hostIdlePercent}%, host load ${r.hostLoadAverage.before}→${r.hostLoadAverage.after}, max ckpt sync ${r.maxCheckpointSyncSeconds} s, max pause ${r.maxPauseMs} ms, valid ${r.valid}`,
      );
      for (const v of r.violationExamples) console.log(`   ${v}`);
    } catch (err) {
      out.push({ run, scenario: sc.name, error: (err as Error).message });
      console.log(`${sc.name} (run ${run}): FAILED: ${(err as Error).message}`);
    }
    save();
  }
}
process.exit(0);
