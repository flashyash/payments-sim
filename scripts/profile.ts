// Measures where a load run's time goes, for the throughput investigation in the README. Same closed loop as load.ts.
// Needs docker-compose.profile.yml applied. Writes results/profiling/<label>.json.
//
//   PROFILE_LABEL=baseline PROFILE_CPU=1 node scripts/profile.ts
import { execFile, execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import { performance } from "node:perf_hooks";
import path from "node:path";
import { checkInvariants } from "../src/invariants.ts";
import { quiesce, setupMerchants } from "./lib/driver.ts";
import { Stack, type ServiceName } from "./lib/stack.ts";
import pg from "pg";

const label = process.env.PROFILE_LABEL ?? "run";
const seconds = Number(process.env.LOAD_SECONDS ?? 15);
const concurrency = Number(process.env.LOAD_CONCURRENCY ?? 20);
const merchantsN = Number(process.env.PROFILE_MERCHANTS ?? 20);
const container = process.env.PROFILE_CONTAINER ?? "payments-sim-db-1";
const inVm = process.env.PROFILE_NATIVE_PG !== "1";
const DBS = { processor: "l_processor", network: "l_network", sink: "l_sink", ledger: "l_ledger" };
const ROOT = path.resolve(import.meta.dirname, "..");
const scratch = path.join(os.tmpdir(), `profile-${label}-${process.pid}`);
rmSync(scratch, { recursive: true, force: true });
mkdirSync(scratch, { recursive: true });
const OUT = path.join(ROOT, "results", "profiling");
mkdirSync(OUT, { recursive: true });

const preload = path.join(ROOT, "scripts/lib/profile-preload.mjs");
const service = (s: ServiceName) => ({ PROFILE_SERVICE: s });
const stack = new Stack({
  dbs: DBS,
  basePort: 3300,
  env: {
    NODE_OPTIONS: `--import ${preload}`,
    PROFILE_DIR: scratch,
    PROFILE_CPU: process.env.PROFILE_CPU ?? "0",
    LEDGER_SHARDS: process.env.LEDGER_SHARDS ?? "16",
  },
  serviceEnv: { api: service("api"), worker: service("worker"), ledger: service("ledger"), network: service("network"), sink: service("sink") },
});

function psCpuSeconds(pid: number): number | null {
  try {
    const out = execFileSync("ps", ["-o", "time=", "-p", String(pid)]).toString().trim();
    const parts = out.split(":").map(Number);
    return parts.length === 3 ? parts[0] * 3600 + parts[1] * 60 + parts[2] : parts[0] * 60 + parts[1];
  } catch {
    return null;
  }
}
const pgrep = (pattern: string) => {
  try {
    return execFileSync("pgrep", ["-f", pattern]).toString().trim().split("\n").map(Number);
  } catch {
    return [];
  }
};
// The forwarder and the VM, by exact CPU time consumed over the window rather than ps's decaying %cpu.
const hostProcs: Record<string, number[]> = {
  sshMux: pgrep("colima/ssh.sock \\[mux\\]"),
  limaHostagent: pgrep("limactl hostagent"),
  limaUsernet: pgrep("limactl usernet"),
  vm: pgrep("com.apple.Virtualization.VirtualMachine"),
};
const self = { client: [process.pid] };

function vmStat(): Promise<number[] | null> {
  if (!inVm) return Promise.resolve(null);
  return new Promise((resolve) => {
    execFile("docker", ["exec", container, "head", "-1", "/proc/stat"], (err, out) => {
      if (err) return resolve(null);
      resolve(out.trim().split(/\s+/).slice(1).map(Number));
    });
  });
}

process.on("uncaughtException", async (err) => {
  console.error(err);
  await stack.stop().catch(() => {});
  process.exit(1);
});
// CPU inside the VM by process name and for the Postgres container's cgroup: docker stats only ever sees the latter.
const vmProcScript = String.raw`cat /sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' ${container}).scope/cpu.stat | awk '$1=="usage_usec"{print "CONTAINER_USEC", $2}'; for f in /proc/[0-9]*/stat; do cat $f 2>/dev/null; done | awk '{c=$2; gsub(/[()]/,"",c); sub(/\/.*/,"",c); t[c]+=$14+$15} END {for (k in t) print k, t[k]}'`;
function vmProcs(): Promise<Record<string, number> | null> {
  if (!inVm || process.env.PROFILE_VM_PROCS === "0") return Promise.resolve(null);
  return new Promise((resolve) => {
    execFile("colima", ["ssh", "--", "sh", "-c", vmProcScript], { maxBuffer: 1 << 22 }, (err, out) => {
      if (err) return resolve(null);
      resolve(Object.fromEntries(out.trim().split("\n").map((l) => { const i = l.lastIndexOf(" "); return [l.slice(0, i), Number(l.slice(i + 1))]; })));
    });
  });
}

await stack.start();
const merchants = await setupMerchants(stack, merchantsN, "p");
const admin = new pg.Client({ connectionString: process.env.PG_BASE_URL ? `${process.env.PG_BASE_URL}/postgres` : "postgres://payments:payments@localhost:5434/postgres" });
await admin.connect();
await admin.query("create extension if not exists pg_stat_statements");
await admin.query("select pg_stat_statements_reset()");
await admin.query("select pg_stat_reset_shared('wal')");
const dbList = Object.values(DBS);
const dbStat = async () =>
  (await admin.query("select sum(xact_commit)::bigint as commits, sum(xact_rollback)::bigint as rollbacks, sum(blk_read_time) as read_ms, sum(blk_write_time) as write_ms from pg_stat_database where datname = any($1)", [dbList])).rows[0];
await new Promise((r) => setTimeout(r, 1000));
const dbBefore = await dbStat();

// Wait events: what every active backend is doing, sampled. CPU shows as a null wait_event.
const waits: Record<string, number> = {};
const stateCounts: Record<string, number> = {};
let samples = 0;
const sampling = setInterval(async () => {
  try {
    const { rows } = await admin.query(
      `select state, coalesce(wait_event_type || ':' || wait_event, 'CPU') as w, count(*)::int as n from pg_stat_activity
        where datname = any($1) group by 1, 2`,
      [dbList],
    );
    samples++;
    for (const r of rows) {
      stateCounts[r.state ?? "null"] = (stateCounts[r.state ?? "null"] ?? 0) + r.n;
      if (r.state === "active") waits[r.w] = (waits[r.w] ?? 0) + r.n;
    }
  } catch {}
}, 50);

const childProcs = () => Object.fromEntries([...stack.services].map(([n, s]) => [n, [s.child!.pid!]]));
const allProcs = () => ({ ...hostProcs, ...self, ...childProcs() });
const cpuTimes = () => Object.fromEntries(Object.entries(allProcs()).map(([k, pids]) => [k, pids.map(psCpuSeconds).reduce<number>((a, b) => a + (b ?? 0), 0)]));

const vmp0 = await vmProcs();
const vm0 = await vmStat();
const cpu0 = cpuTimes();
const elu0 = performance.eventLoopUtilization();
writeFileSync(path.join(scratch, "start"), "");
await new Promise((r) => setTimeout(r, 250));
// Is the machine itself busy with something else during the window? Sampled mid-run.
let hostTop = "";
setTimeout(() => {
  execFile("top", ["-l", "2", "-n", "10", "-o", "cpu", "-s", "3", "-stats", "command,cpu"], (err, out) => {
    if (!err) hostTop = out.split("CPU usage:").pop()!.trim();
  });
}, 4000);
const auth: number[] = [];
const capture: number[] = [];
let failed = 0;
let flows = 0;
const started = performance.now();
const stopAt = started + seconds * 1000;
async function timed(url: string, body: object, ok: number): Promise<{ ms: number; id?: string; ok: boolean }> {
  const t0 = performance.now();
  const res = await fetch(url, { method: "POST", headers: { "content-type": "application/json", "idempotency-key": randomUUID() }, body: JSON.stringify(body) });
  const json = (await res.json()) as { id?: string };
  return { ms: performance.now() - t0, id: json.id, ok: res.status === ok };
}
await Promise.all(
  Array.from({ length: concurrency }, async (_, i) => {
    const merchantId = merchants[i % merchants.length];
    while (performance.now() < stopAt) {
      const a = await timed(`${stack.apiUrl}/v1/payments`, { merchantId, amount: "5000", cardToken: "tok_ok" }, 201);
      auth.push(a.ms);
      if (!a.ok) { failed++; continue; }
      const c = await timed(`${stack.apiUrl}/v1/payments/${a.id}/capture`, {}, 201);
      capture.push(c.ms);
      if (!c.ok) { failed++; continue; }
      flows++;
    }
  }),
);
const elapsed = (performance.now() - started) / 1000;
const cpu1 = cpuTimes();
const vm1 = await vmStat();
const vmp1 = await vmProcs();
const clientElu = performance.eventLoopUtilization(elu0);
writeFileSync(path.join(scratch, "stop"), "");
clearInterval(sampling);

// Everything below is read before the backlog drains, so the query counts match the measured window closely.
const statements = (
  await admin.query(
    `select d.datname as db, s.calls::int, s.total_exec_time as exec_ms, s.total_plan_time as plan_ms, s.plans::int,
            s.mean_exec_time as mean_ms, s.max_exec_time as max_ms, s.rows::bigint, s.wal_bytes::bigint,
            s.shared_blk_read_time as read_ms, s.shared_blk_write_time as write_ms, s.query
       from pg_stat_statements s join pg_database d on d.oid = s.dbid
      where d.datname = any($1)
      order by s.total_exec_time + s.total_plan_time desc`,
    [dbList],
  )
).rows;
await new Promise((r) => setTimeout(r, 1000));
await quiesce(stack, 60_000);
await new Promise((r) => setTimeout(r, 1500));
const dbAfter = await dbStat();
const wal = (await admin.query("select wal_records::bigint, wal_bytes::bigint, wal_write::bigint, wal_sync::bigint, wal_write_time, wal_sync_time from pg_stat_wal")).rows[0];
const report = await checkInvariants(stack.pools);

const cpuPercent = Object.fromEntries(Object.keys(cpu1).map((k) => [k, +((100 * (cpu1[k] - cpu0[k])) / elapsed).toFixed(1)]));
let vmCpu: object | null = null;
if (vm0 && vm1) {
  const d = vm1.map((v, i) => v - vm0[i]);
  const names = ["user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"];
  const total = d.slice(0, 8).reduce((a, b) => a + b, 0);
  vmCpu = Object.fromEntries(names.map((n, i) => [n, +((100 * d[i]) / total).toFixed(1)]));
}
let vmProcessCpuPercent: Record<string, number> | null = null;
if (vmp0 && vmp1) {
  const rows = Object.keys(vmp1).map((k) => [k, ((k === "CONTAINER_USEC" ? (vmp1[k] - (vmp0[k] ?? 0)) / 1e4 : vmp1[k] - (vmp0[k] ?? 0)) / elapsed)] as [string, number]);
  vmProcessCpuPercent = Object.fromEntries(rows.filter(([, v]) => v >= 0.5).sort((a, b) => b[1] - a[1]).map(([k, v]) => [k, +v.toFixed(1)]));
}
const services: Record<string, unknown>[] = [];
await new Promise((r) => setTimeout(r, 400));
for (const f of readdirSync(scratch).filter((x) => x.endsWith(".json"))) services.push(JSON.parse(readFileSync(path.join(scratch, f), "utf8")));
const pct = (xs: number[], p: number) => { const s = [...xs].sort((a, b) => a - b); return +s[Math.min(s.length - 1, Math.floor((p / 100) * s.length))].toFixed(1); };
const totalQueries = statements.reduce((a, s) => a + s.calls, 0);
const summary = {
  label,
  at: new Date().toISOString(),
  concurrency,
  seconds: +elapsed.toFixed(1),
  flows,
  flowsPerSec: +(flows / elapsed).toFixed(1),
  failed,
  authorizeMs: { p50: pct(auth, 50), p99: pct(auth, 99) },
  captureMs: { p50: pct(capture, 50), p99: pct(capture, 99) },
  commitsPerFlow: +(Number(dbAfter.commits - dbBefore.commits) / Math.max(flows, 1)).toFixed(1),
  rollbacks: Number(dbAfter.rollbacks - dbBefore.rollbacks),
  statementsPerFlow: +(totalQueries / Math.max(flows, 1)).toFixed(1),
  pgExecMsPerFlow: +(statements.reduce((a, s) => a + s.exec_ms, 0) / Math.max(flows, 1)).toFixed(2),
  pgPlanMsPerFlow: +(statements.reduce((a, s) => a + s.plan_ms, 0) / Math.max(flows, 1)).toFixed(2),
  hostCpuPercentExact: cpuPercent,
  vmCpuPercent: vmCpu,
  vmProcessCpuPercent,
  clientEventLoopUtilization: +clientElu.utilization.toFixed(3),
  waitSamples: samples,
  avgActiveBackendsByWait: Object.fromEntries(Object.entries(waits).sort((a, b) => b[1] - a[1]).map(([k, v]) => [k, +(v / Math.max(samples, 1)).toFixed(2)])),
  avgBackendsByState: Object.fromEntries(Object.entries(stateCounts).map(([k, v]) => [k, +(v / Math.max(samples, 1)).toFixed(2)])),
  wal,
  blockIo: { readMs: dbAfter.read_ms - dbBefore.read_ms, writeMs: dbAfter.write_ms - dbBefore.write_ms },
  violations: report.violations.length,
  services,
  statements: statements.slice(0, 40).map((s) => ({ ...s, query: s.query.replace(/\s+/g, " ").slice(0, 120) })),
  loadAverage: os.loadavg(),
  hostTop,
};
writeFileSync(path.join(OUT, `${label}.json`), JSON.stringify(summary, (_k, v) => (typeof v === "bigint" ? Number(v) : v), 2));
for (const f of readdirSync(scratch).filter((x) => x.endsWith(".cpuprofile"))) writeFileSync(path.join(OUT, `${label}-${f}`), readFileSync(path.join(scratch, f)));
const svc = (n: string) => services.find((s) => s.name === n) as { eventLoopUtilization: number; cpuPercent: number; eventLoopDelayMs: { p99: number }; poolWait: { n: number; totalMs: number }; queries: { n: number; totalMs: number } } | undefined;
console.log(`${label}: ${summary.flowsPerSec} flows/s  auth p50/p99 ${summary.authorizeMs.p50}/${summary.authorizeMs.p99}  capture ${summary.captureMs.p50}/${summary.captureMs.p99}  failed ${failed}  violations ${summary.violations}`);
console.log(`  commits/flow ${summary.commitsPerFlow}  statements/flow ${summary.statementsPerFlow}  pg exec ms/flow ${summary.pgExecMsPerFlow}  plan ms/flow ${summary.pgPlanMsPerFlow}`);
console.log(`  host cpu% ${JSON.stringify(cpuPercent)}`);
console.log(`  vm cpu% ${JSON.stringify(vmCpu)}`);
console.log(`  vm procs (100 = one vCPU) ${JSON.stringify(vmProcessCpuPercent)}`);
console.log(`  wal sync n ${wal.wal_sync} mean ${(wal.wal_sync_time / Math.max(Number(wal.wal_sync), 1)).toFixed(2)} ms`);
console.log(`  client ELU ${summary.clientEventLoopUtilization}`);
for (const n of ["api", "worker", "ledger", "network", "sink"]) {
  const s = svc(n);
  if (s) console.log(`  ${n.padEnd(8)} ELU ${s.eventLoopUtilization.toFixed(3)} cpu ${s.cpuPercent.toFixed(0)}% eld p99 ${s.eventLoopDelayMs.p99.toFixed(1)} ms  poolwait n ${s.poolWait.n} mean ${(s.poolWait.totalMs / Math.max(s.poolWait.n, 1)).toFixed(2)} ms  queries n ${s.queries.n} mean ${(s.queries.totalMs / Math.max(s.queries.n, 1)).toFixed(2)} ms`);
}
console.log(`  avg active backends by wait ${JSON.stringify(summary.avgActiveBackendsByWait)}`);
console.log(`  backends by state ${JSON.stringify(summary.avgBackendsByState)}`);
console.log(`  host ${hostTop.split("\n")[0]}`);
await admin.end();
await stack.stop();
process.exit(0);
