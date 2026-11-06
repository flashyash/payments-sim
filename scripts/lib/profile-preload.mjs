// Loaded into each service with NODE_OPTIONS=--import by scripts/profile.ts, so no service code changes to be measured.
// Records, per process, between "start" and "stop" marker files: CPU, event-loop utilization and delay, time waiting for a
// pooled Postgres connection, client-side query time, and outbound HTTP time. Written every second because the
// harness ends services with SIGKILL.
import { existsSync, writeFileSync } from "node:fs";
import inspector from "node:inspector";
import { createRequire } from "node:module";
import { monitorEventLoopDelay, performance } from "node:perf_hooks";
import path from "node:path";

const dir = process.env.PROFILE_DIR;
const name = process.env.PROFILE_SERVICE ?? path.basename(process.argv[1] ?? "unknown", ".ts");
const wantCpuProfile = process.env.PROFILE_CPU === "1";
if (dir) {
  const require = createRequire(path.join(process.cwd(), "package.json"));
  const histo = () => ({ n: 0, totalMs: 0, maxMs: 0, buckets: new Array(20).fill(0) });
  const add = (h, ms) => {
    h.n++;
    h.totalMs += ms;
    h.maxMs = Math.max(h.maxMs, ms);
    h.buckets[Math.min(19, Math.max(0, Math.ceil(Math.log2(Math.max(ms, 0.03125)) + 5)))]++; // 2^(i-5) ms upper bound
  };
  let on = false;
  const poolWait = histo();
  const queries = histo();
  const bySql = new Map();
  const http = new Map();
  let poolHighWater = 0;
  const pools = new Set();

  try {
    const pg = require("pg");
    const connect = pg.Pool.prototype.connect;
    pg.Pool.prototype.connect = function (cb) {
      pools.add(this);
      const t0 = performance.now();
      if (cb) return connect.call(this, (...a) => { if (on) add(poolWait, performance.now() - t0); cb(...a); });
      return connect.call(this).then((c) => { if (on) add(poolWait, performance.now() - t0); return c; });
    };
    const query = pg.Client.prototype.query;
    pg.Client.prototype.query = function (...args) {
      const text = typeof args[0] === "string" ? args[0] : args[0]?.text ?? "?";
      const t0 = performance.now();
      const done = () => {
        if (!on) return;
        const ms = performance.now() - t0;
        add(queries, ms);
        const key = text.replace(/\s+/g, " ").slice(0, 70);
        const h = bySql.get(key) ?? histo();
        add(h, ms);
        bySql.set(key, h);
      };
      const last = args.length - 1;
      if (typeof args[last] === "function") {
        const cb = args[last];
        args[last] = (...a) => { done(); cb(...a); };
        return query.apply(this, args);
      }
      const out = query.apply(this, args);
      if (out && typeof out.then === "function") return out.finally(done);
      return out;
    };
  } catch (err) {
    console.error("[profile] pg not wrapped", err);
  }

  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    const t0 = performance.now();
    try {
      return await realFetch(url, init);
    } finally {
      if (on) {
        const u = new URL(String(url));
        const key = `${init?.method ?? "GET"} ${u.port} ${u.pathname.replace(/[0-9a-f-]{36}/g, ":id").replace(/\/hooks\/.*/, "/hooks/:m")}`;
        const h = http.get(key) ?? histo();
        add(h, performance.now() - t0);
        http.set(key, h);
      }
    }
  };

  const eld = monitorEventLoopDelay({ resolution: 1 });
  let elu0, cpu0, wall0, session;
  const write = () => {
    if (!elu0) return;
    const elu = performance.eventLoopUtilization(elu0);
    const cpu = process.cpuUsage(cpu0);
    const wallMs = performance.now() - wall0;
    for (const p of pools) poolHighWater = Math.max(poolHighWater, p.waitingCount);
    writeFileSync(
      path.join(dir, `${name}-${process.pid}.json`),
      JSON.stringify({
        name,
        pid: process.pid,
        wallMs,
        cpuPercent: (100 * (cpu.user + cpu.system)) / 1000 / wallMs,
        eventLoopUtilization: elu.utilization,
        eventLoopDelayMs: { p50: eld.percentile(50) / 1e6, p99: eld.percentile(99) / 1e6, max: eld.max / 1e6 },
        poolWait,
        poolWaitingHighWater: poolHighWater,
        poolSizes: [...pools].map((p) => ({ total: p.totalCount, idle: p.idleCount, waiting: p.waitingCount, max: p.options.max })),
        queries,
        bySql: Object.fromEntries([...bySql].sort((a, b) => b[1].totalMs - a[1].totalMs).slice(0, 60)),
        http: Object.fromEntries(http),
      }),
    );
  };
  const startMarker = path.join(dir, "start");
  const stopMarker = path.join(dir, "stop");
  const timer = setInterval(() => {
    if (!on && !elu0 && existsSync(startMarker)) {
      on = true;
      eld.enable();
      elu0 = performance.eventLoopUtilization();
      cpu0 = process.cpuUsage();
      wall0 = performance.now();
      if (wantCpuProfile) {
        session = new inspector.Session();
        session.connect();
        session.post("Profiler.enable", () => session.post("Profiler.start"));
      }
    }
    if (on) {
      if (existsSync(stopMarker)) {
        on = false;
        write();
        clearInterval(timer);
        if (session) {
          session.post("Profiler.stop", (err, res) => {
            if (!err) writeFileSync(path.join(dir, `${name}-${process.pid}.cpuprofile`), JSON.stringify(res.profile));
          });
        }
      } else write();
    }
  }, 100);
  timer.unref();
}
