import { spawn, type ChildProcess } from "node:child_process";
import { createWriteStream, mkdirSync } from "node:fs";
import path from "node:path";
import { dbUrl } from "../../src/config.ts";
import { createPool, type Pool } from "../../src/db.ts";
import { startProxy, type ProxyFaults } from "../../src/faultProxy.ts";
import type { InvariantPools } from "../../src/invariants.ts";
import { DEFAULT_DBS, LEDGER_DIR, setupDbs, type DbNames } from "../setup-dbs.ts";

// Starts the whole system as real OS processes that can be SIGKILLed and restarted.

const ROOT = path.resolve(import.meta.dirname, "../..");
const LOGS = path.join(ROOT, "results", "tmp");

export type ServiceName = "api" | "worker" | "ledger" | "network" | "sink";

export interface StackOptions {
  dbs?: DbNames;
  basePort?: number;
  withProxies?: boolean;
  env?: NodeJS.ProcessEnv;
  serviceEnv?: Partial<Record<ServiceName, NodeJS.ProcessEnv>>;
  networkFaults?: object;
  sinkFaults?: object;
  fresh?: boolean;
}

export const FAST_RECOVERY: NodeJS.ProcessEnv = {
  LEASE_MS: "2000",
  HTTP_TIMEOUT_MS: "1000",
  AUTH_DEADLINE_MS: "3000",
  RETRY_BASE_MS: "50",
  RETRY_MAX_MS: "500",
  WEBHOOK_RETRY_BASE_MS: "50",
  WEBHOOK_RETRY_MAX_MS: "500",
  WEBHOOK_TIMEOUT_MS: "1000",
};

interface Service {
  name: ServiceName;
  cwd: string;
  script: string;
  env: NodeJS.ProcessEnv;
  port: number | null;
  child?: ChildProcess;
  autoRestart: boolean;
  /** Env for this one incarnation only, e.g. a failpoint that must not fire again after the restart. */
  onceEnv?: NodeJS.ProcessEnv;
  killedAt: string[];
  restarts: number;
  stopping: boolean;
}

export class Stack {
  opts: StackOptions;
  dbs: DbNames;
  ports: { api: number; ledger: number; network: number; sink: number; ledgerProxy: number; networkProxy: number };
  services = new Map<ServiceName, Service>();
  ledgerProxy?: Awaited<ReturnType<typeof startProxy>>;
  networkProxy?: Awaited<ReturnType<typeof startProxy>>;
  pools!: InvariantPools;
  apiUrl: string;
  networkUrl: string;
  sinkUrl: string;
  ledgerUrl: string;

  constructor(opts: StackOptions = {}) {
    this.opts = opts;
    this.dbs = opts.dbs ?? DEFAULT_DBS;
    const base = opts.basePort ?? 3100;
    this.ports = { api: base, ledger: base + 1, network: base + 2, sink: base + 3, ledgerProxy: base + 11, networkProxy: base + 12 };
    this.apiUrl = `http://127.0.0.1:${this.ports.api}`;
    this.networkUrl = `http://127.0.0.1:${this.ports.network}`;
    this.sinkUrl = `http://127.0.0.1:${this.ports.sink}`;
    this.ledgerUrl = `http://127.0.0.1:${this.ports.ledger}`;
    mkdirSync(LOGS, { recursive: true });
  }

  private define(name: ServiceName, cwd: string, script: string, port: number | null, env: NodeJS.ProcessEnv) {
    this.services.set(name, {
      name,
      cwd,
      script,
      port,
      env: { ...process.env, ...this.opts.env, ...env, ...this.opts.serviceEnv?.[name] },
      autoRestart: true,
      killedAt: [],
      restarts: 0,
      stopping: false,
    });
  }

  async start(): Promise<void> {
    await setupDbs(this.dbs, this.opts.fresh ?? true);
    this.pools = {
      processor: createPool(dbUrl(this.dbs.processor), 5),
      network: createPool(dbUrl(this.dbs.network), 5),
      sink: createPool(dbUrl(this.dbs.sink), 5),
      ledger: createPool(dbUrl(this.dbs.ledger), 5),
    };
    const proxied = this.opts.withProxies ?? false;
    if (proxied) {
      this.ledgerProxy = await startProxy(this.ports.ledger, this.ports.ledgerProxy);
      this.networkProxy = await startProxy(this.ports.network, this.ports.networkProxy);
    }
    const processorEnv = {
      DATABASE_URL: dbUrl(this.dbs.processor),
      LEDGER_URL: `http://127.0.0.1:${proxied ? this.ports.ledgerProxy : this.ports.ledger}`,
      NETWORK_URL: `http://127.0.0.1:${proxied ? this.ports.networkProxy : this.ports.network}`,
    };
    this.define("ledger", LEDGER_DIR, "src/server.ts", this.ports.ledger, { DATABASE_URL: dbUrl(this.dbs.ledger), PORT: String(this.ports.ledger), LOG_LEVEL: "error" });
    this.define("network", ROOT, "src/bin/network.ts", this.ports.network, {
      NETWORK_DATABASE_URL: dbUrl(this.dbs.network),
      PORT: String(this.ports.network),
      PROCESSOR_URL: this.apiUrl,
      NETWORK_FAULTS: JSON.stringify(this.opts.networkFaults ?? {}),
    });
    this.define("sink", ROOT, "src/bin/sink.ts", this.ports.sink, {
      SINK_DATABASE_URL: dbUrl(this.dbs.sink),
      PORT: String(this.ports.sink),
      SINK_FAULTS: JSON.stringify(this.opts.sinkFaults ?? {}),
    });
    this.define("api", ROOT, "src/bin/api.ts", this.ports.api, { ...processorEnv, PORT: String(this.ports.api) });
    this.define("worker", ROOT, "src/bin/worker.ts", null, processorEnv);
    for (const name of ["ledger", "network", "sink", "api", "worker"] as const) this.spawn(this.services.get(name)!);
    for (const name of ["ledger", "network", "sink", "api"] as const) await this.waitHealthy(name);
  }

  private spawn(s: Service): void {
    const log = createWriteStream(path.join(LOGS, `${s.name}.log`), { flags: "a" });
    const child = spawn("node", [s.script], { cwd: s.cwd, env: { ...s.env, ...s.onceEnv }, stdio: ["ignore", "pipe", "pipe"] });
    s.onceEnv = undefined;
    s.child = child;
    child.stdout?.pipe(log);
    child.stderr?.on("data", (d: Buffer) => {
      log.write(d);
      const m = /\[failpoint\] pid \d+ killed at (\S+)/.exec(d.toString());
      if (m) s.killedAt.push(m[1]);
    });
    child.on("exit", () => {
      log.end();
      if (s.child === child && s.autoRestart && !s.stopping) {
        s.restarts++;
        setTimeout(() => {
          if (!s.stopping) this.spawn(s);
        }, 100);
      }
    });
  }

  async waitHealthy(name: ServiceName, timeoutMs = 30_000): Promise<void> {
    const s = this.services.get(name)!;
    if (!s.port) return;
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const ok = await fetch(`http://127.0.0.1:${s.port}/health`, { signal: AbortSignal.timeout(500) }).then((r) => r.ok, () => false);
      if (ok) return;
      await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error(`${name} didn't become healthy`);
  }

  kill(name: ServiceName): void {
    this.services.get(name)?.child?.kill("SIGKILL");
  }

  // Replaces a running service, with env that applies to this run only (failpoints) if asked.
  async respawn(name: ServiceName, onceEnv?: NodeJS.ProcessEnv): Promise<void> {
    const s = this.services.get(name)!;
    s.stopping = true;
    const child = s.child;
    if (child && child.exitCode === null && child.signalCode === null) {
      await new Promise<void>((resolve) => {
        child.once("exit", () => resolve());
        child.kill("SIGKILL");
      });
    }
    s.stopping = false;
    s.onceEnv = onceEnv;
    this.spawn(s);
    await this.waitHealthy(name);
  }

  killedAt(): string[] {
    return [...this.services.values()].flatMap((s) => s.killedAt);
  }

  restarts(): Record<string, number> {
    return Object.fromEntries([...this.services.values()].map((s) => [s.name, s.restarts]));
  }

  setNetworkFaults(f: object) {
    return fetch(`${this.networkUrl}/admin/faults`, { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify(f) });
  }

  setSinkFaults(f: object) {
    return fetch(`${this.sinkUrl}/admin/faults`, { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify(f) });
  }

  setProxy(which: "ledger" | "network", f: Partial<ProxyFaults>) {
    (which === "ledger" ? this.ledgerProxy : this.networkProxy)?.set(f);
  }

  async stop(): Promise<void> {
    for (const s of this.services.values()) {
      s.stopping = true;
      s.child?.kill("SIGKILL");
    }
    await this.ledgerProxy?.close();
    await this.networkProxy?.close();
    await Promise.all(Object.values(this.pools ?? {}).map((p: Pool) => p.end().catch(() => {})));
  }
}
