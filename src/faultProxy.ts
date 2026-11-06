import http from "node:http";

// PLUMBING: sits between the processor and the ledger or network, and loses things on purpose.
// dropRequest: the upstream never sees it. dropResponse: the upstream applied it and the caller never finds out.

export interface ProxiedRequest {
  method: string;
  url: string;
  headers: http.IncomingHttpHeaders;
  body: Buffer;
}

export interface ProxyFaults {
  dropRequestRate: number;
  dropResponseRate: number;
  /** Only requests whose path contains this are affected. Empty = all. */
  pathContains: string;
  /** Changes a request on the wire before it goes upstream, whatever pathContains says. The sender never knows. */
  rewrite: ((req: ProxiedRequest) => void) | null;
}

export const NO_PROXY_FAULTS: ProxyFaults = { dropRequestRate: 0, dropResponseRate: 0, pathContains: "", rewrite: null };

export function startProxy(targetPort: number, listenPort: number, initial: Partial<ProxyFaults> = {}) {
  const faults: ProxyFaults = { ...NO_PROXY_FAULTS, ...initial };
  const stats = { forwarded: 0, droppedRequests: 0, droppedResponses: 0 };
  const agent = new http.Agent({ keepAlive: true, maxSockets: 256 });

  const server = http.createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on("data", (d) => chunks.push(d));
    req.on("end", () => {
      if (req.url === "/__faults" && req.method === "PUT") {
        Object.assign(faults, JSON.parse(Buffer.concat(chunks).toString()));
        res.end(JSON.stringify(faults));
        return;
      }
      if (req.url === "/__stats") {
        res.end(JSON.stringify(stats));
        return;
      }
      const applies = !faults.pathContains || req.url?.includes(faults.pathContains);
      if (applies && Math.random() < faults.dropRequestRate) {
        stats.droppedRequests++;
        req.socket.destroy();
        return;
      }
      const outgoing: ProxiedRequest = { method: req.method ?? "GET", url: req.url ?? "/", headers: { ...req.headers }, body: Buffer.concat(chunks) };
      faults.rewrite?.(outgoing);
      if (faults.rewrite && outgoing.headers["content-length"] !== undefined) outgoing.headers["content-length"] = String(outgoing.body.length);
      const upstream = http.request(
        { host: "127.0.0.1", port: targetPort, path: outgoing.url, method: outgoing.method, headers: outgoing.headers, agent },
        (upRes) => {
          const out: Buffer[] = [];
          upRes.on("data", (d) => out.push(d));
          upRes.on("end", () => {
            if (applies && Math.random() < faults.dropResponseRate) {
              stats.droppedResponses++;
              req.socket.destroy();
              return;
            }
            stats.forwarded++;
            res.writeHead(upRes.statusCode ?? 502, upRes.headers);
            res.end(Buffer.concat(out));
          });
          upRes.on("error", () => req.socket.destroy());
        },
      );
      upstream.on("error", () => req.socket.destroy());
      upstream.end(outgoing.body);
    });
  });
  server.keepAliveTimeout = 30_000;
  return new Promise<{ close(): Promise<void>; faults: ProxyFaults; stats: typeof stats; set(f: Partial<ProxyFaults>): void }>((resolve) => {
    server.listen(listenPort, "127.0.0.1", () =>
      resolve({
        faults,
        stats,
        set: (f) => Object.assign(faults, f),
        close: () =>
          new Promise((r) => {
            agent.destroy();
            server.closeAllConnections();
            server.close(() => r());
          }),
      }),
    );
  });
}
