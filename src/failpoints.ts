// Named crash points for the chaos harness. FAILPOINTS="capture.network.after:kill" SIGKILLs this process the first
// time that line runs; FAILPOINTS="capture.*:kill:0.05" kills with 5% probability at any matching point. SIGKILL,
// not exit(): no finally blocks, no flushes, nothing the code could lean on that a real crash wouldn't give it.
// With FAILPOINTS unset this is one string comparison away from a no-op.

interface Rule {
  pattern: RegExp;
  probability: number;
}

function parse(spec: string | undefined): Rule[] {
  if (!spec) return [];
  return spec.split(",").map((part) => {
    const [name, action, p] = part.split(":");
    if (action !== "kill") throw new Error(`unknown failpoint action in ${part}`);
    const pattern = new RegExp("^" + name.replace(/[.]/g, "\\.").replace(/\*/g, ".*") + "$");
    return { pattern, probability: p ? Number(p) : 1 };
  });
}

const rules = parse(process.env.FAILPOINTS);
export const failpointsEnabled = rules.length > 0;

export function failpoint(name: string): void {
  if (rules.length === 0) return;
  for (const r of rules) {
    if (r.pattern.test(name) && Math.random() < r.probability) {
      console.error(`[failpoint] pid ${process.pid} killed at ${name}`);
      process.kill(process.pid, "SIGKILL");
    }
  }
}

// Every point in the code, listed so the harness can prove it crashed at each step of each flow.
export const ALL_FAILPOINTS = [
  "api.admitted",
  "authorize.network.before",
  "authorize.network.after",
  "authorize.open_hold.before",
  "authorize.open_hold.after",
  "authorize.reverse.after",
  "capture.network.before",
  "capture.network.after",
  "capture.post.before",
  "capture.post.after",
  "void.network.after",
  "void.post.after",
  "refund.post.before",
  "refund.post.after",
  "refund.network.after",
  "refund.reverse_post.after",
  "chargeback.post.after",
  "settlement.post.after",
  "inbox.received",
  "outbox.send.before",
  "outbox.send.after",
  "recon.prepared",
] as const;
