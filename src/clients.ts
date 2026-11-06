// HTTP clients for the two systems we can't share a transaction with. The one rule: a timeout or 5xx is UNKNOWN,
// never "failed". Callers either retransmit with the same key/ref or take a deliberate recovery path.

export class Unknown extends Error {
  name = "Unknown";
}

async function call(url: string, init: RequestInit, timeoutMs: number): Promise<Response> {
  try {
    const res = await fetch(url, { ...init, signal: AbortSignal.timeout(timeoutMs) });
    if (res.status >= 500) {
      await res.body?.cancel();
      throw new Unknown(`${res.status} from ${url}`);
    }
    return res;
  } catch (err) {
    if (err instanceof Unknown) throw err;
    throw new Unknown(`${(err as Error).name}: ${(err as Error).message} (${url})`);
  }
}

export interface PostingBody {
  entries: { accountId: string; amount: string }[];
  description: string;
}

export type PostOutcome = { kind: "posted"; txnId: string } | { kind: "rejected"; code: string; message: string };

export interface LedgerTxn {
  id: string;
  entries: { accountId: string; amount: string }[];
}

export class LedgerClient {
  url: string;
  timeoutMs: number;

  constructor(url: string, timeoutMs: number) {
    this.url = url;
    this.timeoutMs = timeoutMs;
  }

  private async posted(res: Response): Promise<PostOutcome> {
    const body = (await res.json().catch(() => ({}))) as { transactionId?: string; error?: string; message?: string };
    if (res.status === 201) return { kind: "posted", txnId: body.transactionId! };
    return { kind: "rejected", code: body.error ?? `http_${res.status}`, message: body.message ?? "" };
  }

  async post(key: string, body: PostingBody): Promise<PostOutcome> {
    const res = await call(
      `${this.url}/transactions`,
      { method: "POST", headers: { "content-type": "application/json", "idempotency-key": key }, body: JSON.stringify(body) },
      this.timeoutMs,
    );
    return this.posted(res);
  }

  async reverse(txnId: string, key: string, description: string): Promise<PostOutcome> {
    const res = await call(
      `${this.url}/transactions/${txnId}/reverse`,
      { method: "POST", headers: { "content-type": "application/json", "idempotency-key": key }, body: JSON.stringify({ description }) },
      this.timeoutMs,
    );
    return this.posted(res);
  }

  // Not idempotent on the ledger's side: a lost response leaves an empty orphan account. Harmless, see DESIGN.md.
  async createAccount(name: string, allowNegative: boolean): Promise<string> {
    const res = await call(
      `${this.url}/accounts`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ name, currency: "USD", allowNegative }),
      },
      this.timeoutMs,
    );
    if (res.status !== 201) throw new Error(`ledger refused account ${name}: ${res.status}`);
    return ((await res.json()) as { id: string }).id;
  }

  async balance(accountId: string): Promise<bigint> {
    const res = await call(`${this.url}/accounts/${accountId}`, { method: "GET" }, this.timeoutMs);
    if (res.status !== 200) throw new Error(`ledger: account ${accountId}: ${res.status}`);
    return BigInt(((await res.json()) as { balance: string }).balance);
  }

  async transaction(txnId: string): Promise<LedgerTxn | null> {
    const res = await call(`${this.url}/transactions/${txnId}`, { method: "GET" }, this.timeoutMs);
    if (res.status === 404) return null;
    return (await res.json()) as LedgerTxn;
  }
}

export interface NetworkReply {
  status: "approved" | "declined";
  reason?: string;
  appliedAt: string;
}

export class NetworkClient {
  url: string;
  timeoutMs: number;

  constructor(url: string, timeoutMs: number) {
    this.url = url;
    this.timeoutMs = timeoutMs;
  }

  private async send(path: string, body: object): Promise<NetworkReply> {
    const res = await call(
      `${this.url}${path}`,
      { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) },
      this.timeoutMs,
    );
    if (res.status !== 200) throw new Error(`network ${path}: ${res.status} ${await res.text()}`);
    return (await res.json()) as NetworkReply;
  }

  auth(b: { ref: string; merchantId: string; cardToken: string; amount: string }) {
    return this.send("/messages/auth", b);
  }
  capture(b: { ref: string; authRef: string; amount: string; final: boolean }) {
    return this.send("/messages/capture", b);
  }
  void(b: { ref: string; authRef: string; merchantId: string; amount: string }) {
    return this.send("/messages/void", b);
  }
  refund(b: { ref: string; authRef: string; amount: string }) {
    return this.send("/messages/refund", b);
  }

  async settlementFile(id: string): Promise<{ id: string; cutoff: string; csv: string } | null> {
    const res = await call(`${this.url}/settlement-files/${id}`, { method: "GET" }, this.timeoutMs);
    if (res.status === 404) return null;
    return (await res.json()) as { id: string; cutoff: string; csv: string };
  }
}
