import pg from "pg";

// bigint comes back as a string by default. Money must stay exact, so parse to BigInt.
pg.types.setTypeParser(20, (v) => BigInt(v));

export type Pool = pg.Pool;
export type Client = pg.PoolClient;
export type Q = Pick<pg.Pool, "query">;

// Every statement with parameters runs as a named prepared statement, so each connection parses it once instead of on
// every call, and later runs send a name rather than the SQL text. Named by a process-wide counter: the same text always
// gets the same name, and a name can't be reused for different text.
const statementNames = new Map<string, string>();
function statementName(text: string): string {
  let name = statementNames.get(text);
  if (!name) {
    name = `s${statementNames.size}`;
    statementNames.set(text, name);
  }
  return name;
}

class PreparingClient extends pg.Client {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  query(...args: any[]): any {
    const [text, values, callback] = args;
    if (typeof text === "string" && Array.isArray(values)) return super.query({ name: statementName(text), text, values }, callback);
    return (super.query as (...a: unknown[]) => unknown)(...args);
  }
}

export function createPool(connectionString: string, max = 10): Pool {
  const pool = new pg.Pool({ connectionString, max, Client: PreparingClient });
  // An idle client losing its connection (Postgres restart) must not crash the process.
  pool.on("error", () => {});
  return pool;
}

export async function tx<T>(pool: Pool, fn: (c: Client) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query("begin");
    const out = await fn(client);
    await client.query("commit");
    return out;
  } catch (err) {
    await client.query("rollback").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}
