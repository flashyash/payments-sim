import pg from "pg";

// bigint comes back as a string by default. Money must stay exact, so parse to BigInt.
pg.types.setTypeParser(20, (v) => BigInt(v));

export type Pool = pg.Pool;
export type Client = pg.PoolClient;
export type Q = Pick<pg.Pool, "query">;

export function createPool(connectionString: string, max = 10): Pool {
  const pool = new pg.Pool({ connectionString, max });
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
