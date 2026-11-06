import { readdir, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import type { Pool } from "./db.ts";

const ROOT = fileURLToPath(new URL("../migrations/", import.meta.url));

export async function migrate(pool: Pool, service: "processor" | "network" | "sink"): Promise<void> {
  await pool.query("create table if not exists schema_migrations (name text primary key)");
  const applied = new Set((await pool.query<{ name: string }>("select name from schema_migrations")).rows.map((r) => r.name));
  for (const file of (await readdir(ROOT + service)).sort()) {
    if (applied.has(file)) continue;
    const sql = await readFile(`${ROOT}${service}/${file}`, "utf8");
    await pool.query(`begin; ${sql}; insert into schema_migrations (name) values ('${file}'); commit;`);
  }
}
