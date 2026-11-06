import { dbUrl } from "../config.ts";
import { createPool } from "../db.ts";
import { buildSink, type SinkFaults } from "../sink/app.ts";

const pool = createPool(process.env.SINK_DATABASE_URL ?? dbUrl("sink"), 10);
const faults = process.env.SINK_FAULTS ? (JSON.parse(process.env.SINK_FAULTS) as Partial<SinkFaults>) : {};
const { app } = buildSink(pool, faults);
await app.listen({ port: Number(process.env.PORT ?? 3103), host: "127.0.0.1" });
