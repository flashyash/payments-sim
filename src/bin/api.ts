import { buildApi } from "../api.ts";
import { loadProcessorConfig } from "../config.ts";
import { buildContext } from "../context.ts";

const ctx = buildContext(loadProcessorConfig());
const app = buildApi(ctx);
await app.listen({ port: Number(process.env.PORT ?? 3100), host: "127.0.0.1" });
