import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import Fastify, { type FastifyBaseLogger } from 'fastify';
import { destination, multistream, pino, type StreamEntry } from 'pino';
import { config } from './config.js';
import { isTransient, migrate, onPoolError, pool } from './db.js';
import * as hub from './hub.js';
import { registerRoutes } from './routes.js';

const sinks: StreamEntry[] = [{ stream: process.stdout }];
if (config.logFile) sinks.push({ stream: destination({ dest: config.logFile, mkdir: true, sync: false }) });
// Request logs carry method, path, status and time. Never headers (so never an API key) and never bodies.
const logger: FastifyBaseLogger = pino(
  { level: config.logLevel, redact: { paths: ['req.headers.authorization', 'req.headers.cookie'], remove: true } },
  multistream(sinks),
);

const app = Fastify({
  loggerInstance: logger,
  bodyLimit: config.maxBodyBytes,
  exposeHeadRoutes: false, // a HEAD on the SSE route must not open a stream
  genReqId: () => randomUUID(),
});

onPoolError((err) => logger.warn({ err }, 'idle database connection failed'));

// ---------- static UI: three files, read once ----------

const webDir = new URL('../web/', import.meta.url);
const CSP = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; base-uri 'none'; form-action 'none'; frame-ancestors 'none'";
const assets: Array<[route: string, file: string, type: string]> = [
  ['/', 'index.html', 'text/html; charset=utf-8'],
  ['/app.js', 'app.js', 'text/javascript; charset=utf-8'],
  ['/style.css', 'style.css', 'text/css; charset=utf-8'],
];
for (const [route, file, type] of assets) {
  const body = await readFile(new URL(file, webDir));
  app.get(route, (_req, reply) => reply.type(type).header('Content-Security-Policy', CSP).send(body));
}

// ---------- API ----------

app.addHook('onSend', async (_req, reply) => {
  // Nothing here may be stored by a browser or a shared cache: responses depend on the API key.
  reply.header('Cache-Control', 'no-store');
  reply.header('X-Content-Type-Options', 'nosniff');
  reply.header('Referrer-Policy', 'no-referrer');
});

app.setNotFoundHandler((_req, reply) => reply.code(404).send({ error: 'not_found' }));

app.setErrorHandler((err: { statusCode?: number }, req, reply) => {
  const status = err.statusCode ?? 500;
  if (status >= 400 && status < 500) {
    // Malformed JSON, body too large, wrong content type. The message is ours, not the parser's.
    const names: Record<number, string> = { 413: 'payload_too_large', 415: 'unsupported_media_type' };
    return reply.code(status).send({ error: names[status] ?? 'bad_request' });
  }
  hub.counters.errors_5xx++;
  req.log.error({ err }, 'request failed');
  // Details stay in the log. The client gets an id to quote and whether a retry makes sense.
  if (isTransient(err)) return reply.code(503).header('Retry-After', '1').send({ error: 'unavailable', request_id: req.id });
  return reply.code(500).send({ error: 'internal', request_id: req.id });
});

registerRoutes(app);

// ---------- lifecycle ----------

await migrate();
await app.listen({ host: '0.0.0.0', port: config.port });

let stopping = false;
async function shutdown(signal: string): Promise<void> {
  if (stopping) return;
  stopping = true;
  logger.info({ signal }, 'shutting down');
  hub.closeAll('shutdown'); // clients reconnect with their cursor; nothing is lost
  await app.close();
  await pool.end();
  process.exit(0);
}
process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));
