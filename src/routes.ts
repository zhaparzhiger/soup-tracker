import { createHash, timingSafeEqual } from 'node:crypto';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import { config } from './config.js';
import { pool } from './db.js';
import * as hub from './hub.js';
import * as repo from './repo.js';
import { RunStream, SSE_HEADERS } from './stream.js';

declare module 'fastify' {
  interface FastifyRequest {
    auth: repo.Auth;
  }
}

const KEY_NAME_RE = /^[A-Za-z0-9_.\/:-]+$/;

/** One body for "no such run", "run of another project" and "malformed id". */
const notFound = (reply: FastifyReply) => reply.code(404).send({ error: 'not_found' });
const badRequest = (reply: FastifyReply, detail: string) => reply.code(400).send({ error: 'bad_request', detail });

function parseName(body: unknown, max: number): string | null {
  const name = (body as { name?: unknown } | null)?.name;
  if (typeof name !== 'string') return null;
  const trimmed = name.trim();
  return trimmed.length >= 1 && trimmed.length <= max ? trimmed : null;
}

/**
 * Strict by hand on purpose: no type coercion. `null`, `"0.5"` or `true` as a value is an
 * error, never a silently stored 0. Returns the points or a message naming the bad index.
 */
function parsePoints(body: unknown): repo.Point[] | string {
  const raw = (body as { points?: unknown } | null)?.points;
  if (!Array.isArray(raw)) return 'body must be {"points": [...]}';
  if (raw.length === 0) return 'points must not be empty';
  if (raw.length > config.maxBatchPoints) return `at most ${config.maxBatchPoints} points per batch`;
  const out: repo.Point[] = [];
  for (let i = 0; i < raw.length; i++) {
    const p = raw[i] as { key?: unknown; step?: unknown; value?: unknown } | null;
    if (typeof p !== 'object' || p === null) return `points[${i}] must be an object`;
    const { key, step, value } = p;
    if (typeof key !== 'string' || key.length < 1 || key.length > config.maxKeyLength || !KEY_NAME_RE.test(key)) {
      return `points[${i}].key must be 1-${config.maxKeyLength} chars of A-Z a-z 0-9 _ . / : -`;
    }
    if (typeof step !== 'number' || !Number.isSafeInteger(step) || step < 0) return `points[${i}].step must be an integer 0..2^53-1`;
    if (typeof value !== 'number' || !Number.isFinite(value)) return `points[${i}].value must be a finite number`;
    out.push({ key, step, value });
  }
  return out;
}

/** A cursor is a non-negative integer. Absent means 0 (from the beginning). */
function parseCursor(raw: unknown): number | null {
  if (raw === undefined || raw === '') return 0;
  if (typeof raw !== 'string' || !/^\d{1,16}$/.test(raw)) return null;
  const n = Number(raw);
  return Number.isSafeInteger(n) ? n : null;
}

function isAdmin(header: string | undefined): boolean {
  if (!config.adminToken || !header?.startsWith('Bearer ')) return false;
  const digest = (s: string) => createHash('sha256').update(s).digest();
  return timingSafeEqual(digest(header.slice(7)), digest(config.adminToken));
}

export function registerRoutes(app: FastifyInstance): void {
  // ---------- public ----------

  app.get('/healthz', async () => {
    await pool.query('SELECT 1');
    return { ok: true };
  });

  // Anyone may create a project; a project is the unit of isolation and starts empty.
  app.post('/projects', async (req, reply) => {
    const name = parseName(req.body, 100);
    if (!name) return badRequest(reply, 'name must be a string of 1-100 characters');
    const project = await repo.createProject(name);
    req.log.info({ project: project.id }, 'project created');
    return reply.code(201).send({ project: { id: project.id, name: project.name }, api_key: project.api_key });
  });

  // Operational counters only, no tenant data. 404 without the admin token: do not reveal it exists.
  app.get('/internal/stats', async (req, reply) => {
    if (!isAdmin(req.headers.authorization)) return notFound(reply);
    const mem = process.memoryUsage();
    return {
      uptime_s: Math.round(process.uptime()),
      cpu_us: process.cpuUsage().user + process.cpuUsage().system,
      memory: { rss: mem.rss, heap_used: mem.heapUsed, external: mem.external },
      db_pool: { total: pool.totalCount, idle: pool.idleCount, waiting: pool.waitingCount },
      streams: hub.stats(),
      counters: hub.counters,
    };
  });

  // ---------- everything below needs a project API key ----------

  app.register(async (api) => {
    api.addHook('onRequest', async (req: FastifyRequest, reply: FastifyReply) => {
      const auth = await repo.authenticate(req.headers.authorization);
      if (!auth) {
        hub.counters.auth_failures++;
        return reply.code(401).header('WWW-Authenticate', 'Bearer').send({ error: 'unauthorized' });
      }
      req.auth = auth;
      req.log = req.log.child({ project: auth.projectId });
    });

    api.get('/project', async (req) => ({ project: await repo.getProject(req.auth.projectId) }));

    api.post('/keys/rotate', async (req, reply) => {
      const key = await repo.rotateKey(req.auth);
      if (!key) return reply.code(401).header('WWW-Authenticate', 'Bearer').send({ error: 'unauthorized' });
      hub.closeStreamsOfKey(req.auth.keyId);
      req.log.info({ key: req.auth.keyId }, 'api key rotated');
      return reply.code(201).send({ api_key: key });
    });

    api.get('/runs', async (req) => ({ runs: await repo.listRuns(req.auth.projectId) }));

    api.post('/runs', async (req, reply) => {
      const name = parseName(req.body, 200);
      if (!name) return badRequest(reply, 'name must be a string of 1-200 characters');
      return reply.code(201).send({ run: await repo.createRun(req.auth.projectId, name) });
    });

    api.get<{ Params: { id: string } }>('/runs/:id', async (req, reply) => {
      const run = await repo.getRun(req.auth.projectId, req.params.id);
      return run ? { run } : notFound(reply);
    });

    api.post<{ Params: { id: string } }>('/runs/:id/metrics', async (req, reply) => {
      const points = parsePoints(req.body);
      if (typeof points === 'string') return badRequest(reply, points);
      const result = await repo.ingest(req.auth.projectId, req.params.id, points);
      if (!result) return notFound(reply);
      hub.counters.ingest_batches++;
      hub.counters.points_accepted += result.accepted;
      hub.counters.points_duplicate += result.duplicates;
      hub.counters.points_conflict += result.conflicts;
      if (result.accepted > 0) hub.wakeRun(req.params.id); // after COMMIT: a hint, never the data
      return result;
    });

    // The same log the stream serves, as plain JSON pages: `after` is the seq cursor.
    api.get<{ Params: { id: string }; Querystring: { after?: string; limit?: string } }>('/runs/:id/metrics', async (req, reply) => {
      const after = parseCursor(req.query.after);
      const limit = req.query.limit === undefined ? 1000 : parseCursor(req.query.limit);
      if (after === null || limit === null || limit < 1 || limit > 5000) return badRequest(reply, 'after must be a seq, limit 1..5000');
      const run = await repo.getRun(req.auth.projectId, req.params.id);
      if (!run) return notFound(reply);
      const points = await repo.readPoints(req.auth.projectId, run.id, after, limit);
      const last = points.length > 0 ? points[points.length - 1]![0] : after;
      return { points, next_after: last, last_seq: run.last_seq };
    });

    api.get<{ Params: { id: string }; Querystring: { after?: string } }>('/runs/:id/events', async (req, reply) => {
      const after = parseCursor(req.headers['last-event-id'] ?? req.query.after);
      if (after === null) return badRequest(reply, 'Last-Event-ID must be a seq');
      const run = await repo.getRun(req.auth.projectId, req.params.id);
      if (!run) return notFound(reply);

      const stream = new RunStream(req.auth, run.id, after, reply.raw, req.log);
      if (!hub.register(stream)) return reply.code(429).header('Retry-After', '5').send({ error: 'too_many_streams' });

      reply.hijack(); // from here the socket is ours
      reply.raw.writeHead(200, SSE_HEADERS);
      req.raw.socket.setNoDelay(true);
      req.log.info({ run: run.id, after }, 'stream opened');
      stream.start();
    });
  });
}
