// Project A must not be able to read, write, subscribe to, or even confirm the existence of
// anything in project B. "Not found" and "not yours" must be indistinguishable.

import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { expect, test } from '@playwright/test';
import { call, createProject, createRun, ingest, note, points, rotate, sleep, until, type Project, type Res } from '../helpers/api.js';
import { db, dbPoints, dbRun } from '../helpers/db.js';
import { SseClient } from '../helpers/sse.js';

const SECRET_METRIC = 'project_b/secret_metric';
const SECRET_VALUE = 0.730519284617;

let A: Project;
let B: Project;
let runA: { id: string };
let runB: { id: string };
const issuedKeys: string[] = [];

test.beforeAll(async () => {
  A = await createProject('isolation A');
  B = await createProject('isolation B');
  runA = await createRun(A.key, 'run of A');
  runB = await createRun(B.key, 'run of B — confidential name');
  await ingest(B.key, runB.id, points(SECRET_METRIC, 0, 50, () => SECRET_VALUE));
  await ingest(A.key, runA.id, points('a/metric', 0, 5));
  issuedKeys.push(A.key, B.key);
});

/** Everything about a response an attacker could compare, minus what differs on every request. */
const fingerprint = (r: Res) => {
  const headers: Record<string, string> = {};
  r.headers.forEach((v, k) => {
    if (k !== 'date') headers[k] = v;
  });
  return { status: r.status, body: r.text, headers };
};

const VALID_BATCH = { points: [{ key: 'x', step: 0, value: 1 }] };
/** Every route that takes a run id, called with A's key. */
const runRoutes = (id: string): Array<[string, () => Promise<Res>]> => [
  ['GET /runs/:id', () => call(`/runs/${id}`, { key: A.key })],
  ['GET /runs/:id/metrics', () => call(`/runs/${id}/metrics`, { key: A.key })],
  ['GET /runs/:id/events', () => call(`/runs/${id}/events`, { key: A.key, headers: { accept: 'text/event-stream' } })],
  ['POST /runs/:id/metrics', () => call(`/runs/${id}/metrics`, { key: A.key, body: VALID_BATCH })],
];

test('read, write and subscribe: B\'s run answers A exactly like a run that does not exist', async () => {
  const before = await dbRun(runB.id);
  const ghost = randomUUID();
  for (let i = 0; i < runRoutes(ghost).length; i++) {
    const [name, callB] = runRoutes(runB.id)[i]!;
    const [, callGhost] = runRoutes(ghost)[i]!;
    const real = await callB();
    const missing = await callGhost();
    expect(real.status, name).toBe(404);
    expect(fingerprint(real), `${name}: same status, body and headers as a nonexistent id`).toEqual(fingerprint(missing));
    expect(real.text).toBe('{"error":"not_found"}');
  }
  // Sanity: the same calls do work for the owner, so the 404s above are not just a broken route.
  expect((await call(`/runs/${runB.id}`, { key: B.key })).status).toBe(200);
  expect((await call(`/runs/${runB.id}/metrics`, { key: B.key })).json.points).toHaveLength(50);
  // And the write attempt wrote nothing.
  expect(await dbRun(runB.id)).toEqual(before);
  expect((await dbPoints(runB.id)).some((p) => p[1] === 'x')).toBe(false);
});

test('the run list contains only the caller\'s runs', async () => {
  const listA = await call('/runs', { key: A.key });
  const listB = await call('/runs', { key: B.key });
  expect(listA.json.runs.map((r: any) => r.id)).toEqual([runA.id]);
  expect(listB.json.runs.map((r: any) => r.id)).toEqual([runB.id]);
  expect(listA.text).not.toContain(runB.id);
  expect(listA.text).not.toContain('confidential');
  expect((await call('/project', { key: A.key })).json.project.id).toBe(A.id);
});

test('live stream: A\'s stream never carries B\'s points, also while both are written at once', async () => {
  const stream = new SseClient(runA.id, A.key, { keepRaw: true });
  expect(await stream.open()).toBe(200);
  await stream.untilSynced(5);

  // B writes 300 points, A writes 20, interleaved, while A is subscribed.
  await Promise.all([
    (async () => { for (let i = 0; i < 6; i++) await ingest(B.key, runB.id, points(SECRET_METRIC, 1000 + i * 50, 50, () => SECRET_VALUE)); })(),
    (async () => { for (let i = 0; i < 4; i++) await ingest(A.key, runA.id, points('a/metric', 100 + i * 5, 5)); })(),
  ]);
  await stream.untilSynced(25);
  await sleep(2500); // longer than one poll interval: anything that was going to leak has leaked by now
  stream.destroy();

  expect(stream.points.map((p) => p[1])).toEqual(Array(25).fill('a/metric'));
  expect(stream.raw).not.toContain('project_b');
  expect(stream.raw).not.toContain(String(SECRET_VALUE));
  expect(stream.cursor).toBe(25);
  note(`A's stream: ${stream.points.length} points, ${stream.bytes} bytes, none of B's 350 points`);
});

test('ids cannot be guessed or probed: every malformed or near-miss id gives the same 404', async () => {
  const missing = fingerprint(await call(`/runs/${randomUUID()}`, { key: A.key }));
  const flipped = runB.id.slice(0, -1) + (runB.id.endsWith('0') ? '1' : '0');
  const probes = [flipped, runB.id.toUpperCase(), `${runB.id} `, runB.id.replaceAll('-', ''), '1', '0', 'null', '%00', '..%2F..%2Fruns', "' OR '1'='1", `${runB.id}'--`, 'a'.repeat(5000)];
  for (const probe of probes) {
    const res = await call(`/runs/${encodeURIComponent(probe)}`, { key: A.key });
    expect(fingerprint(res), `probe ${probe.slice(0, 40)}`).toEqual(missing);
  }
  // URLs the router itself rejects (bad percent-encoding) must not answer in a different voice.
  for (const rawPath of ['/runs/%zz', '/runs/%E0%A4%A', `/runs/${runB.id}%`]) {
    expect(fingerprint(await call(rawPath, { key: A.key })), `raw path ${rawPath}`).toEqual(missing);
  }

  // Ids are random UUIDv4: no order, no counter, nothing to increment.
  const ids: string[] = [];
  for (let i = 0; i < 100; i++) ids.push((await createRun(A.key, `probe ${i}`)).id);
  expect(new Set(ids).size).toBe(100);
  for (const id of ids) expect(id).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  expect([...ids].sort(), 'creation order is not id order').not.toEqual(ids);
  const firstHex = new Set(ids.map((id) => id[0]));
  note(`100 run ids: all UUIDv4, ${firstHex.size}/16 distinct first hex digits, not sorted by creation time`);
  expect(firstHex.size).toBeGreaterThan(8);
  await db.query('DELETE FROM runs WHERE project_id = $1 AND name LIKE $2', [A.id, 'probe %']);
});

test('missing, malformed, unknown and revoked keys are all the same 401', async () => {
  const temp = await createProject('isolation temp');
  const fresh = await rotate(temp.key);
  issuedKeys.push(temp.key, fresh);
  const expected = fingerprint(await call('/runs'));
  expect(expected.status).toBe(401);
  expect(expected.body).toBe('{"error":"unauthorized"}');
  const badKeys = ['', 'sk_short', 'x'.repeat(46), `sk_${'A'.repeat(43)}`, temp.key /* revoked */, `${A.key}x`, A.key.toLowerCase(), ` ${A.key}`];
  for (const key of badKeys) {
    for (const path of ['/runs', `/runs/${runA.id}`, `/runs/${runA.id}/events`, '/project']) {
      const res = await fetch(`${process.env.API_URL ?? 'http://localhost:8080'}${path}`, { headers: { authorization: `Bearer ${key}` } });
      const headers: Record<string, string> = {};
      res.headers.forEach((v, k) => { if (k !== 'date') headers[k] = v; });
      expect({ status: res.status, body: await res.text(), headers }, `key "${key.slice(0, 12)}…" on ${path}`).toEqual(expected);
    }
  }
  // Other schemes and the key in the wrong place do not work either.
  expect((await fetch(`${process.env.API_URL}/runs`, { headers: { authorization: `Basic ${A.key}` } })).status).toBe(401);
  expect((await call(`/runs?api_key=${A.key}`)).status).toBe(401);
});

test('cache: nothing is cacheable, there are no validators, and one connection serves two keys correctly', async () => {
  const paths = ['/runs', `/runs/${runA.id}`, `/runs/${runA.id}/metrics`, '/project', `/runs/${randomUUID()}`];
  for (const path of paths) {
    const res = await call(path, { key: A.key, headers: { 'if-none-match': '*', 'if-modified-since': new Date(Date.now() + 86_400_000).toUTCString() } });
    expect(res.status, `${path} ignores conditional headers`).not.toBe(304);
    expect(res.headers.get('cache-control'), path).toBe('no-store');
    expect(res.headers.get('etag'), path).toBeNull();
    expect(res.headers.get('last-modified'), path).toBeNull();
  }
  const sse = new SseClient(runA.id, A.key);
  await sse.open();
  expect(sse.headers['cache-control']).toBe('no-store');
  sse.destroy();
  expect((await call('/runs')).headers.get('cache-control'), '401 is not cacheable either').toBe('no-store');

  // Same URL, alternating keys, 40 times: each answer belongs to the key that asked.
  for (let i = 0; i < 40; i++) {
    const mine = i % 2 === 0 ? { key: A.key, run: runA.id, other: runB.id } : { key: B.key, run: runB.id, other: runA.id };
    const res = await call('/runs', { key: mine.key });
    expect(res.text).toContain(mine.run);
    expect(res.text).not.toContain(mine.other);
  }
});

test('error messages carry no data: bad input, oversized body, lock timeout (503)', async () => {
  const leaks = /SELECT|INSERT|UPDATE|postgres|pg_|node_modules|\.ts:|\.js:|\bat \w+ \(|project_b|confidential/i;
  const bad: Array<[string, Promise<Res>]> = [
    ['invalid JSON', call(`/runs/${runA.id}/metrics`, { key: A.key, raw: '{"points":[{' })],
    ['wrong shape', call(`/runs/${runA.id}/metrics`, { key: A.key, body: { points: 'no' } })],
    ['bad point', call(`/runs/${runA.id}/metrics`, { key: A.key, body: { points: [{ key: 'ok', step: -1, value: 1 }] } })],
    ['too large', call(`/runs/${runA.id}/metrics`, { key: A.key, raw: `{"points":[],"pad":"${'x'.repeat(1_100_000)}"}` })],
    ['wrong content type', call(`/runs/${runA.id}/metrics`, { key: A.key, raw: 'points=1', headers: { 'content-type': 'text/plain' } })],
    ['bad cursor', call(`/runs/${runA.id}/metrics?after=abc`, { key: A.key })],
    ['unknown route', call('/runs/x/y/z', { key: A.key })],
  ];
  for (const [name, pending] of bad) {
    const res = await pending;
    expect(res.status, name).toBeGreaterThanOrEqual(400);
    expect(res.status, name).toBeLessThan(500);
    expect(Object.keys(res.json).every((k) => k === 'error' || k === 'detail'), `${name}: only error/detail in ${res.text}`).toBe(true);
    expect(res.text, name).not.toMatch(leaks);
    note(`${name.padEnd(18)} -> ${res.status} ${res.text}`);
  }

  // A server-side failure: hold the run's row lock so the ingest hits lock_timeout (5 s).
  const holder = await db.connect();
  try {
    await holder.query('BEGIN');
    await holder.query('SELECT 1 FROM runs WHERE id = $1 FOR UPDATE', [runA.id]);
    const started = Date.now();
    const res = await call(`/runs/${runA.id}/metrics`, { key: A.key, body: VALID_BATCH });
    expect(res.status).toBe(503);
    expect(res.headers.get('retry-after')).toBe('1');
    expect(Object.keys(res.json).sort()).toEqual(['error', 'request_id']);
    expect(res.text).not.toMatch(leaks);
    note(`lock timeout        -> ${res.status} ${res.text} after ${Date.now() - started} ms (failed, not hung)`);
  } finally {
    await holder.query('ROLLBACK');
    holder.release();
  }
  expect((await dbPoints(runA.id)).some((p) => p[1] === 'x'), 'the timed-out batch wrote nothing').toBe(false);
});

test('the per-project stream limit is per project: A at its limit does not block B', async () => {
  const limit = 100;
  const open: SseClient[] = [];
  try {
    for (let i = 0; i < limit; i++) {
      const s = new SseClient(runA.id, A.key);
      expect(await s.open()).toBe(200);
      open.push(s);
    }
    const over = new SseClient(runA.id, A.key);
    expect(await over.open(), 'stream 101 of project A').toBe(429);
    const other = new SseClient(runB.id, B.key);
    expect(await other.open(), 'project B is unaffected').toBe(200);
    other.destroy();
  } finally {
    for (const s of open) s.destroy();
  }
  // The slots come back when the connections close.
  await until('slots to be released', async () => {
    const s = new SseClient(runA.id, A.key);
    const status = await s.open();
    s.destroy();
    return status === 200;
  });
});

test('timing: "not yours" and "does not exist" take the same time (weak evidence, reported)', async () => {
  const time = async (id: string) => {
    const t = performance.now();
    await call(`/runs/${id}`, { key: A.key });
    return performance.now() - t;
  };
  const real: number[] = [];
  const ghost: number[] = [];
  for (let i = 0; i < 20; i++) await time(randomUUID()); // warm up
  for (let i = 0; i < 400; i++) {
    // interleaved, order alternating, so drift affects both equally
    if (i % 2) { real.push(await time(runB.id)); ghost.push(await time(randomUUID())); }
    else { ghost.push(await time(randomUUID())); real.push(await time(runB.id)); }
  }
  const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[xs.length >> 1]!;
  const p90 = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length * 0.9)]!;
  const delta = median(real) - median(ghost);
  note(`other project's run: median ${median(real).toFixed(3)} ms, p90 ${p90(real).toFixed(3)} ms`);
  note(`nonexistent run:     median ${median(ghost).toFixed(3)} ms, p90 ${p90(ghost).toFixed(3)} ms`);
  note(`difference of medians: ${(delta * 1000).toFixed(0)} µs over 400 pairs — below what a remote attacker can use; ids are 122-bit random anyway`);
  expect(Math.abs(delta)).toBeLessThan(1);
});

test('server logs contain no API keys and no metric values', async () => {
  const path = process.env.API_LOG;
  test.skip(!path, 'API_LOG is not set (the log volume is only mounted in docker compose)');
  await sleep(1500); // the log file is written asynchronously
  const log = await readFile(path!, 'utf8');
  const lines = log.split('\n').length;
  for (const key of issuedKeys) expect(log.includes(key), 'an issued API key appears in the log').toBe(false);
  expect(log.match(/sk_[A-Za-z0-9_-]{43}/g) ?? [], 'anything shaped like an API key').toEqual([]);
  expect(log).not.toContain('Bearer ');
  expect(log).not.toContain(String(SECRET_VALUE));
  expect(log).not.toContain(SECRET_METRIC);
  // What the log does contain: paths (with run ids) and project ids. Reported, not hidden.
  const withRunId = (log.match(new RegExp(runB.id, 'g')) ?? []).length;
  note(`${lines} log lines scanned: 0 API keys, 0 "Bearer", 0 metric names or values`);
  note(`run ids do appear in request paths (${withRunId} lines mention B's run id) — acceptable only because an id alone opens nothing`);
});
