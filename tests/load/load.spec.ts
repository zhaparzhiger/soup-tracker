// Load test behind the SHIP / DON'T SHIP verdict. Not part of the default suite:
//   docker compose --profile test run --rm tests npm run load
//
// Open loop: every run sends on a fixed clock whether or not the previous request has returned,
// so a slow server shows up as latency instead of quietly lowering the offered load.
// Each point's value is its send time, so a viewer can measure "sent -> on my screen" lag.

import { expect, test } from '@playwright/test';
import { createProject, createRun, ingest, note, sleep, stats, until, type Point } from '../helpers/api.js';
import { db } from '../helpers/db.js';
import { SseClient } from '../helpers/sse.js';

const env = (name: string, fallback: number) => Number(process.env[name] ?? fallback);
const RUNS = env('LOAD_RUNS', 50);
const RATE = env('LOAD_RATE', 10); // events per second per run
const SECONDS = env('LOAD_SECONDS', 60);
const FLUSH_MS = env('LOAD_FLUSH_MS', 1000); // one batch per run per flush
const VIEWERS = env('LOAD_VIEWERS', 2); // live pages per run
const PROJECTS = env('LOAD_PROJECTS', 5);

const pct = (sorted: number[], p: number) => (sorted.length ? sorted[Math.min(sorted.length - 1, Math.floor((sorted.length * p) / 100))]! : NaN);
const summary = (xs: number[]) => {
  const s = [...xs].sort((a, b) => a - b);
  return `p50 ${pct(s, 50).toFixed(0)} · p95 ${pct(s, 95).toFixed(0)} · p99 ${pct(s, 99).toFixed(0)} · max ${(s[s.length - 1] ?? NaN).toFixed(0)} ms`;
};

test(`${RUNS} runs x ${RATE} events/s for ${SECONDS} s, ${VIEWERS} live viewer(s) per run`, async () => {
  const perBatch = Math.max(1, Math.round((RATE * FLUSH_MS) / 1000));
  const ticks = Math.floor((SECONDS * 1000) / FLUSH_MS);
  const expectedPerRun = ticks * perBatch;

  // ---- setup ----
  const projects = await Promise.all(Array.from({ length: PROJECTS }, (_, i) => createProject(`load ${i}`)));
  const runs = await Promise.all(
    Array.from({ length: RUNS }, async (_, i) => {
      const project = projects[i % PROJECTS]!;
      return { key: project.key, id: (await createRun(project.key, `load run ${i}`)).id };
    }),
  );
  const lag: number[] = [];
  const viewers: SseClient[] = [];
  for (const run of runs) {
    for (let v = 0; v < VIEWERS; v++) {
      const viewer = new SseClient(run.id, run.key, { onPoint: (p) => lag.push(Date.now() - p[3]) });
      expect(await viewer.open()).toBe(200);
      viewers.push(viewer);
    }
  }
  const before = await stats();

  // ---- load ----
  const latency: number[] = [];
  let failures = 0;
  let inFlight = 0;
  let maxInFlight = 0;
  const pending: Promise<void>[] = [];
  const peak = { pool_waiting: 0, buffered: 0, rss: 0, streams_waiting: 0 };
  let sampling = true;
  const sampler = (async () => {
    while (sampling) {
      const s = await stats().catch(() => null);
      if (s) {
        peak.pool_waiting = Math.max(peak.pool_waiting, s.db_pool.waiting);
        peak.buffered = Math.max(peak.buffered, s.streams.buffered_bytes);
        peak.rss = Math.max(peak.rss, s.memory.rss);
        peak.streams_waiting = Math.max(peak.streams_waiting, s.streams.waiting_for_drain);
      }
      await sleep(500);
    }
  })();

  async function send(run: { key: string; id: string }, tick: number): Promise<void> {
    const points: Point[] = Array.from({ length: perBatch }, (_, i) => ({ key: 'm', step: tick * perBatch + i, value: Date.now() }));
    inFlight++;
    maxInFlight = Math.max(maxInFlight, inFlight);
    const started = performance.now();
    for (;;) {
      const res = await ingest(run.key, run.id, points).catch(() => null);
      if (res?.status === 200) break;
      failures++;
      await sleep(100);
    }
    latency.push(performance.now() - started);
    inFlight--;
  }

  const t0 = Date.now();
  for (let tick = 0; tick < ticks; tick++) {
    for (let r = 0; r < runs.length; r++) {
      // Spread the runs across the flush interval instead of firing them all in the same millisecond.
      const due = t0 + tick * FLUSH_MS + (r * FLUSH_MS) / runs.length;
      const wait = due - Date.now();
      if (wait > 0) await sleep(wait);
      pending.push(send(runs[r]!, tick));
    }
  }
  await Promise.all(pending);
  const elapsed = (Date.now() - t0) / 1000;

  // ---- settle and verify ----
  await until('every viewer to reach the head', () => viewers.every((v) => v.cursor >= expectedPerRun), 120_000, 100);
  sampling = false;
  await sampler;
  const after = await stats();
  for (const v of viewers) v.destroy();

  const stored = await db.query('SELECT id, last_seq, (SELECT count(*) FROM points p WHERE p.run_id = r.id)::int AS n FROM runs r WHERE id = ANY($1::uuid[])', [runs.map((r) => r.id)]);
  const wrong = stored.rows.filter((r) => r.n !== expectedPerRun || r.last_seq !== expectedPerRun);
  const viewerProblems = viewers.filter((v) => v.gaps > 0 || v.dups > 0 || v.points.length !== expectedPerRun).length;

  const total = RUNS * expectedPerRun;
  const cpu = (after.cpu_us - before.cpu_us) / 1e6 / elapsed;
  note(`offered: ${RUNS} runs x ${RATE} ev/s = ${RUNS * RATE} ev/s, as ${perBatch}-point batches every ${FLUSH_MS} ms = ${(RUNS * 1000) / FLUSH_MS} req/s, ${viewers.length} open streams`);
  note(`done:    ${total} points in ${elapsed.toFixed(1)} s = ${(total / elapsed).toFixed(0)} ev/s, ${latency.length} requests, ${failures} failed requests, max ${maxInFlight} in flight`);
  note(`ingest latency (request -> ack):        ${summary(latency)}`);
  note(`delivery lag  (sent -> seen by viewer): ${summary(lag)}  over ${lag.length} deliveries`);
  note(`API process: CPU ${(cpu * 100).toFixed(0)}% of one core · RSS peak ${(peak.rss / 1e6).toFixed(0)} MB · DB pool waiting peak ${peak.pool_waiting} · stream buffers peak ${(peak.buffered / 1024).toFixed(0)} KiB · streams blocked peak ${peak.streams_waiting}`);
  note(`server errors (5xx) during the run: ${after.counters.errors_5xx - before.counters.errors_5xx} · slow-consumer closes: ${after.counters.streams_closed_slow - before.counters.streams_closed_slow}`);
  note(`verify:  runs with a wrong stored count: ${wrong.length}/${RUNS} · viewers with a gap, duplicate or missing point: ${viewerProblems}/${viewers.length}`);

  expect(wrong, 'every run stored exactly what was sent').toEqual([]);
  expect(viewerProblems, 'every viewer saw every point exactly once, in order').toBe(0);
  expect(failures, 'no request had to be retried').toBe(0);
});
