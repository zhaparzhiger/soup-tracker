// End to end: a real browser on the run page, a sender on a bad network, and the database.
// At the end four views of the run must be identical:
//   what was sent  ==  what is stored  ==  what the API returns  ==  what the page shows.
// And while it runs, the page must never be ahead of the database.

import { expect, test } from '@playwright/test';
import { apiPoints, call, createProject, createRun, ingest, INGEST_PROXY_URL, note, PROXY_URL, seeded, sleep, until, type Point } from '../helpers/api.js';
import { db, dbPoints, dbRun } from '../helpers/db.js';
import { addToxic, removeToxic, resetProxies, setEnabled } from '../helpers/toxi.js';

const SEED = Number(process.env.SEED ?? 20261110);
const KEYS = ['train/loss', 'train/accuracy', 'lr'];
const STEPS = 400;

test.afterEach(resetProxies);

test('page == API == database == what was sent, through disorder, duplicates, late points and dropped connections', async ({ page }) => {
  const rng = seeded(SEED);
  const project = await createProject('e2e consistency');
  const run = await createRun(project.key, 'chaos run');

  // ---- ground truth: 1200 points, values with awkward floats on purpose ----
  const truth: Point[] = [];
  for (const key of KEYS) for (let step = 0; step < STEPS; step++) truth.push({ key, step, value: (rng.next() - 0.5) * 10 ** rng.int(-8, 8) });

  // ---- plan the sending: disorder, uneven batches, 10% held back as "late" ----
  const shuffled = rng.shuffle(truth);
  const late = shuffled.splice(0, Math.floor(truth.length / 10));
  const batches: Point[][] = [];
  for (let i = 0; i < shuffled.length; ) {
    const size = rng.int(5, 40);
    batches.push(shuffled.slice(i, i + size));
    i += size;
  }

  // ---- open the page the way a user would, through the proxy ----
  await page.goto(PROXY_URL + '/');
  await page.getByTestId('api-key-input').fill(project.key);
  await page.getByTestId('connect').click();
  await page.locator(`[data-run-id="${run.id}"] a`).click();
  await expect(page.getByTestId('run-title')).toHaveText('chaos run');
  await expect(page.getByTestId('status')).toHaveAttribute('data-state', 'live');

  const pageState = () =>
    page.evaluate(() => {
      const text = (id: string) => document.querySelector(`[data-testid="${id}"]`)?.textContent ?? '';
      return { seq: Number(text('synced-seq')), count: Number(text('point-count')), state: document.querySelector<HTMLElement>('[data-testid="status"]')?.dataset.state ?? '', diag: text('diagnostics') };
    });
  const diagNumber = (diag: string, name: string) => Number(new RegExp(`${name} (\\d+)`).exec(diag)?.[1] ?? NaN);

  // ---- the "always" check: sample the page, then the database, the whole time ----
  const violations: string[] = [];
  let samples = 0;
  let sampling = true;
  let maxLag = 0;
  const sampler = (async () => {
    let previous = 0;
    while (sampling) {
      const p = await pageState().catch(() => null);
      if (p) {
        const stored = (await dbRun(run.id)).last_seq; // read after the page: must be >= what the page showed
        samples++;
        if (p.seq > stored) violations.push(`page at seq ${p.seq} but only ${stored} stored`);
        if (p.count !== p.seq) violations.push(`page shows ${p.count} points at seq ${p.seq}`);
        if (p.seq < previous) violations.push(`page went backwards ${previous} -> ${p.seq}`);
        previous = p.seq;
        maxLag = Math.max(maxLag, stored - p.seq);
      }
      await sleep(100);
    }
  })();

  // ---- the sender: retries until acknowledged, like a real client on a bad network ----
  const sent = { requests: 0, failures: 0, retriesAlreadyStored: 0, ghostRequests: 0, duplicateSends: 0, accepted: 0, duplicates: 0, conflicts: 0 };
  async function deliver(batch: Point[]): Promise<void> {
    for (let attempt = 0; ; attempt++) {
      sent.requests++;
      try {
        const res = await ingest(project.key, run.id, batch, INGEST_PROXY_URL, AbortSignal.timeout(4000));
        if (res.status === 200) {
          sent.accepted += res.json.accepted;
          sent.duplicates += res.json.duplicates;
          sent.conflicts += res.json.conflicts;
          // A retry that finds its points already there: an earlier attempt was stored, its answer was lost.
          if (attempt > 0 && res.json.duplicates > 0) sent.retriesAlreadyStored++;
          return;
        }
        if (res.status < 500) throw new Error(`batch rejected: HTTP ${res.status} ${res.text}`);
      } catch (err) {
        if (String((err as Error).message).startsWith('batch rejected')) throw err;
      }
      sent.failures++;
      await sleep(Math.min(1000, 50 * 2 ** attempt));
    }
  }
  /** Sends a slice of the plan, 4 requests in flight, with the faults a real network produces. */
  async function sendRange(from: number, to: number): Promise<void> {
    let next = from;
    await Promise.all(
      Array.from({ length: 4 }, async () => {
        while (next < to) {
          const batch = batches[next++]!;
          if (rng.chance(0.15)) {
            // The request reaches the server but the client gives up on the answer.
            sent.ghostRequests++;
            void ingest(project.key, run.id, batch, INGEST_PROXY_URL, AbortSignal.timeout(rng.int(1, 60))).catch(() => {});
          }
          await deliver(batch);
          if (rng.chance(0.25)) {
            sent.duplicateSends++;
            await deliver(batch); // acknowledged, sent again anyway
          }
          await sleep(rng.int(0, 40));
        }
      }),
    );
  }
  const cut = (n: number) => Math.floor((batches.length * n) / 100);

  // Requests of the sender overtake each other from here on.
  await addToxic('ingest', 'jitter', 'latency', { latency: 25, jitter: 25 }, 'upstream');

  // Phase 1: normal.
  await sendRange(0, cut(25));

  // Phase 2: every connection of the sender is reset 30 ms after it opens. With the jitter above
  // some requests die before they reach the server and some after it has stored the batch.
  // The fault stays until the sender has really failed 12 times.
  await addToxic('ingest', 'reset', 'reset_peer', { timeout: 30 }, 'downstream');
  const failuresBefore = sent.failures;
  const healNetwork = (async () => {
    await until('sender to hit 12 connection resets', () => sent.failures - failuresBefore >= 12, 30_000);
    await removeToxic('ingest', 'reset');
  })();
  await sendRange(cut(25), cut(40));
  await healNetwork;

  // Phase 3: the browser loses its connection (TCP reset, then refused). Writes continue meanwhile.
  await setEnabled('browser', false);
  await until('page to notice the outage', async () => (await pageState()).state === 'reconnecting', 15_000);
  const duringOutage = await pageState();
  await sendRange(cut(40), cut(65));
  const missed = (await dbRun(run.id)).last_seq - duringOutage.seq;
  await setEnabled('browser', true);
  await until('page to catch up after the outage', async () => (await pageState()).seq >= duringOutage.seq + missed, 30_000);

  // Phase 4: the browser's connection goes silent (no data, no close). Only the watchdog can notice.
  const stallsBefore = diagNumber((await pageState()).diag, 'stalls');
  await addToxic('browser', 'blackhole', 'timeout', { timeout: 0 }, 'downstream');
  await sendRange(cut(65), cut(85));
  await until('watchdog to cut the silent connection', async () => diagNumber((await pageState()).diag, 'stalls') > stallsBefore, 30_000, 250);
  await removeToxic('browser', 'blackhole');

  // Phase 5: the rest, then the late points, then old batches again, re-batched differently.
  await sendRange(cut(85), batches.length);
  for (let i = 0; i < late.length; i += 30) await deliver(late.slice(i, i + 30));
  const rebatched = rng.shuffle(truth).slice(0, 200);
  for (let i = 0; i < rebatched.length; i += 50) await deliver(rebatched.slice(i, i + 50));

  // A conflicting write: same (key, step), different value. First write must win.
  const victim = truth[rng.int(0, truth.length - 1)]!;
  const conflict = await ingest(project.key, run.id, [{ ...victim, value: victim.value + 1 }]);
  expect(conflict.json).toMatchObject({ accepted: 0, duplicates: 0, conflicts: 1 });

  // ---- converge ----
  await removeToxic('ingest', 'jitter');
  const stored = await dbRun(run.id);
  await until('page to reach the stored head', async () => {
    const p = await pageState();
    return p.seq === stored.last_seq && p.state === 'live';
  }, 30_000);
  sampling = false;
  await sampler;

  // ---- compare the four views ----
  const byKeyStep = (a: [string, number, number], b: [string, number, number]) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : a[1] - b[1]);
  const sentView = truth.map((p): [string, number, number] => [p.key, p.step, p.value]).sort(byKeyStep);

  const rows = await dbPoints(run.id);
  const dbView = rows.map((r): [string, number, number] => [r[1], r[2], r[3]]).sort(byKeyStep);

  const viaApi = await apiPoints(project.key, run.id, 97, PROXY_URL);
  const apiView = viaApi.map((r): [string, number, number] => [r[1], r[2], r[3]]).sort(byKeyStep);

  await page.getByTestId('toggle-data').click();
  await expect(page.getByTestId('data-note')).toHaveText(`All ${truth.length} points.`);
  const domRows = await page.locator('[data-testid="data-rows"] tr').evaluateAll((trs) => trs.map((tr) => [...tr.children].map((td) => td.textContent ?? '')));
  const pageView = domRows.map((r): [string, number, number] => [r[0]!, Number(r[1]), Number(r[2])]).sort(byKeyStep);

  const summary = await page.locator('[data-testid="summary"] tr').evaluateAll((trs) => trs.map((tr) => [...tr.children].map((td) => td.textContent ?? '')));
  const final = await pageState();
  const reconnects = diagNumber(final.diag, 'reconnects');
  const stalls = diagNumber(final.diag, 'stalls');
  const gaps = diagNumber(final.diag, 'gaps');

  note(`seed ${SEED} · ${truth.length} unique points in ${batches.length} batches + ${late.length} late`);
  note(`sender: ${sent.requests} requests, ${sent.failures} failed (connection reset or timeout) and were retried, ${sent.retriesAlreadyStored} of those retries found the batch already stored`);
  note(`sender: ${sent.ghostRequests} requests abandoned mid-flight, ${sent.duplicateSends} batches sent twice on purpose`);
  note(`acks the sender saw: accepted ${sent.accepted} (the other ${truth.length - sent.accepted} points were stored by requests whose answer never arrived), duplicates ${sent.duplicates}, conflicts ${sent.conflicts + 1} (1 forced)`);
  note(`browser outage: page stayed at seq ${duringOutage.seq} while ${missed} points were stored, then caught up`);
  note(`page: reconnects ${reconnects}, stalls ${stalls}, gaps ${gaps} · ${final.diag}`);
  note(`"always" check: ${samples} samples, max lag ${maxLag} points, violations ${violations.length}`);
  note(`views: sent ${sentView.length} · database ${dbView.length} · API ${apiView.length} · page ${pageView.length} rows`);

  expect(violations, 'the page was never ahead of the database and never went backwards').toEqual([]);
  expect(samples).toBeGreaterThan(50);

  expect(dbView, 'database == what was sent (first write wins)').toEqual(sentView);
  expect(apiView, 'API == database').toEqual(dbView);
  expect(pageView, 'page == database').toEqual(dbView);

  // The log itself: seq is 1..N without holes, in the API in the same order as in the database.
  expect(rows.map((r) => r[0])).toEqual(Array.from({ length: truth.length }, (_, i) => i + 1));
  expect(viaApi).toEqual(rows);
  expect(stored).toEqual({ last_seq: truth.length, conflicts: sent.conflicts + 1, stored: truth.length });
  expect(final).toMatchObject({ seq: truth.length, count: truth.length, state: 'live' });
  expect(summary.map((r) => [r[0], Number(r[1]), Number(r[2])])).toEqual([...KEYS].sort().map((k) => [k, STEPS, STEPS - 1]));

  // The faults really happened, and the client handled them without a gap.
  expect(sent.failures, 'sender saw real failures').toBeGreaterThan(0);
  expect(sent.duplicates, 'server saw real duplicates').toBeGreaterThan(0);
  expect(missed, 'points were stored while the browser was offline').toBeGreaterThan(0);
  expect(reconnects).toBeGreaterThanOrEqual(2);
  expect(stalls).toBeGreaterThanOrEqual(1);
  expect(gaps).toBe(0);

  // The run list agrees too.
  await page.goBack();
  await expect(page.locator(`[data-run-id="${run.id}"] td`).nth(1)).toHaveText(String(truth.length));
  const listed = await call('/runs', { key: project.key });
  expect(listed.json.runs.find((r: any) => r.id === run.id)).toMatchObject({ last_seq: truth.length, conflicts: sent.conflicts + 1 });
  await db.query('SELECT 1');
});
