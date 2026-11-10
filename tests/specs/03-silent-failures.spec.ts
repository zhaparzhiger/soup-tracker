// Failures that would not raise an error by themselves. Each test provokes one for real and
// then checks the stored state, not just the HTTP status.

import { expect, test } from '@playwright/test';
import { call, createProject, createRun, ingest, note, points, rotate, seeded, sleep, stats, until, type Point } from '../helpers/api.js';
import { db, dbPoints, dbRun } from '../helpers/db.js';
import { SseClient } from '../helpers/sse.js';

const seqs = (n: number) => Array.from({ length: n }, (_, i) => i + 1);

test.describe('events lost on reconnect', () => {
  test('a reader that is cut off 40 times at random moments receives every seq exactly once', async () => {
    const rng = seeded(7);
    const p = await createProject('sf reconnect');
    const run = await createRun(p.key, 'reconnect');
    const TOTAL = 6000;

    let writing = true;
    const writer = (async () => {
      for (let step = 0; step < TOTAL; step += 20) {
        expect((await ingest(p.key, run.id, points('m', step, 20))).status).toBe(200);
        await sleep(10);
      }
      writing = false;
    })();

    const reader = new SseClient(run.id, p.key);
    let cuts = 0;
    while (writing || reader.cursor < TOTAL) {
      await reader.open();
      if (cuts < 40 && writing) {
        await sleep(rng.int(5, 80));
        reader.destroy(); // abrupt, mid-message more often than not
        cuts++;
      } else {
        await reader.untilSeq(TOTAL, 30_000);
        reader.destroy();
      }
    }
    await writer;

    note(`${cuts} forced disconnects, ${reader.connections} connections, ${reader.points.length} points received, dups ${reader.dups}, gaps ${reader.gaps}`);
    expect(cuts).toBe(40);
    expect(reader.gaps, 'a seq was skipped').toBe(0);
    expect(reader.dups, 'a seq at or below the cursor was replayed').toBe(0);
    expect(reader.points.map((x) => x[0])).toEqual(seqs(TOTAL));
    expect(reader.points).toEqual(await dbPoints(run.id));
  });

  test('16 concurrent writers to one run: seq stays gapless and live readers never skip a late commit', async () => {
    const p = await createProject('sf concurrent');
    const run = await createRun(p.key, 'concurrent');
    const WRITERS = 16, BATCHES = 25, SIZE = 10;
    const TOTAL = WRITERS * BATCHES * SIZE;

    const readers = [new SseClient(run.id, p.key), new SseClient(run.id, p.key), new SseClient(run.id, p.key)];
    for (const r of readers) await r.open();

    await Promise.all(
      Array.from({ length: WRITERS }, async (_, w) => {
        for (let b = 0; b < BATCHES; b++) expect((await ingest(p.key, run.id, points(`writer_${w}`, b * SIZE, SIZE))).status).toBe(200);
      }),
    );
    for (const r of readers) await r.untilSynced(TOTAL, 30_000);
    for (const r of readers) r.destroy();

    const rows = await dbPoints(run.id);
    expect(rows.map((x) => x[0])).toEqual(seqs(TOTAL));
    for (const r of readers) {
      expect(r.gaps).toBe(0);
      expect(r.dups).toBe(0);
      expect(r.points).toEqual(rows);
    }
    note(`${WRITERS} writers x ${BATCHES} batches: ${TOTAL} points, seq 1..${TOTAL} contiguous, 3 live readers each got all of them in order`);
  });

  test('points committed without a wake-up (another instance, lost hint) still arrive via the poll', async () => {
    const p = await createProject('sf poll');
    const run = await createRun(p.key, 'poll');
    await ingest(p.key, run.id, points('m', 0, 3));
    const reader = new SseClient(run.id, p.key);
    await reader.open();
    await reader.untilSynced(3);

    // Write straight to the database, the way a second API instance would: this process gets no hint.
    const client = await db.connect();
    try {
      await client.query('BEGIN');
      await client.query('SELECT 1 FROM runs WHERE id = $1 FOR NO KEY UPDATE', [run.id]);
      await client.query("INSERT INTO points (run_id, seq, key, step, value) VALUES ($1, 4, 'm', 3, 0.5), ($1, 5, 'm', 4, 0.75)", [run.id]);
      await client.query('UPDATE runs SET last_seq = 5 WHERE id = $1', [run.id]);
      await client.query('COMMIT');
    } finally {
      client.release();
    }
    const committed = Date.now();
    await reader.untilSynced(5, 6000);
    const delay = Date.now() - committed;
    reader.destroy();
    note(`delivered ${delay} ms after commit with no wake-up (poll interval 2 s ± 25%)`);
    expect(delay).toBeLessThan(3500);
    expect(reader.points.map((x) => x[0])).toEqual([1, 2, 3, 4, 5]);
  });
});

test.describe('duplicates', () => {
  test('the same batch 8 times at once, then re-batched: stored once, delivered once, counted', async () => {
    const p = await createProject('sf duplicates');
    const run = await createRun(p.key, 'duplicates');
    const batch = points('m', 0, 200, (s) => Math.sin(s));
    const reader = new SseClient(run.id, p.key);
    await reader.open();

    const results = await Promise.all(Array.from({ length: 8 }, () => ingest(p.key, run.id, batch)));
    expect(results.map((r) => r.status)).toEqual(Array(8).fill(200));
    const sum = (f: string) => results.reduce((n, r) => n + r.json[f], 0);
    expect({ accepted: sum('accepted'), duplicates: sum('duplicates'), conflicts: sum('conflicts') }).toEqual({ accepted: 200, duplicates: 1400, conflicts: 0 });

    // A retry that overlaps two old batches and adds new points, with a duplicate inside the batch itself.
    const overlap: Point[] = [...batch.slice(150), ...points('m', 200, 50, (s) => Math.sin(s)), batch[10]!, batch[10]!];
    const res = await ingest(p.key, run.id, overlap);
    expect(res.json).toEqual({ accepted: 50, duplicates: 52, conflicts: 0, last_seq: 250 });

    await reader.untilSynced(250);
    reader.destroy();
    const rows = await dbPoints(run.id);
    expect(rows.map((x) => x[0])).toEqual(seqs(250));
    expect(new Set(rows.map((x) => x[2])).size, 'each step stored once').toBe(250);
    expect(reader.points).toEqual(rows);
    expect(reader.dups).toBe(0);
    note(`8 identical concurrent batches of 200: accepted ${sum('accepted')}, duplicates ${sum('duplicates')}; reader got ${reader.points.length} points, none twice`);
  });

  test('a different value for a stored point is rejected and counted, never silently overwritten', async () => {
    const p = await createProject('sf conflict');
    const run = await createRun(p.key, 'conflict');
    await ingest(p.key, run.id, [{ key: 'loss', step: 1, value: 0.5 }]);
    const res = await ingest(p.key, run.id, [{ key: 'loss', step: 1, value: 0.9 }, { key: 'loss', step: 2, value: 0.4 }, { key: 'loss', step: 2, value: 0.3 }]);
    expect(res.json).toEqual({ accepted: 1, duplicates: 0, conflicts: 2, last_seq: 2 });
    expect(await dbPoints(run.id)).toEqual([[1, 'loss', 1, 0.5], [2, 'loss', 2, 0.4]]);
    expect(await dbRun(run.id)).toEqual({ last_seq: 2, conflicts: 2, stored: 2 });
    expect((await call(`/runs/${run.id}`, { key: p.key })).json.run.conflicts, 'visible to the user through the API').toBe(2);
  });
});

test.describe('queue overflow for a slow client', () => {
  test('a reader that stops reading: server memory stays flat, it is cut off, and resumes with no loss', async () => {
    const p = await createProject('sf slow');
    const run = await createRun(p.key, 'slow');
    const longKey = (i: number) => `slow_client/${'k'.repeat(100)}/${i % 4}`;
    const before = await stats();

    const reader = new SseClient(run.id, p.key, { startPaused: true });
    await reader.open(); // connected, and from now on reads nothing

    // Write until the server reports a stream blocked on a full socket, then 40 batches more.
    let written = 0;
    let blockedAt = 0;
    let maxBuffered = 0;
    let maxRss = before.memory.rss;
    for (let batch = 0; batch < 600 && (blockedAt === 0 || batch < blockedAt + 40); batch++) {
      const pts: Point[] = Array.from({ length: 1000 }, (_, i) => ({ key: longKey(i), step: batch * 250 + (i >> 2), value: batch + i / 1000 }));
      expect((await ingest(p.key, run.id, pts)).status).toBe(200);
      written += 1000;
      const s = await stats();
      maxBuffered = Math.max(maxBuffered, s.streams.max_buffered_bytes);
      maxRss = Math.max(maxRss, s.memory.rss);
      if (blockedAt === 0 && s.streams.waiting_for_drain >= 1) blockedAt = batch;
    }
    expect(blockedAt, 'backpressure engaged (the stream is waiting for the socket to drain)').toBeGreaterThan(0);
    const pendingBytes = written * 135; // ~135 bytes per point on the wire
    note(`reader paused; ${written} points (~${(pendingBytes / 1e6).toFixed(0)} MB of stream) written; server blocked on the socket after batch ${blockedAt}`);
    note(`server-side buffer for that stream: max ${(maxBuffered / 1024).toFixed(0)} KiB; RSS ${(before.memory.rss / 1e6).toFixed(0)} MB -> max ${(maxRss / 1e6).toFixed(0)} MB`);
    expect(maxBuffered, 'the server holds at most about one page per stream, not the backlog').toBeLessThan(1024 * 1024);
    expect(maxRss - before.memory.rss, 'RSS did not grow with the backlog').toBeLessThan(pendingBytes / 2 + 60e6);

    // Still not reading: after the drain timeout (30 s) the server must give up on this client.
    const closedBefore = before.counters.streams_closed_slow;
    await until('server to disconnect the stuck reader', async () => (await stats()).counters.streams_closed_slow > closedBefore, 45_000, 500);
    note('server closed the stuck stream after the drain timeout (streams_closed_slow +1)');

    // The client wakes up: it gets what was already in flight, sees the close, and resumes from its cursor.
    reader.resume();
    await until('reader to see the connection close', () => reader.ended, 20_000);
    const gotBeforeClose = reader.points.length;
    expect(gotBeforeClose).toBeLessThan(written);
    reader.startPaused = false;
    while (reader.cursor < written) {
      await reader.open();
      await until('resumed reader to finish or be cut again', () => reader.cursor >= written || reader.ended, 60_000);
    }
    reader.destroy();
    note(`after resume: ${gotBeforeClose} points arrived from the old connection, the rest over ${reader.connections - 1} reconnect(s); dups ${reader.dups}, gaps ${reader.gaps}`);
    expect(reader.gaps).toBe(0);
    expect(reader.dups).toBe(0);
    expect(reader.points.map((x) => x[0])).toEqual(seqs(written));
    expect((await dbRun(run.id)).stored).toBe(written);
  });
});

test.describe('partial writes', () => {
  test('one invalid point rejects the whole batch: nothing of it is stored', async () => {
    const p = await createProject('sf atomic');
    const run = await createRun(p.key, 'atomic');
    const good = points('m', 0, 100);
    const cases: Array<[string, unknown]> = [
      ['null value (what JSON.stringify makes of NaN)', null],
      ['numeric string', '0.5'],
      ['boolean', true],
      ['overflow to Infinity', '__1e999__'],
      ['object', { v: 1 }],
    ];
    for (const [name, value] of cases) {
      const body = JSON.stringify({ points: [...good.slice(0, 57), { key: 'm', step: 57, value }, ...good.slice(58)] }).replace('"__1e999__"', '1e999');
      const res = await call(`/runs/${run.id}/metrics`, { key: p.key, raw: body });
      expect(res.status, name).toBe(400);
      expect(res.json.detail, name).toBe('points[57].value must be a finite number');
    }
    const badSteps: unknown[] = [-1, 1.5, '3', 2 ** 53, null];
    for (const step of badSteps) expect((await call(`/runs/${run.id}/metrics`, { key: p.key, body: { points: [...good, { key: 'm', step, value: 1 }] } })).status, `step ${step}`).toBe(400);
    expect((await ingest(p.key, run.id, points('m', 0, 1001))).status, '1001 points').toBe(400);
    expect((await call(`/runs/${run.id}/metrics`, { key: p.key, raw: `{"points":[{"key":"m","step":0,"value":1}],"pad":"${'x'.repeat(1_100_000)}"}` })).status, 'body over 1 MB').toBe(413);
    expect(await dbRun(run.id), 'after 12 rejected batches').toEqual({ last_seq: 0, conflicts: 0, stored: 0 });

    expect((await ingest(p.key, run.id, good)).json).toEqual({ accepted: 100, duplicates: 0, conflicts: 0, last_seq: 100 });
    note('5 bad values, 5 bad steps, 1001 points, 1.1 MB body: all rejected, 0 rows stored; the corrected batch stored 100');
  });

  test('a batch whose database connection is killed between INSERT and COMMIT leaves nothing behind', async () => {
    const p = await createProject('sf partial');
    const run = await createRun(p.key, 'partial');
    await ingest(p.key, run.id, points('m', 0, 10));
    const before = await dbRun(run.id);
    const batch = points('m', 10, 1000);
    const tuples = async () => Number((await db.query("SELECT n_tup_ins FROM pg_stat_user_tables WHERE relname = 'points'")).rows[0].n_tup_ins);
    const insertedBefore = await tuples();

    // An uncommitted row that collides with point #500 of the batch. The API's INSERT writes
    // 500 rows inside its transaction and then has to wait for us.
    const blocker = await db.connect();
    let status = 0;
    try {
      await blocker.query('BEGIN');
      await blocker.query("INSERT INTO points (run_id, seq, key, step, value) VALUES ($1, 1000000000, 'm', 510, 0)", [run.id]);
      const pending = ingest(p.key, run.id, batch);
      const pid = await until('the API transaction to block on our row', async () =>
        (await db.query("SELECT pid FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query LIKE 'INSERT INTO points%' AND pid <> pg_backend_pid()")).rows[0]?.pid, 8000);
      await db.query('SELECT pg_terminate_backend($1)', [pid]); // the connection dies mid-transaction
      const res = await pending;
      status = res.status;
      expect(res.status).toBe(503);
      expect(res.headers.get('retry-after')).toBe('1');
      await blocker.query('ROLLBACK');
    } finally {
      blocker.release();
    }

    expect(await dbRun(run.id), 'no row of the killed batch is visible and the head did not move').toEqual(before);
    await sleep(1200); // table statistics are flushed asynchronously
    const written = (await tuples()) - insertedBefore;
    note(`API answered ${status}; rows physically written by the killed transaction: ${written} (+1 ours); visible afterwards: 0`);
    expect(written, 'the transaction really had written rows before it died').toBeGreaterThanOrEqual(500);

    // The client's retry of the same batch succeeds completely, and the API is healthy.
    expect((await ingest(p.key, run.id, batch)).json).toEqual({ accepted: 1000, duplicates: 0, conflicts: 0, last_seq: 1010 });
    expect((await dbPoints(run.id)).map((x) => x[0])).toEqual(seqs(1010));
    expect((await call('/healthz')).status).toBe(200);
  });
});

test.describe('an old API key that keeps working after rotation', () => {
  test('after rotation the old key is dead everywhere: REST, new streams and streams already open', async () => {
    const p = await createProject('sf rotation');
    const run = await createRun(p.key, 'rotation');
    await ingest(p.key, run.id, points('m', 0, 5));
    const openStream = new SseClient(run.id, p.key);
    await openStream.open();
    await openStream.untilSynced(5);

    const t0 = Date.now();
    const fresh = await rotate(p.key);
    const rotatedAt = Date.now();

    // 1. REST with the old key, immediately.
    expect((await call('/runs', { key: p.key })).status).toBe(401);
    expect((await call(`/runs/${run.id}`, { key: p.key })).status).toBe(401);
    expect((await ingest(p.key, run.id, points('m', 100, 5))).status).toBe(401);
    expect((await call('/keys/rotate', { key: p.key, method: 'POST' })).status, 'cannot rotate again with the old key').toBe(401);
    // 2. A new stream with the old key.
    expect(await new SseClient(run.id, p.key).open()).toBe(401);
    // 3. The stream that was already open.
    await until('open stream of the old key to be closed', () => openStream.ended, 5000, 5);
    const closedAfter = Date.now() - rotatedAt;
    expect(openStream.revoked, 'it was told why').toBe(true);

    // The new key works, and what it writes never reached the old stream.
    expect((await ingest(fresh, run.id, points('m', 5, 5))).json).toMatchObject({ accepted: 5, last_seq: 10 });
    await sleep(300);
    expect(openStream.points).toHaveLength(5);
    expect((await dbRun(run.id)).stored).toBe(10);
    expect((await call('/runs', { key: fresh })).status).toBe(200);
    note(`rotation took ${rotatedAt - t0} ms; old key: 401 on REST at once; open stream closed ${closedAfter} ms after the rotation response`);
    expect(closedAfter).toBeLessThan(1000);
  });

  test('a key revoked behind this process\'s back (another instance) still loses its open streams within a poll', async () => {
    const p = await createProject('sf rotation poll');
    const run = await createRun(p.key, 'rotation poll');
    const stream = new SseClient(run.id, p.key);
    await stream.open();
    await stream.untilSynced(0);

    await db.query('UPDATE api_keys SET revoked_at = now() WHERE project_id = $1', [p.id]);
    const revokedAt = Date.now();
    await until('stream to be closed by the periodic key check', () => stream.ended, 6000, 10);
    const delay = Date.now() - revokedAt;
    note(`key revoked directly in the database: open stream closed after ${delay} ms (poll interval 2 s ± 25%)`);
    expect(stream.revoked).toBe(true);
    expect(delay).toBeLessThan(3500);
    expect((await call('/runs', { key: p.key })).status, 'no auth cache: REST is rejected at once').toBe(401);
  });

  test('two rotations racing with the same key: exactly one wins', async () => {
    const p = await createProject('sf rotation race');
    const results = await Promise.all(Array.from({ length: 6 }, () => call('/keys/rotate', { key: p.key, method: 'POST' })));
    const statuses = results.map((r) => r.status).sort();
    expect(statuses).toEqual([201, 401, 401, 401, 401, 401]);
    const active = await db.query('SELECT count(*)::int AS n FROM api_keys WHERE project_id = $1 AND revoked_at IS NULL', [p.id]);
    expect(active.rows[0].n, 'exactly one active key afterwards').toBe(1);
  });
});
