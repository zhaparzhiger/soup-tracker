// Runs last. Whatever the earlier tests (and any load test) did to the database, these must hold
// for every run. They are also the queries I would run as a periodic audit in production.

import { expect, test } from '@playwright/test';
import { note } from '../helpers/api.js';
import { db } from '../helpers/db.js';

test('for every run: last_seq == number of stored points, seq is 1..N, no (key, step) twice', async () => {
  const totals = (await db.query('SELECT (SELECT count(*) FROM runs)::int AS runs, (SELECT count(*) FROM points)::int AS points, (SELECT count(*) FROM projects)::int AS projects')).rows[0];

  const headMismatch = await db.query(
    `SELECT r.id, r.last_seq, s.n, s.min_seq, s.max_seq
       FROM runs r LEFT JOIN (SELECT run_id, count(*) AS n, min(seq) AS min_seq, max(seq) AS max_seq FROM points GROUP BY run_id) s ON s.run_id = r.id
      WHERE r.last_seq <> coalesce(s.n, 0) OR coalesce(s.max_seq, 0) <> coalesce(s.n, 0) OR coalesce(s.min_seq, 1) <> 1`,
  );
  const duplicates = await db.query('SELECT run_id, key, step, count(*) FROM points GROUP BY run_id, key, step HAVING count(*) > 1');
  const orphans = await db.query('SELECT count(*)::int AS n FROM points p LEFT JOIN runs r ON r.id = p.run_id WHERE r.id IS NULL');
  const keysPerProject = await db.query('SELECT project_id FROM api_keys WHERE revoked_at IS NULL GROUP BY project_id HAVING count(*) > 1');

  note(`${totals.projects} projects, ${totals.runs} runs, ${totals.points} points in the database`);
  note(`runs whose head disagrees with their points: ${headMismatch.rowCount} · duplicate (run, key, step): ${duplicates.rowCount} · orphan points: ${orphans.rows[0].n} · projects with two active keys: ${keysPerProject.rowCount}`);
  expect(headMismatch.rows).toEqual([]);
  expect(duplicates.rows).toEqual([]);
  expect(orphans.rows[0].n).toBe(0);
  expect(keysPerProject.rows).toEqual([]);
});
