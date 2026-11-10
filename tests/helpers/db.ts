// Direct database access: the tests compare what the API and the page say against the stored rows.

import pg from 'pg';
import type { StoredPoint } from './api.js';

pg.types.setTypeParser(20, Number); // int8

export const db = new pg.Pool({ connectionString: process.env.DATABASE_URL ?? 'postgres://tracker:tracker@localhost:5432/tracker', max: 6 });

export async function dbPoints(runId: string): Promise<StoredPoint[]> {
  const { rows } = await db.query({ text: 'SELECT seq, key, step, value FROM points WHERE run_id = $1 ORDER BY seq', values: [runId], rowMode: 'array' });
  return rows as StoredPoint[];
}

export async function dbRun(runId: string): Promise<{ last_seq: number; conflicts: number; stored: number }> {
  const { rows } = await db.query(
    'SELECT r.last_seq, r.conflicts, (SELECT count(*) FROM points p WHERE p.run_id = r.id) AS stored FROM runs r WHERE r.id = $1',
    [runId],
  );
  return rows[0];
}
