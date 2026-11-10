// All SQL lives here. Rule: every function that touches a run takes projectId and puts
// `project_id = $n` in the statement. There is no way to reach a run by id alone.

import { createHash, randomBytes } from 'node:crypto';
import { pool, tx } from './db.js';

export type Auth = { keyId: string; projectId: string };
export type Run = { id: string; name: string; last_seq: number; conflicts: number; created_at: string; updated_at: string };
export type Point = { key: string; step: number; value: number };
/** [seq, key, step, value] — the wire format of a stored point. */
export type StoredPoint = [number, string, number, number];
export type IngestResult = { accepted: number; duplicates: number; conflicts: number; last_seq: number };

const RUN_COLS = 'id, name, last_seq, conflicts, created_at, updated_at';
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const KEY_RE = /^sk_[A-Za-z0-9_-]{43}$/;

/** Checked before any query so a malformed id never reaches Postgres (and its error text). */
export const isUuid = (s: unknown): s is string => typeof s === 'string' && UUID_RE.test(s);

// ---------- API keys ----------

function newApiKey(): { key: string; hash: Buffer; prefix: string } {
  const key = 'sk_' + randomBytes(32).toString('base64url');
  return { key, hash: hashKey(key), prefix: key.slice(0, 7) };
}

const hashKey = (key: string): Buffer => createHash('sha256').update(key).digest();

export async function authenticate(header: string | undefined): Promise<Auth | null> {
  const key = header?.startsWith('Bearer ') ? header.slice(7) : '';
  if (!KEY_RE.test(key)) return null;
  const { rows } = await pool.query(
    'SELECT id, project_id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL',
    [hashKey(key)],
  );
  return rows[0] ? { keyId: rows[0].id, projectId: rows[0].project_id } : null;
}

export async function isKeyActive(keyId: string): Promise<boolean> {
  const { rowCount } = await pool.query('SELECT 1 FROM api_keys WHERE id = $1 AND revoked_at IS NULL', [keyId]);
  return rowCount === 1;
}

/**
 * Revokes the calling key and issues a new one in the same transaction, so there is no
 * moment with two valid keys or with none. Returns null if the key was already revoked
 * (two rotations racing: the second one waits on the row lock, then sees revoked_at set).
 */
export async function rotateKey(auth: Auth): Promise<string | null> {
  return tx(async (c) => {
    const revoked = await c.query(
      'UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND project_id = $2 AND revoked_at IS NULL',
      [auth.keyId, auth.projectId],
    );
    if (revoked.rowCount !== 1) return null;
    const k = newApiKey();
    await c.query('INSERT INTO api_keys (project_id, key_hash, prefix) VALUES ($1, $2, $3)', [auth.projectId, k.hash, k.prefix]);
    return k.key;
  });
}

// ---------- projects and runs ----------

export async function createProject(name: string): Promise<{ id: string; name: string; api_key: string }> {
  return tx(async (c) => {
    const { rows } = await c.query('INSERT INTO projects (name) VALUES ($1) RETURNING id, name', [name]);
    const k = newApiKey();
    await c.query('INSERT INTO api_keys (project_id, key_hash, prefix) VALUES ($1, $2, $3)', [rows[0].id, k.hash, k.prefix]);
    return { id: rows[0].id, name: rows[0].name, api_key: k.key };
  });
}

export async function getProject(projectId: string): Promise<{ id: string; name: string } | null> {
  const { rows } = await pool.query('SELECT id, name FROM projects WHERE id = $1', [projectId]);
  return rows[0] ?? null;
}

export async function createRun(projectId: string, name: string): Promise<Run> {
  const { rows } = await pool.query(`INSERT INTO runs (project_id, name) VALUES ($1, $2) RETURNING ${RUN_COLS}`, [projectId, name]);
  return rows[0];
}

export async function listRuns(projectId: string): Promise<Run[]> {
  const { rows } = await pool.query(
    `SELECT ${RUN_COLS} FROM runs WHERE project_id = $1 ORDER BY created_at DESC, id LIMIT 500`,
    [projectId],
  );
  return rows;
}

/** The choke point: the only way to turn a run id into a run. */
export async function getRun(projectId: string, runId: string): Promise<Run | null> {
  if (!isUuid(runId)) return null;
  const { rows } = await pool.query(`SELECT ${RUN_COLS} FROM runs WHERE id = $1 AND project_id = $2`, [runId, projectId]);
  return rows[0] ?? null;
}

// ---------- points ----------

/**
 * Points after a cursor, in seq order. Used by the SSE stream and by GET /runs/:id/metrics.
 * The join re-checks the project on every read, not only when the stream was opened.
 */
export async function readPoints(projectId: string, runId: string, after: number, limit: number): Promise<StoredPoint[]> {
  if (!isUuid(runId)) return [];
  const { rows } = await pool.query({
    name: 'read-points',
    text: `SELECT p.seq, p.key, p.step, p.value
             FROM points p JOIN runs r ON r.id = p.run_id
            WHERE p.run_id = $1 AND r.project_id = $2 AND p.seq > $3
            ORDER BY p.seq LIMIT $4`,
    values: [runId, projectId, after, limit],
    rowMode: 'array',
  });
  return rows as StoredPoint[];
}

const idOf = (key: string, step: number): string => `${key}\u0000${step}`; // key charset excludes \0

/**
 * Stores a batch atomically and idempotently. Returns null if the run is not in this project.
 *
 * The row lock on the run serializes writers to one run. That gives two things:
 *  - seq is gapless and in commit order, so a reader's "seq > cursor" never skips a row;
 *  - nobody can insert the same (key, step) between our "what exists?" read and our insert.
 */
export async function ingest(projectId: string, runId: string, points: Point[]): Promise<IngestResult | null> {
  if (!isUuid(runId)) return null;

  // Duplicates inside the batch itself: the first occurrence wins.
  const unique = new Map<string, Point>();
  let duplicates = 0;
  let conflicts = 0;
  for (const p of points) {
    const first = unique.get(idOf(p.key, p.step));
    if (!first) unique.set(idOf(p.key, p.step), p);
    else if (first.value === p.value) duplicates++;
    else conflicts++;
  }
  const batch = [...unique.values()];

  return tx(async (c) => {
    // NO KEY UPDATE, not UPDATE: it still excludes other writers of this run, but does not
    // collide with the KEY SHARE lock that every insert into points takes via the foreign key.
    const run = await c.query('SELECT last_seq FROM runs WHERE id = $1 AND project_id = $2 FOR NO KEY UPDATE', [runId, projectId]);
    if (run.rowCount !== 1) return null;
    let lastSeq: number = run.rows[0].last_seq;

    const existing = await c.query(
      `SELECT p.key, p.step, p.value
         FROM unnest($2::text[], $3::bigint[]) AS i(key, step)
         JOIN points p ON p.run_id = $1 AND p.key = i.key AND p.step = i.step`,
      [runId, batch.map((p) => p.key), batch.map((p) => p.step)],
    );
    const stored = new Map<string, number>(existing.rows.map((r) => [idOf(r.key, r.step), r.value]));

    const fresh: Point[] = [];
    for (const p of batch) {
      const value = stored.get(idOf(p.key, p.step));
      if (value === undefined) fresh.push(p);
      else if (value === p.value) duplicates++;
      else conflicts++; // first write wins; counted so the dropped value is visible
    }

    if (fresh.length > 0) {
      await c.query(
        `INSERT INTO points (run_id, seq, key, step, value)
         SELECT $1, i.seq, i.key, i.step, i.value
           FROM unnest($2::bigint[], $3::text[], $4::bigint[], $5::float8[]) AS i(seq, key, step, value)`,
        [runId, fresh.map((_, i) => lastSeq + i + 1), fresh.map((p) => p.key), fresh.map((p) => p.step), fresh.map((p) => p.value)],
      );
      lastSeq += fresh.length;
    }
    if (fresh.length > 0 || conflicts > 0) {
      await c.query('UPDATE runs SET last_seq = $2, conflicts = conflicts + $3, updated_at = now() WHERE id = $1', [runId, lastSeq, conflicts]);
    }
    return { accepted: fresh.length, duplicates, conflicts, last_seq: lastSeq };
  });
}
