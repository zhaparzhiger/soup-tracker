import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { config } from './config.js';

// int8 comes back as a string by default. Every bigint we store (seq, step, counters) is
// validated to be a safe integer on the way in, so a number is exact. Fail loudly if not.
pg.types.setTypeParser(20, (text) => {
  const n = Number(text);
  if (!Number.isSafeInteger(n)) throw new Error(`bigint ${text} does not fit a JS number`);
  return n;
});

export const pool = new pg.Pool({
  connectionString: config.databaseUrl,
  max: config.dbPoolSize,
  // A stuck lock or query must turn into an error the client can retry, not a hung request.
  options: '-c statement_timeout=10000 -c lock_timeout=5000 -c idle_in_transaction_session_timeout=15000',
});

export function onPoolError(handler: (err: Error) => void): void {
  // An idle connection killed by the server emits here. Without a listener Node would crash.
  pool.on('error', handler);
}

/**
 * Runs fn in one transaction. Commits if it returns, rolls back if it throws.
 * If COMMIT itself fails the outcome is unknown to us; the caller gets the error and the
 * client retries, which is safe because ingestion is idempotent.
 */
export async function tx<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  let broken: Error | undefined;
  // The pool only listens for errors on idle clients. If the connection of a checked-out client
  // dies, pg emits 'error' on it, and with no listener Node kills the whole process. (This
  // happened: test-output/run-1-api-crash.log.) The failed query still rejects below.
  const onError = (err: Error) => {
    broken = err;
  };
  client.on('error', onError);
  try {
    await client.query('BEGIN');
    const out = await fn(client);
    await client.query('COMMIT');
    return out;
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackErr) {
      broken = rollbackErr as Error; // connection is dead: tell the pool to drop it
    }
    throw err;
  } finally {
    client.removeListener('error', onError);
    client.release(broken);
  }
}

export async function migrate(): Promise<void> {
  const sql = await readFile(new URL('./schema.sql', import.meta.url), 'utf8');
  const client = await pool.connect();
  try {
    await client.query('SELECT pg_advisory_lock(727001)');
    await client.query(sql);
  } finally {
    await client.query('SELECT pg_advisory_unlock(727001)').catch(() => {});
    client.release();
  }
}

/** Errors where "try again in a moment" is the right answer for the client. */
export function isTransient(err: unknown): boolean {
  const { code, message } = (err ?? {}) as { code?: unknown; message?: unknown };
  // node-pg reports a dropped connection without a code.
  if (typeof message === 'string' && /Connection terminated|connection error|timeout exceeded when trying to connect/i.test(message)) return true;
  if (typeof code !== 'string') return false;
  return (
    code.startsWith('08') || // connection exception
    code.startsWith('53') || // insufficient resources
    code.startsWith('57') || // operator intervention: query cancelled, admin shutdown
    code === '55P03' || // lock_not_available
    code === '40001' || // serialization_failure
    code === '40P01' || // deadlock_detected
    code === 'ECONNREFUSED' ||
    code === 'ECONNRESET' ||
    code === 'EPIPE'
  );
}
