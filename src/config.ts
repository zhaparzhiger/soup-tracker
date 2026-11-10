function num(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isFinite(n) || n < 0) throw new Error(`${name} must be a non-negative number, got "${raw}"`);
  return n;
}

export const config = {
  port: num('PORT', 8080),
  databaseUrl: process.env.DATABASE_URL ?? 'postgres://tracker:tracker@localhost:5432/tracker',
  dbPoolSize: num('DB_POOL_SIZE', 20),
  /** Empty = /internal/stats is disabled. */
  adminToken: process.env.ADMIN_TOKEN ?? '',
  /** Optional second log destination (the test suite scans it for leaked secrets). */
  logFile: process.env.LOG_FILE ?? '',
  logLevel: process.env.LOG_LEVEL ?? 'info',

  maxBatchPoints: 1000,
  maxBodyBytes: 1024 * 1024,
  maxKeyLength: 128,

  /** Points per SSE message, and the most a single stream ever holds in memory. */
  streamPageSize: num('STREAM_PAGE_SIZE', 500),
  /** Every stream re-reads the database this often even without a wake-up hint. */
  streamPollMs: num('STREAM_POLL_MS', 2000),
  heartbeatMs: num('HEARTBEAT_MS', 5000),
  /** A client whose socket stays full this long is disconnected. It can resume from its cursor. */
  drainTimeoutMs: num('STREAM_DRAIN_TIMEOUT_MS', 30000),
  maxStreamsPerProject: num('MAX_STREAMS_PER_PROJECT', 100),
} as const;
