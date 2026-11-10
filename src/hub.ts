// Registry of open SSE streams. It carries no data: a "wake" is only a hint that a run has
// something new, and every stream re-reads the database (with the project filter) itself.

import { config } from './config.js';

export interface StreamHandle {
  readonly projectId: string;
  readonly keyId: string;
  readonly runId: string;
  wake(): void;
  close(reason: CloseReason): void;
  bufferedBytes(): number;
  lag(): { cursor: number; waitingForDrain: boolean };
}
export type CloseReason = 'client' | 'revoked' | 'slow' | 'error' | 'shutdown';

const all = new Set<StreamHandle>();
const byRun = new Map<string, Set<StreamHandle>>();
const perProject = new Map<string, number>();

export const counters = {
  ingest_batches: 0,
  points_accepted: 0,
  points_duplicate: 0,
  points_conflict: 0,
  streams_opened: 0,
  streams_closed_client: 0,
  streams_closed_revoked: 0,
  streams_closed_slow: 0,
  streams_closed_error: 0,
  streams_closed_shutdown: 0,
  streams_rejected_limit: 0,
  auth_failures: 0,
  errors_5xx: 0,
};

/** False when the project already has its maximum number of streams. */
export function register(s: StreamHandle): boolean {
  const open = perProject.get(s.projectId) ?? 0;
  if (open >= config.maxStreamsPerProject) {
    counters.streams_rejected_limit++;
    return false;
  }
  perProject.set(s.projectId, open + 1);
  all.add(s);
  let set = byRun.get(s.runId);
  if (!set) byRun.set(s.runId, (set = new Set()));
  set.add(s);
  counters.streams_opened++;
  return true;
}

export function unregister(s: StreamHandle, reason: CloseReason): void {
  if (!all.delete(s)) return;
  const open = (perProject.get(s.projectId) ?? 1) - 1;
  if (open <= 0) perProject.delete(s.projectId);
  else perProject.set(s.projectId, open);
  const set = byRun.get(s.runId);
  set?.delete(s);
  if (set?.size === 0) byRun.delete(s.runId);
  counters[`streams_closed_${reason}`]++;
}

export function wakeRun(runId: string): void {
  const set = byRun.get(runId);
  if (set) for (const s of set) s.wake();
}

export function closeStreamsOfKey(keyId: string): void {
  for (const s of [...all]) if (s.keyId === keyId) s.close('revoked');
}

export function closeAll(reason: CloseReason): void {
  for (const s of [...all]) s.close(reason);
}

export function stats() {
  let buffered = 0;
  let maxBuffered = 0;
  let waiting = 0;
  for (const s of all) {
    const b = s.bufferedBytes();
    buffered += b;
    if (b > maxBuffered) maxBuffered = b;
    if (s.lag().waitingForDrain) waiting++;
  }
  return { open: all.size, waiting_for_drain: waiting, buffered_bytes: buffered, max_buffered_bytes: maxBuffered };
}
