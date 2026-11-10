// One SSE connection = one cursor into a run's event log. No queue, no replay buffer.
//
// The pump reads "points after my cursor" from the database, writes one page, and reads the
// next page only after the socket has drained. First load, catch-up after a disconnect and
// live tailing are this same loop.

import type { ServerResponse } from 'node:http';
import type { FastifyBaseLogger } from 'fastify';
import { config } from './config.js';
import * as hub from './hub.js';
import { isKeyActive, readPoints, type Auth } from './repo.js';

export class RunStream implements hub.StreamHandle {
  readonly projectId: string;
  readonly keyId: string;
  private cursor: number;
  private closed = false;
  private pumping = false;
  private dirty = false;
  private lastWriteAt = Date.now();
  private lastSyncSent = -1;
  private cancelDrainWait: (() => void) | null = null;
  private timer: NodeJS.Timeout | null = null;

  constructor(
    auth: Auth,
    readonly runId: string,
    after: number,
    private readonly res: ServerResponse,
    private readonly log: FastifyBaseLogger,
  ) {
    this.projectId = auth.projectId;
    this.keyId = auth.keyId;
    this.cursor = after;
  }

  /** Call once, after hub.register() accepted the stream and the SSE headers were written. */
  start(): void {
    this.res.on('close', () => this.close('client'));
    this.res.on('error', () => this.close('client'));
    // Jitter so that streams opened together do not poll in lockstep.
    this.timer = setInterval(() => void this.tick(), config.streamPollMs * (0.75 + Math.random() / 2));
    this.wake();
  }

  /** Something may be new for this run. Safe to call at any time, from anywhere. */
  wake(): void {
    if (this.closed) return;
    if (this.pumping) this.dirty = true;
    else void this.pump();
  }

  private async pump(): Promise<void> {
    this.pumping = true;
    try {
      do {
        this.dirty = false;
        for (;;) {
          const rows = await readPoints(this.projectId, this.runId, this.cursor, config.streamPageSize);
          if (this.closed) return;
          if (rows.length === 0) break;
          const last = rows[rows.length - 1]![0];
          const flushed = this.write(`id: ${last}\nevent: points\ndata: ${JSON.stringify(rows)}\n\n`);
          this.cursor = last;
          if (!flushed && !(await this.waitForDrain())) return;
          if (rows.length < config.streamPageSize) break;
        }
        // "As of this read, nothing exists after seq". Lets the page say `live` honestly.
        if (!this.closed && this.lastSyncSent !== this.cursor) {
          this.write(`event: sync\ndata: {"seq":${this.cursor}}\n\n`);
          this.lastSyncSent = this.cursor;
        }
      } while (this.dirty && !this.closed);
    } catch (err) {
      // A failed read must not leave a silent, stuck stream. Close it: the client reconnects
      // with its cursor and nothing is lost.
      this.log.warn({ err, run: this.runId }, 'stream read failed, closing');
      this.close('error');
    } finally {
      this.pumping = false;
    }
  }

  /** Resolves true when the socket drained, false if the stream was closed while waiting. */
  private waitForDrain(): Promise<boolean> {
    return new Promise((resolve) => {
      const done = (ok: boolean) => {
        clearTimeout(timeout);
        this.res.off('drain', onDrain);
        this.cancelDrainWait = null;
        resolve(ok);
      };
      const onDrain = () => done(true);
      const timeout = setTimeout(() => {
        this.close('slow'); // close() calls cancelDrainWait → done(false)
      }, config.drainTimeoutMs);
      this.cancelDrainWait = () => done(false);
      this.res.once('drain', onDrain);
    });
  }

  private async tick(): Promise<void> {
    if (this.closed) return;
    try {
      if (!(await isKeyActive(this.keyId))) return this.close('revoked');
    } catch {
      // Database hiccup: keep the stream, the next tick checks again.
    }
    if (this.closed || this.cancelDrainWait) return; // blocked on a slow client: add nothing to its buffer
    this.wake();
    if (Date.now() - this.lastWriteAt >= config.heartbeatMs) this.write(': ping\n\n');
  }

  private write(chunk: string): boolean {
    this.lastWriteAt = Date.now();
    return this.res.write(chunk);
  }

  close(reason: hub.CloseReason): void {
    if (this.closed) return;
    this.closed = true;
    if (this.timer) clearInterval(this.timer);
    hub.unregister(this, reason);
    this.log.info({ run: this.runId, cursor: this.cursor, reason }, 'stream closed');
    this.cancelDrainWait?.();
    if (reason === 'client') return;
    if (reason === 'slow' || reason === 'error') {
      this.res.destroy(); // nothing useful can be flushed
      return;
    }
    this.res.end(reason === 'revoked' ? 'event: revoked\ndata: {}\n\n' : undefined);
  }

  bufferedBytes(): number {
    return this.res.writableLength;
  }

  lag(): { cursor: number; waitingForDrain: boolean } {
    return { cursor: this.cursor, waitingForDrain: this.cancelDrainWait !== null };
  }
}

export const SSE_HEADERS = {
  'Content-Type': 'text/event-stream; charset=utf-8',
  'Cache-Control': 'no-store',
  'X-Accel-Buffering': 'no', // ask nginx-style proxies not to buffer the stream
  'X-Content-Type-Options': 'nosniff',
} as const;
