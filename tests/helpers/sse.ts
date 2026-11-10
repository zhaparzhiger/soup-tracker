// A strict SSE reader on a raw socket, so a test can pause reading, kill the connection abruptly,
// and see every byte. It records anomalies instead of hiding them: a replayed seq is a `dup`,
// a skipped seq is a `gap`.

import http from 'node:http';
import { API_URL, until, type StoredPoint } from './api.js';

export class SseClient {
  status = 0;
  headers: http.IncomingHttpHeaders = {};
  points: StoredPoint[] = [];
  cursor: number;
  lastSync: number | null = null;
  revoked = false;
  ended = false;
  bytes = 0;
  raw = '';
  dups = 0;
  gaps = 0;
  connections = 0;
  /** When true the next open() connects and then reads nothing until resume(). */
  startPaused: boolean;
  private req: http.ClientRequest | null = null;
  private res: http.IncomingMessage | null = null;
  private buffer = '';
  private generation = 0;

  constructor(
    private readonly runId: string,
    private readonly key: string,
    private readonly o: { base?: string; after?: number; startPaused?: boolean; keepRaw?: boolean; onPoint?: (p: StoredPoint) => void } = {},
  ) {
    this.cursor = o.after ?? 0;
    this.startPaused = o.startPaused ?? false;
  }

  /** Opens (or re-opens from the current cursor). Resolves with the HTTP status. */
  open(): Promise<number> {
    this.ended = false;
    this.buffer = '';
    this.connections++;
    const generation = ++this.generation; // late events of an older connection must not touch this one
    return new Promise((resolve, reject) => {
      const url = new URL(`/runs/${this.runId}/events`, this.o.base ?? API_URL);
      const req = http.request(
        url,
        { agent: false, headers: { authorization: `Bearer ${this.key}`, accept: 'text/event-stream', 'last-event-id': String(this.cursor) } },
        (res) => {
          this.res = res;
          this.status = res.statusCode ?? 0;
          this.headers = res.headers;
          res.setEncoding('utf8');
          if (this.startPaused) res.pause();
          res.on('data', (chunk: string) => generation === this.generation && this.feed(chunk));
          const finish = () => {
            if (generation === this.generation) this.ended = true;
          };
          res.on('end', finish).on('close', finish).on('error', finish);
          resolve(this.status);
        },
      );
      req.on('error', (err) => {
        if (generation === this.generation) this.ended = true;
        reject(err);
      });
      req.end();
      this.req = req;
    });
  }

  private feed(chunk: string): void {
    this.bytes += Buffer.byteLength(chunk);
    if (this.o.keepRaw) this.raw += chunk;
    this.buffer += chunk;
    let end: number;
    while ((end = this.buffer.indexOf('\n\n')) >= 0) {
      this.handle(this.buffer.slice(0, end));
      this.buffer = this.buffer.slice(end + 2);
    }
  }

  private handle(message: string): void {
    let event = 'message';
    let data = '';
    for (const line of message.split('\n')) {
      if (line.startsWith('event:')) event = line.slice(6).trim();
      else if (line.startsWith('data:')) data += line.slice(5).trim();
    }
    if (event === 'revoked') this.revoked = true;
    else if (event === 'sync') this.lastSync = JSON.parse(data).seq;
    else if (event === 'points') {
      for (const p of JSON.parse(data) as StoredPoint[]) {
        if (p[0] <= this.cursor) this.dups++;
        else if (p[0] !== this.cursor + 1) this.gaps++;
        this.points.push(p);
        this.o.onPoint?.(p);
        this.cursor = Math.max(this.cursor, p[0]);
      }
    }
  }

  pause(): void {
    this.res?.pause();
  }

  resume(): void {
    this.res?.resume();
  }

  /** Kills the socket without a clean close, like a network drop. */
  destroy(): void {
    this.req?.destroy();
    this.ended = true;
  }

  untilSeq(seq: number, timeoutMs = 15_000): Promise<true> {
    return until(`stream to reach seq ${seq} (at ${this.cursor})`, () => this.cursor >= seq, timeoutMs);
  }

  untilSynced(seq: number, timeoutMs = 15_000): Promise<true> {
    return until(`stream to sync at seq ${seq} (cursor ${this.cursor}, sync ${this.lastSync})`, () => this.cursor === seq && this.lastSync === seq, timeoutMs);
  }
}
