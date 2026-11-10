// Thin HTTP client for the tests. No retries, no hidden behaviour: a test sees exactly what the server said.

export const API_URL = process.env.API_URL ?? 'http://localhost:8080';
/** The same API behind toxiproxy: the browser goes through this one. */
export const PROXY_URL = process.env.PROXY_URL ?? 'http://localhost:8666';
/** A second proxy for the metric sender, so its faults are independent of the browser's. */
export const INGEST_PROXY_URL = process.env.INGEST_PROXY_URL ?? 'http://localhost:8667';
export const ADMIN_TOKEN = process.env.ADMIN_TOKEN ?? 'dev-admin-token';

export type Point = { key: string; step: number; value: number };
/** [seq, key, step, value] */
export type StoredPoint = [number, string, number, number];
export type Res = { status: number; text: string; json: any; headers: Headers };
export type Project = { id: string; key: string };

type CallOptions = { method?: string; key?: string; body?: unknown; raw?: string | Uint8Array; base?: string; headers?: Record<string, string>; signal?: AbortSignal };

export async function call(path: string, o: CallOptions = {}): Promise<Res> {
  const hasBody = o.body !== undefined || o.raw !== undefined;
  const res = await fetch((o.base ?? API_URL) + path, {
    method: o.method ?? (hasBody ? 'POST' : 'GET'),
    headers: {
      ...(o.key ? { authorization: `Bearer ${o.key}` } : {}),
      ...(hasBody ? { 'content-type': 'application/json' } : {}),
      ...o.headers,
    },
    body: o.raw ?? (o.body !== undefined ? JSON.stringify(o.body) : undefined),
    signal: o.signal,
  });
  const text = await res.text();
  let json: any = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* not JSON */
  }
  return { status: res.status, text, json, headers: res.headers };
}

export async function createProject(name: string): Promise<Project> {
  const res = await call('/projects', { body: { name } });
  if (res.status !== 201) throw new Error(`createProject: HTTP ${res.status} ${res.text}`);
  return { id: res.json.project.id, key: res.json.api_key };
}

export async function createRun(key: string, name: string): Promise<{ id: string; name: string }> {
  const res = await call('/runs', { key, body: { name } });
  if (res.status !== 201) throw new Error(`createRun: HTTP ${res.status} ${res.text}`);
  return res.json.run;
}

export const ingest = (key: string, runId: string, points: Point[], base = API_URL, signal?: AbortSignal): Promise<Res> =>
  call(`/runs/${runId}/metrics`, { key, body: { points }, base, signal });

export async function rotate(key: string): Promise<string> {
  const res = await call('/keys/rotate', { key, method: 'POST' });
  if (res.status !== 201) throw new Error(`rotate: HTTP ${res.status} ${res.text}`);
  return res.json.api_key;
}

export async function stats(): Promise<any> {
  const res = await call('/internal/stats', { key: ADMIN_TOKEN });
  if (res.status !== 200) throw new Error(`stats: HTTP ${res.status}`);
  return res.json;
}

/** All points of a run through the public API, page by page. */
export async function apiPoints(key: string, runId: string, pageSize = 100, base = API_URL): Promise<StoredPoint[]> {
  const all: StoredPoint[] = [];
  for (let after = 0; ; ) {
    const res = await call(`/runs/${runId}/metrics?after=${after}&limit=${pageSize}`, { key, base });
    if (res.status !== 200) throw new Error(`apiPoints: HTTP ${res.status}`);
    all.push(...res.json.points);
    if (res.json.points.length < pageSize) return all;
    after = res.json.next_after;
  }
}

export const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** Polls until fn returns something truthy. Throws with the label on timeout. */
export async function until<T>(label: string, fn: () => Promise<T> | T, timeoutMs = 10_000, everyMs = 25): Promise<NonNullable<T>> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const v = await fn();
    if (v) return v as NonNullable<T>;
    if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs} ms waiting for: ${label}`);
    await sleep(everyMs);
  }
}

/** Evidence lines end up in the raw test output next to the test name. */
export const note = (...parts: unknown[]) => console.log('      │', ...parts);

/** Deterministic PRNG so a failing run can be replayed with the same seed. */
export function seeded(seed: number) {
  let a = seed >>> 0;
  const next = () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  return {
    next,
    int: (lo: number, hi: number) => lo + Math.floor(next() * (hi - lo + 1)),
    chance: (p: number) => next() < p,
    shuffle<T>(items: T[]): T[] {
      const out = [...items];
      for (let i = out.length - 1; i > 0; i--) {
        const j = Math.floor(next() * (i + 1));
        [out[i], out[j]] = [out[j]!, out[i]!];
      }
      return out;
    },
  };
}

export const points = (key: string, fromStep: number, count: number, value: (step: number) => number = (s) => s / 8): Point[] =>
  Array.from({ length: count }, (_, i) => ({ key, step: fromStep + i, value: value(fromStep + i) }));
