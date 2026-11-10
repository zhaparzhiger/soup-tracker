// Control of toxiproxy: real TCP faults between a client and the API, not simulated flags.

const TOXIPROXY_URL = process.env.TOXIPROXY_URL ?? 'http://localhost:8474';
const UPSTREAM = new URL(process.env.API_URL ?? 'http://localhost:8080').host;

export type ProxyName = 'browser' | 'ingest';
const LISTEN: Record<ProxyName, string> = { browser: '0.0.0.0:8666', ingest: '0.0.0.0:8667' };

async function toxi(method: string, path: string, body?: unknown): Promise<Response> {
  const res = await fetch(TOXIPROXY_URL + path, { method, body: body ? JSON.stringify(body) : undefined });
  if (!res.ok && res.status !== 404 && res.status !== 409) throw new Error(`toxiproxy ${method} ${path}: HTTP ${res.status} ${await res.text()}`);
  return res;
}

/** Fresh proxies, enabled, without toxics. */
export async function setupProxies(): Promise<void> {
  for (const name of Object.keys(LISTEN) as ProxyName[]) {
    await toxi('DELETE', `/proxies/${name}`);
    await toxi('POST', '/proxies', { name, listen: LISTEN[name], upstream: UPSTREAM, enabled: true });
  }
}

/** Disabling a proxy resets every connection through it and refuses new ones: a hard outage. */
export const setEnabled = (name: ProxyName, enabled: boolean) => toxi('POST', `/proxies/${name}`, { enabled });

export const addToxic = (proxy: ProxyName, name: string, type: string, attributes: Record<string, number>, stream: 'upstream' | 'downstream' = 'downstream') =>
  toxi('POST', `/proxies/${proxy}/toxics`, { name, type, stream, toxicity: 1, attributes });

export const removeToxic = (proxy: ProxyName, name: string) => toxi('DELETE', `/proxies/${proxy}/toxics/${name}`);

/** Enables every proxy and removes every toxic. */
export const resetProxies = () => toxi('POST', '/reset');
