import { API_URL, call, until } from './api.js';
import { setupProxies } from './toxi.js';

export default async function globalSetup(): Promise<void> {
  await until('API to be healthy', async () => (await call('/healthz').catch(() => null))?.status === 200, 60_000, 500);
  await setupProxies();
  console.log(`target ${API_URL} · proxies ready`);
}
