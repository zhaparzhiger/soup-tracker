#!/usr/bin/env node
// Demo sender: creates a run and streams a fake training curve in batches, retrying on failure.
//   node tools/sim.mjs --key sk_... [--url http://localhost:8080] [--name demo] [--steps 600] [--rate 10]
// Retries resend the same points; the server de-duplicates by (run, key, step).

const arg = (name, fallback) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 ? process.argv[i + 1] : fallback;
};
const url = arg('url', 'http://localhost:8080');
const key = arg('key');
const steps = Number(arg('steps', '600'));
const rate = Number(arg('rate', '10')); // steps per second
if (!key) {
  console.error('usage: node tools/sim.mjs --key sk_... [--url URL] [--name NAME] [--steps N] [--rate N]');
  process.exit(2);
}
const headers = { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const created = await fetch(`${url}/runs`, { method: 'POST', headers, body: JSON.stringify({ name: arg('name', `demo ${new Date().toISOString().slice(11, 19)}`) }) });
if (!created.ok) {
  console.error(`could not create a run: HTTP ${created.status}`);
  process.exit(1);
}
const { run } = await created.json();
console.log(`run ${run.id} — open ${url}/#/runs/${run.id}`);

let buffer = [];
for (let step = 0; step < steps; step++) {
  const t = step / steps;
  buffer.push(
    { key: 'train/loss', step, value: 2.5 * Math.exp(-4 * t) + 0.05 * Math.random() },
    { key: 'train/accuracy', step, value: Math.min(0.99, 1 - Math.exp(-5 * t) + 0.02 * Math.random()) },
    { key: 'lr', step, value: 0.001 * (1 - t) },
  );
  if (buffer.length >= 30 || step === steps - 1) {
    for (let attempt = 0; ; attempt++) {
      try {
        const res = await fetch(`${url}/runs/${run.id}/metrics`, { method: 'POST', headers, body: JSON.stringify({ points: buffer }) });
        if (res.ok) {
          const ack = await res.json();
          console.log(`step ${step}: accepted ${ack.accepted}, duplicates ${ack.duplicates}, last_seq ${ack.last_seq}`);
          break;
        }
        if (res.status < 500) throw new Error(`rejected: HTTP ${res.status} ${await res.text()}`);
      } catch (err) {
        if (String(err.message).startsWith('rejected')) throw err;
      }
      await sleep(Math.min(5000, 200 * 2 ** attempt));
    }
    buffer = [];
  }
  await sleep(1000 / rate);
}
console.log('done');
