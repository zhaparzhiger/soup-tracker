# Soup Tracker — mini experiment tracker

Take-home task for the Soup internship (Full-Stack Engineer). A client sends training metrics in
batches over an unreliable network; a web page shows the run live. The point of the exercise is
two proofs: **the page always matches what is stored**, and **a project only sees its own data**.

| Read this | For |
|---|---|
| [DESIGN.md](DESIGN.md) | The one-page design, written and committed **before** the code (first commit). |
| [NOTE.md](NOTE.md) | Results: evidence, what the tests do not catch, silent failures, verdict, AI use. |
| [test-output/](test-output/) | Raw output of every test run, including the failed ones. |

## Run it

```bash
docker compose up --build
```

Open <http://localhost:8080>, create a project and save the API key it shows (once). Then send a
demo run with the bundled sender and watch the page update:

```bash
docker compose exec api node tools/sim.mjs --url http://127.0.0.1:8080 --key sk_YOUR_KEY
```

Or by hand:

```bash
curl -X POST localhost:8080/runs -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' -d '{"name":"my run"}'
curl -X POST localhost:8080/runs/$RUN/metrics -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
     -d '{"points":[{"key":"loss","step":1,"value":0.9},{"key":"loss","step":2,"value":0.7}]}'
```

## API

All routes except the first two need `Authorization: Bearer <api key>`.

| Route | What it does |
|---|---|
| `POST /projects` `{name}` | Creates a project. Returns its API key, once. |
| `GET /healthz` | Liveness, including the database. |
| `GET /project` | The project this key belongs to. |
| `POST /keys/rotate` | Revokes the calling key and returns a new one, atomically. |
| `GET /runs` · `POST /runs` `{name}` · `GET /runs/{id}` | Run list, create, read. |
| `POST /runs/{id}/metrics` `{points:[{key,step,value}]}` | Ingest a batch (≤1000 points, ≤1 MB). Atomic and idempotent. Answers `{accepted, duplicates, conflicts, last_seq}`. |
| `GET /runs/{id}/metrics?after=<seq>&limit=` | The stored points after a cursor, as JSON pages. |
| `GET /runs/{id}/events` | SSE stream of the same log. Resume with `Last-Event-ID: <seq>`. |
| `GET /internal/stats` | Operational counters. Needs the admin token (`ADMIN_TOKEN`). |

## Tests

Everything runs in Docker; nothing is needed on the host.

```bash
# the whole suite: browser end-to-end, isolation, silent failures, database invariants (~1.5 min)
docker compose --profile test run --rm --build tests

# load test behind the verdict (parameters: LOAD_RUNS, LOAD_RATE, LOAD_SECONDS, LOAD_FLUSH_MS, LOAD_VIEWERS)
docker compose --profile test run --rm tests npm run load

# break the server on purpose and check that the tests notice (host, needs bash + perl + git)
bash tests/mutation-check.sh
```

The suite uses [toxiproxy](https://github.com/Shopify/toxiproxy) between the clients and the API,
so "the connection drops" is a real TCP reset or a real silent blackhole, not a flag in the code.
There are **no test hooks in the server**: faults come from the network proxy or from the database
(a killed backend, a held lock).

| File | Proves |
|---|---|
| `tests/specs/01-consistency.e2e.spec.ts` | sent == database == API == page, through disorder, duplicates, late points and two browser outages; and the page is never ahead of the database while it runs. |
| `tests/specs/02-isolation.spec.ts` | Project A cannot read, write, subscribe to, probe, or tell apart "not yours" from "not found"; cache headers; error bodies; logs; timing. |
| `tests/specs/03-silent-failures.spec.ts` | Reconnect loss, duplicates, slow client, partial writes, key rotation — each provoked for real. |
| `tests/specs/04-invariants.spec.ts` | For every run in the database: `last_seq == count(points)`, `seq` is `1..N`, no duplicates. |
| `tests/load/load.spec.ts` | 50 runs × 10 events/s with live viewers; latency, lag, and the same correctness checks. |

## Layout

```
src/        API server (TypeScript, Fastify, node-postgres)
  repo.ts     every SQL statement; each one that touches a run carries the project id
  stream.ts   one SSE connection = one cursor; no queue, no replay buffer
  routes.ts   HTTP routes and strict input validation
  hub.ts      registry of open streams (wake-up hints, limits, counters)
  schema.sql  four tables
web/        the UI: three static files, no framework
tools/      demo sender
tests/      Playwright specs, helpers, load test, mutation check
```

Stack: Node 22, TypeScript, Fastify 5, PostgreSQL 17, plain JS + SVG in the browser, Playwright.
