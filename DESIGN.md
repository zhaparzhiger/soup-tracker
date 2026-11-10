# Design (written before the code)

Two promises: **the page never shows anything that is not stored**, and **a project only sees its own data**.

## 1. Event format and ordering

A point is `{key, step, value}`; `POST /runs/{id}/metrics` takes `{"points":[...]}` (≤1000 points, ≤1 MB).
Every point has **two orders**, kept apart on purpose:

| Order | Assigned by | Used for |
|---|---|---|
| `step` | the client | **display**: the chart sorts by step, so a late or out-of-order point is just an insert |
| `seq` | the server, per run, gapless `1..N`, in commit order | **delivery**: the live stream and catch-up |

`seq` is assigned in the ingest transaction under a row lock on the run (`SELECT … FOR UPDATE`): writers of
one run are serialized, so if `seq = n` is visible, every smaller `seq` is too. (A global auto-increment id cannot
promise that: transactions commit out of id order, and a reader already past the gap silently skips the late one.)
Points are **immutable**, so the page state is a pure function of the set of points received.

## 2. The same batch is sent again

Idempotency is **per point, not per batch**: a point's identity is `(run_id, key, step)`, a unique constraint
(a retrying client may re-batch, so a batch id is not enough). The answer says what happened:
`{"accepted": 7, "duplicates": 3, "conflicts": 0, "last_seq": 4211}`.

- **accepted** — new, stored with the next `seq`. **duplicate** — same value already stored: ignored, nothing re-sent.
- **conflict** — same `(key, step)`, *different* value: first write wins; the run's `conflicts` counter keeps the drop visible.
- A batch is **atomic**: one transaction. One invalid point rejects the whole batch (400); nothing is written.

## 3. Catching up after a disconnect

`GET /runs/{id}/events` is an SSE stream; each message has `id: <seq of its last point>`. The client reconnects
with `Last-Event-ID: <last seq applied>` and gets every later point **from the database**, then the live tail.

- **One code path** for first load, catch-up and live: "read points after my cursor". No replay buffer, so no retention window: offline for 30 s or 3 days is the same.
- **No per-client queue.** A connection is a cursor; the next page is read only after the socket drained, so a slow client cannot grow server memory. It lags, and is cut off after 30 s without draining.
- In-process notifications are only a **wake-up hint**; every stream also polls every 2 s, so a lost hint or a second instance costs latency, never data.
- The client applies a point only if `seq == cursor + 1` (lower: skip; higher: a gap, reconnect from the cursor). Heartbeat every 5 s, reconnect after 12 s of silence. The page shows `live` / `catching up` / `reconnecting` and its `seq`.

## 4. Project isolation

- A key belongs to one project: 256-bit random, stored as a SHA-256 hash, sent in the `Authorization` header, never in a URL.
- **Every query that touches a run carries the key's `project_id`.** One function turns `(project, run id)` into a run; every route, SSE included, goes through it.
- Another project's run, a missing run and a malformed id give **the same 404, same body**. Run ids are random UUIDv4.
- Wake-up hints carry no data; data is re-read with the project filter. Responses are `no-store`, no ETags.
- Rotation revokes the old key in the transaction that creates the new one; no auth cache. Streams of a revoked key are closed, and each stream re-checks its key on every poll.

**Known weak spots.** (1) Isolation lives in application code, not in the database (no row-level security): an
endpoint that forgets the filter leaks; mitigation: one choke point, and tests. (2) First load replays the whole
history: fine for thousands of points, not millions. (3) Two API instances stay correct but lag up to 2 s.
(4) First-write-wins: a client cannot correct a value.
