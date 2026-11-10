# Design (written before the code)

Mini experiment tracker. Two promises to keep: **the page never shows anything that is not stored**,
and **a project only ever sees its own data**.

## 1. Event format and ordering

A metric point is `{key, step, value}` — e.g. `{"key":"loss","step":120,"value":0.34}`.
`POST /runs/{id}/metrics` takes `{"points":[...]}` (max 1000 points, 1 MB).

Every point has **two orders**, and keeping them apart is the core of the design:

| Order | Who assigns it | Used for |
|---|---|---|
| `step` | the client | **display** — the chart sorts by step, so a late or out-of-order point is just an insert |
| `seq` | the server, per run, gapless `1..N`, in commit order | **delivery** — the live stream and catch-up |

`seq` is assigned inside the ingest transaction while holding a row lock on the run
(`SELECT … FOR UPDATE`). Writers to one run are serialized, so if a point with `seq = n` is
visible, every `seq < n` is visible too. A global auto-increment id would not give that: two
transactions can commit out of id order and a reader that already moved its cursor past the gap
would silently skip the late one.

Points are **immutable**: there is no update or delete. So the log is append-only and the page
state is a pure function of the set of points received.

## 2. The same batch is sent again

Idempotency is **per point, not per batch**: the identity of a point is `(run_id, key, step)`
(unique constraint in the database). A retrying client may re-batch, so a batch id would not be enough.

For each point in a batch the server answers one of three things, and reports the counts:
`{"accepted": 7, "duplicates": 3, "conflicts": 0, "last_seq": 4211}`.

- **accepted** — new, stored, gets the next `seq`.
- **duplicate** — same `(key, step)` and same value already stored: ignored. No new `seq`, nothing re-broadcast.
- **conflict** — same `(key, step)` but a *different* value: first write wins, the new value is dropped,
  and the run's `conflicts` counter goes up so the loss is visible, not silent.

A batch is **atomic**: one transaction. One invalid point rejects the whole batch with 400 and nothing is written.

## 3. Catching up after a disconnect

`GET /runs/{id}/events` is an SSE stream. Each message carries `id: <seq of its last point>`.
The client reconnects with `Last-Event-ID: <last seq it applied>` and the server replays every
point with a larger `seq` **from the database**, then keeps tailing.

- There is **one code path** for first load, catch-up and live: "read points after my cursor".
  No in-memory replay buffer, so there is no retention window — offline for 30 seconds or 3 days is the same.
- There is **no per-client queue** on the server. A connection is only a cursor. The server reads
  the next page from the database only after the socket has drained, so a slow client cannot make
  memory grow; it just falls behind and catches up later. A client that does not drain for 30 s is disconnected.
- "New data" notifications inside the process are only a **hint to wake up**. Every stream also
  polls the database every 2 s, so a lost hint (or a second API instance) costs latency, never data.
- The client checks the stream itself: it applies a point only if `seq == cursor + 1`. A smaller
  `seq` is skipped, a larger one means a gap — it drops the connection and resumes from its cursor.
- Heartbeat comment every 5 s; if the browser sees nothing for 12 s it reconnects (dead connections that never send FIN).

The page shows its own state honestly: `live`, `catching up` or `reconnecting`, plus the `seq` it is synced to.

## 4. Project isolation

- An API key belongs to one project. Keys are 256-bit random, stored only as a SHA-256 hash, sent in
  the `Authorization` header (never in a URL, so never in access logs).
- **Every query that touches a run carries `project_id = <project of the key>`.** There is one
  function that turns `(project, run id)` into a run; all routes go through it, including the SSE route.
- A run from another project and a run that does not exist give the **same response: 404, same body**.
  A malformed id too. Run ids are random UUIDv4 (122 bits), so they cannot be enumerated.
- Wake-up hints are keyed by run id and carry no data; the data is always re-read with the project filter.
- All API responses are `Cache-Control: no-store`, no ETags.
- Key rotation revokes the old key in the same transaction that creates the new one. There is no
  auth cache. Open streams of the revoked key are closed, and each stream re-checks its key on every poll.
- At most 100 open streams per project, so one project cannot take all the connections.

## Known weak spots (before writing any code)

1. Isolation is enforced in application code, not by the database (no row-level security). A new
   endpoint that forgets the project filter would leak. Mitigation: one choke-point function + tests.
2. The first load replays the full history of a run. Fine for thousands of points, not for millions.
3. One API instance is assumed for low latency. Two instances stay correct (poll), but lag up to 2 s.
4. First-write-wins means a client that really wants to correct a value cannot.
