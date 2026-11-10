# Note

**Result:** 23 of 23 tests pass, twice in a row on a fresh database. **Verdict: SHIP** for 50 runs × 10 events/s,
with one condition (section 5). Every number below is in [test-output/](test-output/), including the runs that failed.

## 1. Does the page always match what is stored?

One test (`01-consistency`) drives a real browser on the run page while a sender pushes 1200 points through
a faulty network (toxiproxy): shuffled order, 10% held back as late, batches sent twice, requests abandoned
mid-flight, 12 connection resets, and the browser cut off twice — once by a TCP reset (it stayed at seq 466
while 255 more points were stored), once by a silent blackhole that only the client's watchdog can detect.

- **At the end, four views are identical row for row:** what was sent = database = `GET /metrics` = the table in the page DOM.
- **While it runs:** ~135 samples of (page, then database). The page was never ahead of the database, never
  went backwards, and its point count always equalled its `seq`. So at every moment it showed a true past state.
- **Can this test fail?** Yes. With the server changed to resume "from now" instead of from the client's cursor, it fails (`mutation-check.txt`).

**What it would not catch:** (a) a wrong value shown *between* two samples and corrected later — only the final
state is compared exactly; (b) the chart drawing itself — I compare the data table and counters, which share
the SVG's in-memory series; (c) two API instances, a database failover, another browser, a second tab;
(d) runs over 5000 points, where the table is cut and the line thinned; (e) an HTTP proxy that buffers SSE
(toxiproxy is TCP-level); (f) a bug in the sender before the request is made; (g) other timings: one seed, and a pass is not a proof.

## 2. Does a project see only its own data?

| Attempt by project A on project B | Result |
|---|---|
| Read run, read metrics, **write** metrics, **subscribe** to the stream | 404, byte-identical (status, body, headers) to a run that does not exist; nothing written |
| 15 malformed / near-miss / injected ids, bad percent-encoding | the same 404; run ids are UUIDv4, not ordered |
| A's open stream while B writes 350 points | 25 points, all A's; B's metric name and value never in the bytes |
| Missing, malformed, unknown, **revoked** key on 4 routes | the same 401 |
| Cache | every response `no-store`, no ETag, conditional requests ignored |
| 100 open streams (A's limit) | A's 101st gets 429; B still connects |

**What else could leak.** *Errors:* 4xx bodies hold only `error`/`detail`, 5xx only a request id (checked on a
forced 503). *Logs:* a test scans them — no API keys, no metric names or values, no query strings. Run ids **do**
appear in logged paths; acceptable only because an id alone opens nothing. *Timing:* "not yours" vs "does not
exist" differ by tens of µs in the median of 400 pairs — noise, but a test cannot prove absence.
*Not covered:* performance — one project can still slow down the others (section 5).

## 3. Silent failures

| Failure | How I provoked it | Result |
|---|---|---|
| Events lost on reconnect | Reader cut 40 times at random moments during 6000 writes; 16 concurrent writers with 3 live readers; a commit made behind the server's back (no wake-up) | every `seq` exactly once, 0 gaps, 0 duplicates; the un-notified commit arrived by the 2 s poll |
| Duplicates | Same 200-point batch 8× concurrently, then re-batched with overlap | stored once: `accepted 200, duplicates 1400`. A *different* value for a stored point is refused and counted in `conflicts` |
| Slow client | Reader that stops reading while 42 000 points (~6 MB) are written | server-side buffer for that stream peaked at 67 KiB; cut off after 30 s; resumed from its cursor with 0 gaps |
| Partial writes | 12 invalid batches; and a transaction **killed between INSERT and COMMIT** with 500 of 1000 rows written (verified by probing for uncommitted rows) | 0 rows left, head unchanged, API answers 503, the retry stores all 1000 |
| Old key after rotation | Rotate with a stream open; revoke directly in the database; 6 rotations at once | REST 401 at once; open stream closed in ~20 ms (or ≤2.5 s when this process was not told); exactly one rotation wins |

Not checked: a crash of Postgres itself or a failover; a sender that overflows its own buffer while offline.

## 4. What failed on the way (kept, not edited)

- **Run 1, 3 failures.** Killing one database connection mid-transaction **crashed the whole API process**
  (`run-1-api-crash.log`): a checked-out `pg` client has no error listener. No data was lost — the invariant
  test at the end of that same run passed — but every open stream of every project was dropped and requests
  were refused until Docker restarted the process (about a second here). Fixed. Also: an over-long
  id got the framework's own 414 that echoed the path back. Fixed.
- **Run 2, 1 failure.** My guard "the sender really saw failures" fired: the resets had missed every request. The fault now stays until 12 requests have failed.
- **Run 3, passed, but wrong.** An evidence line said "611 rows written by the killed transaction" where at most 500 was possible (lagging table statistics). Replaced by an exact probe.

## 5. Verdict: SHIP, if clients batch

| Load (single API process, laptop, Docker) | Requests/s | Ack p99 | Sent→seen p99 | API CPU | Errors |
|---|---|---|---|---|---|
| **50 runs × 10 ev/s, one batch per second, 100 live viewers, 60 s** | 50 | 11 ms | 12 ms | 21% of a core | 0 |
| Same events, **one request per event** | 500 | 423 ms | 534 ms | 101% | 0 |
| 10× the runs (500 × 10 ev/s), batched | 500 | 356 ms | 513 ms | 101% | 0 |

At the asked load the process uses a fifth of one core, and every run was verified (stored count, every viewer gapless).
The limit is **requests, not events**: about 500 requests/s saturates the one Node process, because each ingest
costs 7 database round trips. It stays correct there, but with no margin.

**What to change first:** a per-project rate limit. Today one client that does not batch can saturate the process
for every project — a real hole in isolation that none of my tests cover. Second: do the ingest in one SQL
round trip. Third: a second instance, with `LISTEN/NOTIFY` instead of the 2 s poll.

## 6. AI tools

Claude Code (Claude Fable 5.2) wrote the first version of everything here in one session — design, server, UI,
tests, the draft of this note — ran the tests and made the fixes in section 4. Where its first answer was wrong:
the four items of section 4, each caught by a test or by reading the raw output instead of trusting the green
tick; and `FOR UPDATE` on the run row, which collides with the foreign-key lock (found while building the kill test).

**My part.** I did not write this code by hand. I asked the tool to explain the task before anything else, had it
write the design first, and then let it build and test. What I did myself: I ran the full suite on my own machine
and read the raw output, and I read the design, this note and the four core files (`repo.ts`, `stream.ts`,
`routes.ts`, `app.js`) until I could explain each decision. I accepted the tool's design as it is; the choices are
now mine to defend in the discussion.

## 7. What surprised me, what still concerns me

What surprised me most is that the failure that mattered was one nobody had designed for. Ordering and duplicates
held from the first run; what broke was a single dead database connection, which took down the whole process for
every project while all the data stayed correct. "Correct" and "available" turned
out to be separate properties, and the tests had only been looking at the first. What still concerns me: isolation
lives in application code, so one forgotten `project_id` in a new query is a leak and only the tests stand in the
way; and one project that does not batch can slow down all the others.
