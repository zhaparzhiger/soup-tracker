# Raw test output

Nothing here is edited, with one exception that is marked inside the file: in `run-1-first-run-3-failures.txt`
a 5000-character probe string was shortened so the file stays readable.

| File | What it is |
|---|---|
| `run-1-first-run-3-failures.txt` | First full run: 20 passed, 3 failed. The API process crashed mid-suite. |
| `run-1-api-crash.log` | The crash from that run: unhandled `error` on a checked-out database client. |
| `run-2-one-failure.txt` | After the fixes: 22 passed, 1 failed — the test's own guard that the network faults really happened. |
| `run-3-passed-but-one-evidence-line-wrong.txt` | 23 passed, but the "611 rows" line in test 19 was wrong evidence (lagging table statistics). |
| `final-run-1.txt`, `final-run-2.txt` | Two consecutive runs of the final code on a fresh database: 23 passed, 23 passed. |
| `mutation-check.txt` | Five deliberate bugs in the server; every one is caught by the tests meant to catch it. |
| `load-1x-50runs-500eps.txt` | The asked load: 50 runs × 10 events/s, one batch per second, 100 live viewers, 60 s. |
| `load-1x-unbatched-500rps.txt` | Same events, one request per event (500 requests/s). |
| `load-1x-unbatched-no-viewers.txt` | Same, without viewers — to see how much of the cost is ingestion alone. |
| `load-10x-500runs-5000eps.txt` | Ten times the runs, batched (5000 events/s, 500 requests/s). |
| `experiment-fastify-default-coercion.txt` | Why input is validated by hand: Fastify's default schema validation stores `null` as `0`. |

Lines starting with `│` are evidence printed by the tests themselves (counts, timings, what was compared).
