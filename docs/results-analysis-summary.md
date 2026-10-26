# Benchmark result

**Move the live-stream services. Leave ordinary short requests on Bottle.**

That is a partial move, and it matches the rule in [decision_criteria.md](decision_criteria.md): the open-tab test passed, the short-request tests did not, and calculation and health checks do not argue against a move.

The run started 16 Oct 2026. Each point is six repeats of 60 seconds, one process. The comparison is Bottle with a pool of 200 threads (A′) against FastAPI async (B), at 700 clients. Ranges below are the low and high of those six repeats. Scored with `python3 analyze.py results`.

## T1 — open tabs — confirmed

This is the reason to move. Bottle gives each open tab its own thread and stops at 200. FastAPI keeps a light note per tab on one worker, so it held every tab and still answered “are you alive”.

| Open tabs | Bottle (200 threads) | FastAPI async |
| --- | --- | --- |
| 700 | 200 of 700 held. 71% dropped. Alive-check failed 14–43% of the time. Memory 129–146 MiB. | All 700 held. None dropped. Alive-check instant, no failures. Memory 108–118 MiB. |
| 1,200 | Still 200 held. 83% dropped. | All 1,200 held. None dropped. Alive-check still instant. Memory 111–122 MiB. |

At 200 tabs, Bottle’s own ceiling, it held all 200 and the alive-check failed completely: every thread was taken. FastAPI held all 200 and the check stayed instant.

FastAPI written as ordinary functions (variant C) held 72–295 of 700 tabs. Installing FastAPI without async waiting does not pass this test.

## T2 — waiting on another service — not confirmed

Both sides miss the bar of 1,200 successful requests a second, with 99 out of 100 finishing in under 2 seconds and at most 1% failing.

At 700 clients, each request waited 50 ms on another service. Bottle still answered 200–350 a second, with 31–60% timing out. FastAPI answered none: every request timed out. FastAPI was worse here, so short waits on other services stay on Bottle.

## T3 — database — not confirmed

Same bar, same result at 700 clients. Bottle did about 500 a second, just under 2 seconds, with 4–7% failing. FastAPI did about 960–1,050 a second, also just under 2 seconds, with 7–8% failing. Neither side threw a pool error (no HTTP 500s).

FastAPI did clear that bar at 50 and at 200 clients (about 1,200–1,350 a second, no failures). The thesis is scored at 700, where it misses, so database routes stay on Bottle.

## T4 — heavy calculation — does not block the move

At 700 clients every variant timed out, Bottle included. One process cannot hash that much for 700 people at once. At one client, Bottle and FastAPI both did about 170 a second, with almost all replies under 0.01 seconds. FastAPI is not clearly slower, so this test does not veto the move.

## T5 — “are you alive” — does not block the move

At 700 clients FastAPI did 14,900–16,200 a second, with the slowest 1% at 0.07–0.08 seconds. Bottle did 3,800–3,900 a second, at 0.23–0.26 seconds. During the open-tab test, FastAPI’s alive-check also stayed clean. FastAPI is faster on this check, so it is not a regression.

## One rule for the move

Blocking work inside an `async` function freezes FastAPI’s only worker. We forced that: 50 clients ran a calculation with no pause, and the alive-check timed out in 5 of 6 runs. Waiting that cannot pause (`requests`, `urlopen`, sync database, `time.sleep`, a tight loop) stays in an ordinary function. FastAPI then runs it on a side thread and the alive-check keeps working.

## What to migrate

The decision rule names three services. Hours are the ranges already in [risk_matrix.md](risk_matrix.md). They are the service rewrite, not a new quote from this run.

| Service | Why it is in the move | Estimate |
| --- | --- | --- |
| market-data | Serves `GET /stream`. This is the open-tab result. | 6–9 h |
| pricing | Serves `GET /valuation-stream`. Same result. The cache of 50 trades and fan-out stay a Bottle change first (T6): FastAPI does not add cores for marking every position. | 7–11 h |
| monitoring | Named with the stream services by the decision rule. It has no trader tabs of its own; it probes the others and serves a snapshot. | 3–4 h |

Those three rows add up to **16–24 h**. Leave `trading_shared` synchronous and call it from ordinary functions. Rewriting that package to async is a separate 16–24 h, and this result does not ask for it: the database test did not pass.

## What stays on Bottle

Blotter, books, trade-action, the generator, and integration. Short requests and database routes did not clear the bar at 700 clients. Integration also has two stream endpoints; it stays with this group because its rewrite is the large one (29–44 h in the same estimate) and the short-request tests did not support a wider move.

Books today handles one request at a time. After the first overload its single worker stayed busy, so its later zeros are a stuck worker, not a clean measurement. The comparison above uses Bottle with 200 threads.
