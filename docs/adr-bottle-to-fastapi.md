# ADR-001: Move all services from Bottle (WSGI) to FastAPI (ASGI)

**Status:** Accepted  
**Date:** 19 October 2026  
**Deciders:** engineering  
**Related:** [microservice-inventory.md](microservice-inventory.md), [decision_criteria.md](decision_criteria.md), [report.md](report.md), [report_migaration_cost.md](report_migaration_cost.md)

## Context

We run eight Python services on Bottle, one process each. Seven of them give every HTTP request and every open SSE tab its own thread. Books is the exception: one thread, one request at a time.

The production target is 700–1200 traders. A trader usually leaves two live tabs open (market data and valuations), sometimes three. That is 700–1200 streams on a single service, and 1400–3600 across the desk. The question was whether the current model survives that, and whether a move to FastAPI is worth it as the system grows.

We did not benchmark the eight production handlers. Each mixes streams, the database, and compute, so a slow result would not say which part failed. The measurement used a small sample with one route per kind of work, Bottle with a 200-thread pool (A′) against FastAPI async (B). Full write-up: [report.md](report.md).

## Decision

**GO — full refactor.** All eight services move to FastAPI. We do not keep a Bottle island and a FastAPI island.

The live-tab test is the reason. Short waits on other services and database writes miss the 1200 req/s bar at 700 clients. That does not shrink the decision. It tells us how to write after the move: the event loop gets a cap, the database stays on today’s sync SQLAlchemy, and blocking work stays out of `async def`.

Installing FastAPI and keeping ordinary `def` handlers is not this decision. That variant held 72–295 of 700 tabs.

## Evidence

Six repeats of 60 seconds, one process. Bottle = Gunicorn, 200 threads. FastAPI = one event loop. Ranges are min–max of those repeats.

| What we measured | Bottle (200 threads) | FastAPI async |
| --- | --- | --- |
| 700 open streams | Holds 200, drops 71%. Alive-check fails 14–43% of the time. Memory 129–146 MiB | Holds all 700. Alive-check instant, no failures. Memory 108–118 MiB |
| 1,200 open streams | Still 200, drops 83% | Holds all 1,200. Alive-check still instant. Memory 111–122 MiB |
| Database writes at 700 clients | ~500/s, 4–7% fail, tail ~2 s | ~960–1,050/s, 7–8% fail, tail ~2 s. Faster, still under 1,200/s |
| Wait 50 ms on another service, 700 clients | 200–350/s, 32–60% time out | 0 replies. Every attempt times out. The other service is almost idle |
| Heavy hash / empty alive-check | FastAPI is not slower | Does not veto the move |

At 200 tabs — Bottle’s own pool ceiling — it holds every stream and the alive-check fails completely. Every thread is sitting on a tab.

Cost of the full move, one person who already knows these services: **150–220 hours**. Shared pattern (uvicorn, `lifespan`, today’s JSON errors, not 422) is 8–12 h once. Rewriting `trading_shared` to async is 16–24 h extra and is **out of scope**: the database test did not ask for it.

## Options

**1. Full move (chosen)**  
All eight services on FastAPI, one request model, one way to start background threads.

- *For:* the only result that scales with trader count; one stack to hire for and to run; Pydantic and a larger ecosystem come with the same rewrite.
- *Against:* largest bill (150–220 h); the team has to learn that `await` is the only thing that frees the loop; uvicorn does not run `if __name__ == "__main__"`, so today’s background threads will silently not start if we copy-paste.
- *Cost:* 150–220 h, plus whatever we blow if the first service overruns.

**2. Partial move**  
Only market-data (`GET /stream`) and pricing (`GET /valuation-stream`). The rest stays on Bottle. About 50–70 h.

- *For:* cheapest path to the measured win.
- *Against:* two frameworks, two start commands, two error bodies. Every new person learns both. The next stream or WebSocket repeats the argument. Mixed-framework was rejected for that reason, not because the short-request tests passed.

**3. Stay on Bottle and tune it**  
Thread the books server (1–2 h), index pricing by symbol, one SSE frame per tick, raise or remove a 200-thread cap.

- *For:* no framework change.
- *Against:* one tab still costs one thread for the whole time it is open. A larger pool is more RAM, not more cores. At 700–1200 tabs the model still pays a worker per headset. FastAPI does not make marking faster either — that work is the same on both sides — but staying does not fix the thing we actually fail.

## Consequences

**We accept**

- One process per service stays. A second worker would get its own memory and its own queue. Seven of eight services hold state in process.
- Streams are `async`. Database commits, `urlopen`, `time.sleep`, `queue.Queue.get()`, and tight CPU stay in ordinary functions or in the existing background threads.
- How many calls to another service may sit on the loop at once is capped. At 10 clients that wait is still clean; at 50 the loop is already full; at 700 FastAPI answers nothing. That traffic does not exist today. The cap is so it cannot appear by accident.
- JSON errors stay as they are (`error` / `message` / `accepted_so_far`). FastAPI’s default 422 is a contract break.
- Background threads move into `lifespan`. `/health` can be green while the worker never started. That is a test, not a comment.
- Books’ closed session and the 50-trade pricing cache are fixed in this refactor or left as explicit follow-ups. FastAPI does not fix them by existing.
- New people need FastAPI. The hiring pool is larger, not smaller. The new skill is “do not block the loop”, not the framework name.

**We give up**

- A quiet mix: some routes on FastAPI, threads still from `__main__`, database sometimes sync and sometimes async.
- Using this move to make PnL faster. The GIL stays. Valuation on every tick stays a core problem on either server.

**Risk we already priced**

The bill can exceed the table. Stop rule: market-data is first (it teaches the stream). If it crosses about 36 h (1.5× the top of 16–24 h), the rest of the stack will be dearer than the table and we stop the move. Same rule as the books pilot (10 h, stop at 15 h), applied to the service that actually justifies the work.

## When we reopen this

The decision holds while the world still looks like the measurement: one process, 700–1200 traders, two or three tabs, a light SSE frame, sync database, no burst of 700 calls to a neighbour in the same second.

Reopen it when one of these is true:

1. **The connection is no longer “one quiet tab”.** WebSocket, push, a new fan-out, or many frames per tick. Re-run the open-tab test (T1), not a short HTTP test.
2. **The loop is stuck in production.** `/health` on a streaming service has p99 above 2 s or starts failing, while the process is idle or pinned to one core. That is either a blocked `async def` or a new load the sample did not have.
3. **The scale assumption dies.** The target is no longer 700–1200 traders, or we need a second worker for CPU. N = 2 is a different system.
4. **A neighbour wait becomes real traffic.** If we start calling another service at tens of clients in the same second, S2 applies: cap first, then measure again.
5. **The first service overruns.** Market-data over ~36 h → stop and write a new decision, do not “keep going because we started”.
6. **FastAPI cannot clear a problem we thought it would.** Provider quotas, a second process “just in case”, or marking every open position. Those are not framework bugs. Treat them as new work, not as a silent undo of GO.

Do not reopen this because FastAPI has `/docs`, because the database write was twice as fast, or because someone prefers Bottle. Those facts were already on the table.
