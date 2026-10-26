# Risk matrix: migrating from Bottle to FastAPI

P = probability, W = impact. Scale: **L** low, **M** medium, **H** high.

Decision-criteria rule **R**: GO requires every **H/H** risk to have a mitigation whose cost is already in the migration estimate. That is true of R1 and R4 (and any later H/H items). Others either have lower P or W, or their mitigation is a measurement / decision rule rather than migration hours.

**Test coverage** did not get a separate ID: it is **R3** (5 of 8 services have no tests). At the top of the list below are two risks that were not in the matrix before: false safety of staying in the same language (R13), and libraries that do not fit FastAPI/ASGI beyond version pins alone (R14; R9 = version conflicts and two interpreters).

## Summary


| ID  | Risk                                                                                      | P | W |
| --- | ----------------------------------------------------------------------------------------- | - | - |
| R13 | False safety: “it is still Python, so the migration is safe”                              | M | H |
| R14 | Libraries / drivers do not fit FastAPI (ASGI, async, Pydantic)                            | M | H |
| R1  | A blocking call inside `async def` stalls the event loop                                  | H | H |
| R2  | Silent HTTP contract change                                                               | H | M |
| R3  | Missing / insufficient tests → regression on migration (5 of 8 services)                  | M | H |
| R4  | Background threads do not start under uvicorn, or state doubles with `--workers > 1`      | H | H |
| R5  | A badly designed benchmark drives a bad decision                                          | M | H |
| R6  | Underestimated cost because logic is tangled with Bottle and the shared package           | M | M |
| R7  | Different server: signals, logs, behaviour under overload                                 | M | M |
| R8  | Default pool of 40 threads for `def` endpoints                                            | M | M |
| R9  | Version conflicts (Pydantic, httpx, anyio) and two interpreters                           | M | M |
| R10 | Little asyncio experience: shared state, missing `await`, missing timeouts                | M | M |
| R11 | Time pressure leaves a mixed Bottle + FastAPI system                                      | M | H |
| R12 | Postgres pool and the second `AuditLogger` connection                                     | M | H |


In the starter matrix R9 had impact L. Here it is M: one mounted `trading_shared` and the integration image on Python 3.10 while the others use 3.11 mean a bad pin breaks more than one service.

---

## R13. False safety of the same language — M / H

Bottle → FastAPI stays in Python. The team is more likely to treat it as “switching a framework” than as rewriting the system (e.g. to .NET), where a new runtime, new failure modes, and full contract tests are obvious from day one. That impression **lowers caution**: R1 (blocking I/O in `async def`), R4 (thread startup), R2 (422 instead of 400), and R3 (no tests) get skipped because “the code looks the same”.

Here the illusion is concrete: the same `app.py` files, the same domain models, the same `trading_shared` — so the temptation to “just change decorators and the server” is high, while in-memory state, SSE on `queue.Queue`, and synchronous `urlopen` do not transfer 1:1.

**Mitigation.** Treat the migration as a change of concurrency model and HTTP contract, not as renaming imports. Before the first service: checklist R1–R4 plus contract tests on Bottle (cost already in the estimate). In review ask outright: “what would we do differently if this were another language?” — if the answer is “full tests and a separate startup plan”, the same applies here. Pilot on books with a hard 15 h limit (criteria) before falling back to NO-GO.

**Warning signal.** Estimate or review says “it is only the framework”. A service enters migration without contract tests or with threads still in `__main__`. After start “`/health` works”, but background / SSE / error codes differ from the recording.

## R14. Libraries that do not fit FastAPI / ASGI — M / H

This is not the same as R9 (bad pin of the same library on two Pythons). Here a library **does not fit the new runtime at all**: ASGI, `async def`, Pydantic v2, lifespan — or it is sync-only and pulls the team into R1.

Our stack is synchronous: SQLAlchemy + psycopg without async, `urllib.request` / no httpx, no Pydantic in the services, Bottle with no ASGI middleware. After migration we must either swap the driver/client (`create_async_engine`, `httpx.AsyncClient`) or deliberately keep `def` and the thread pool. Some patterns (Bottle’s global `response`, hooks, any Bottle plugins) have no 1:1 in FastAPI. A transitive dependency can also pull Pydantic v1 next to v2 (touches R9).

**Mitigation.** Before migrating a service, list I/O dependencies (DB, HTTP, queues) and choose: an async replacement with a timeout **or** a `def` endpoint with a comment. Do not pull libraries “because the FastAPI tutorial uses them”. Shared `trading_shared` first gets a clear async path or stays sync called from `def` (16–24 h in the estimate). Spike on integration (most I/O) before that whole service enters a sprint.

**Warning signal.** Import or runtime error on the first `async` endpoint (no async driver, wrong URL dialect). p99 jumps after pasting `requests` / `urlopen` into `async def` (then also R1). Two major Pydantic versions in the dependency tree.

## R1. Blocking call in `async def` — H / H

One synchronous I/O inside `async def` (`urlopen`, `requests`, `time.sleep`, sync DB driver, `queue.Queue.get()`) occupies the process’s only event loop. Every other request, including `/health`, stalls.

We have no async calls at all. The shared DB path is sync SQLAlchemy and psycopg 3 (`postgresql+psycopg://`), used from handlers and background threads. There are 11 inter-service `urllib.request.urlopen` calls. Four SSE endpoints wait on `queue.Queue.get()`: pricing `GET /valuation-stream`, market-data `GET /stream`, integration `GET /market-data/stream` and `GET /market-data/stream/<provider>`.

Worst case: `PollWorkers.run_quote_cycle` in integration calls `fetch_quote` → `urlopen` **with no timeout**. Today that hangs one poll thread. Moved into `async def` it hangs the whole service. The same applies to `pricing_worker` (blocking SSE read) and blotter `valuation_worker` (SSE, 30 s timeout).

**Mitigation.** Rule: only async I/O in `async def`. Sync DB and CPU stay in `def` (thread pool) or move to async SQLAlchemy. Ruff `ASYNC` rules. After each service, a load test with light `/health` beside a deliberate blocking `/cpu-async` (safety test in the criteria). A timeout on every `urlopen`, including the integration poll, before that code enters the loop.

Cost is in the estimate: async `trading_shared` 16–24 h, 3 h per each of 4 SSE endpoints, 1 h per service called from a handler (`httpx.AsyncClient` with timeout).

**Warning signal.** p99 jumps while CPU is idle. `/health` waits though the process barely computes. In integration the poll cycle never finishes and `_quote_cycle_lock` holds the next lap.

## R2. Silent contract change — H / M

FastAPI returns different defaults than Bottle, and the client (another service or the frontend, 54 routes) gets no exception at the sender — it gets another status or JSON and reacts on its own.

Differences that change behaviour here:

- Domain errors today are `{"error": "..."}` with 400, 404, 409, 500, 502, 503. FastAPI validation returns **422** and `{"detail": [...]}`.
- The shape is not uniform. trade-generation uses `{"message": "Generator started successfully."}` and `{"message": "No data available."}`. trade-action on a full queue adds a field: `{"error": "System overloaded, queue is full", "accepted_so_far": <n>}`. The error handler must keep both families, not only `error`.
- An uncaught DB error on some blotter routes falls into Bottle’s default **HTML 500**. FastAPI will make JSON. Same class of “HTML vs JSON” change, the other way, and it still breaks a client that inspects body or `Content-Type`.
- Trailing slash: FastAPI answers **307**. Our paths have none (`/valuations/<trade_id>`, `/books`, `/trade-actions`). A client that appends `/` gets 404 today and a redirect after migration.
- Path parameters are strings. The valuation key is `str(trade_id)`. Declaring `int` in FastAPI rejects an id Bottle accepted, or strips leading zeros.
- A bad `asset_class` in integration is **400**. A Pydantic model without a custom handler turns that into 422.

**Mitigation.** Before migrating a service, record contract tests on Bottle: status, JSON keys (`error` / `message` / `accepted_so_far`), and no 307 for inventory paths. One exception handler restores 400 instead of 422 and the existing body shape. Paths stay strings until a test shows the client sends a number.

Contract-test cost is in the estimate (0.75 h per route in a service without tests, 0.1 h to port an existing test).

**Warning signal.** Another service gets a code or format that was not in the recording. Monitoring shows `DOWN` or rising `errors_5m`, and the server log shows 422 or 307.

## R3. Missing test coverage → regression on migration — M / H

This is the “not every service has tests” risk — no separate ID beside it.

Five of eight services have no tests: monitoring, books, integration, trade-generation, trade-action. There are 46 unit tests for pricing (20), blotter (12), and market-data (14), against a mocked DB, in-process WSGI. There is no integration test against live Postgres and a live stream.

The costliest gap is **trade-action**: the only place that writes `trades`. OPEN does `INSERT ... ON CONFLICT DO NOTHING`; CLOSE takes `SELECT ... FOR NO KEY UPDATE` and holds the row lock until `commit`. A regression here loses or duplicates trades; blotter and pricing only read them. The generator retries the intent 3 times and swallows errors in the background loop — a bad trade-action response looks like a “slower generator”, not an exception.

Separately: pricing keeps at most 50 newest `ACTIVE` rows in `active_trades_cache`, because reload calls `get_trades` with default `limit=50`. That is existing behaviour. A migration that “while we are here” switches to the full set changes the business result without changing the framework. A contract test must freeze what is, or the change must be an explicit decision.

**Mitigation.** Bar: a service does not enter migration until every inventory route has a contract test on Bottle. Then service by service, not all eight at once. At the end one stack run (generator → trade-action → pricing → blotter) on live compose. Cost of missing-service tests is in the estimate.

**Warning signal.** A service falls below that bar and is still rewritten. Or after migration trade-action `rejected` / `duplicates` / `errors` on `GET /status` rise under the same generator.

## R4. Background threads and process count — H / H

Not in the starter matrix. It is how these services actually boot.

Background threads start in `if __name__ == "__main__"`, before `app.run`. Under uvicorn that block never runs: the process answers HTTP, but pricing does not mark, market-data does not publish ticks, blotter does not pull valuations, monitoring does not probe, integration does not poll, trade-action does not drain the queue. The generator is only a partial exception — its thread starts on `POST /start`, not on import, and that endpoint must survive the server change too.

Seven services hold process memory state (valuations, trade cache, trade-action queue, snapshot, flags, SSE subscribers). `--workers > 1` yields two such states and two consumers of the same queue. Trade writes cease to be a single consumer.

Thread counts: pricing 6, integration 5, market-data 3, blotter 1, monitoring 1, trade-action 1, generator 1 after `POST /start`, books 0. All `daemon=True`.

**Mitigation.** Start and stop in `lifespan`, N = 1 in compose and in the benchmark. Cost 1 h per service is in the estimate.

**Warning signal.** After start `/health` is UP but counters stay flat: `received_events = 0`, `is_running = false` where the generator should have started itself, empty valuation queue, no new `trades` rows. Two workers show up as duplicated audit rows and two independent `/status` views.

## R5. Bad benchmark — M / H

A bad measurement looks like a fact and shifts the GO / NO-GO decision. In this system it is easy to measure something other than what constrains us.

Facts that break a naïve benchmark, already closed by the criteria methodology:

- **7 of 8 services hold in-memory state.** More than one worker process is a different system, not a faster same one. Baseline is N = 1.
- Comparing against single-threaded Bottle (variant A) only measures `books-service`. The rest already run a thread per request. The decision compares A′ (`gthread`) to FastAPI. Moving books to FastAPI alone looks like a big win; one line `server=ThreadedServer` fixes it.
- The largest load is not HTTP. On every tick pricing recalculates trades from the cache (50 newest active, R3), today ~500 valuations/s at a 100 ms tick, and pushes them to SSE and a DB batch every 0.2 s. After removing the limit of 50 at 10 000 trades that would be ~100 000 valuations/s. An HTTP-only benchmark says nothing about that stream.
- `urllib.request.urlopen` opens a connection per call, and `wsgiref` handles one request per connection. Keep-alive in the load generator gives gunicorn and uvicorn an advantage this system does not have.
- On macOS container ports go through Docker Desktop’s proxy. At thousands of req/s the bottleneck may be the proxy, not the server.
- A load generator with no time limit waits forever; without `-w` it counts in-flight requests aborted at the deadline as errors.

**Mitigation.** Measure as written in the criteria: N = 1, A′ as baseline, `oha` on the same compose network, `--disable-keepalive`, 2 s timeout, interleaved repeats. A difference counts only when min–max ranges do not overlap. A second person reads the methodology before the run. An HTTP result is not an argument about the valuation stream.

**Warning signal.** Results jump between repeats. One variant looks “too good” only with keep-alive, many workers, or books compared to variant A. Postgres or generator CPU exceeds 80%, so we are not measuring the server.

## R6. Underestimated entanglement cost — M / M

The “0.5 h per route” rate assumes a handler can move on its own. Here some code cannot.

- Shared package `shared/trading_shared` is mounted into every container (`./shared:/app/shared`), not baked into the image. Sessions, repositories, and `AuditLogger` are synchronous. An async version of that package is 16–24 h and a precondition for every service that touches the DB.
- Integration: 2 files import Bottle outside handlers. That is the only entanglement line-item in the estimate (2 h per file) and the largest single service (29–44 h, 16 routes, 2 SSE).
- Domain and framework also meet in threads: integration handler `GET /market-data/refresh` takes the same `_quote_cycle_lock` as the background poll and does not return until the cycle finishes. Moving only the route without the poll cycle leaves two ways into the same code.

Estimate midpoint ≈ 125 h, acceptance bar 140 h. Pilot is books-service, estimate 10 h.

**Mitigation.** Inventory is done. Pilot on books. If the pilot exceeds 15 h (1.5×), stop and go NO-GO, because the rolled-up cost then exceeds 180 h. Integration stays last, after the shared package, not first “because it has the most I/O”.

**Warning signal.** Pilot takes more than 1.5× the estimate. Or the first service beyond books needs `trading_shared` changes that are not in that service’s own estimate row.

## R7. Runtime environment change — M / M

Today the process is `python app.py` and `wsgiref` with `ThreadingMixIn` (`custom_server.ThreadedServer`): a daemon thread per request and per open SSE, no cap, no Gunicorn, no uvicorn. books does not use that adapter — one thread. Images are `python:3.11-slim`, integration `python:3.10-slim`, `CMD ["python", "app.py"]`.

Uvicorn handles SIGTERM, access logs, and connection limits differently. Threads are `daemon=True`, so killing the process drops them mid-`commit`. For trade-action that leaves an open transaction and a `trades` row lock. Under overload the only explicit signal in code is trade-action **503** when `queue.Queue(maxsize=100)` is full. Other services have no cap — threads grow without limit. Uvicorn with a connection limit will refuse where `wsgiref` still accepted.

**Mitigation.** Dockerfile and compose are a separate estimate line (4–6 h). On each service: start, SIGTERM, and restart drill — `lifespan` threads finish the current `commit` and exit. Log format stays `shared.trading_shared.logging_config`. trade-action 503 stays application behaviour, not a code glued on by the server.

**Warning signal.** Container ends with SIGKILL after the grace period. A hole in the logs at start or shutdown. Under load the client gets connection refused instead of 503 with `accepted_so_far`.

## R8. Pool of 40 threads for `def` — M / M

A plain `def` endpoint under uvicorn goes to the anyio pool, default **40** threads. Our `ThreadedServer` has no ceiling: every SSE and every request gets its own thread.

Four SSE endpoints hold a thread for the whole connection lifetime if left as `def`. Today subscribers are few (blotter and one browser on the valuation stream; market-data counts `active_subscribers` in the handler). Near 40 concurrent streams per process the pool is full and ordinary `GET /health` on that process stalls while CPU is idle. The criteria say to revisit when stream count exceeds half the thread limit.

The flip side: books today has exactly one thread. Moving it onto a pool of 40 increases capacity. That is not an argument for FastAPI (see R5), but that service must not be used as proof that the pool of 40 is tight.

**Mitigation.** SSE and other long I/O as `async def` (or a deliberately larger custom pool). Short `def` only where work is CPU or sync DB and returns quickly. Record the limit in config; do not leave the default by accident.

**Warning signal.** Throughput plateaus at idle CPU with about 40 parallel connections. `/health` waits, and process threads sit in the pool, not in the interpreter.

## R9. Dependency versions — M / M

In the starter matrix impact was low, assuming one lockfile. Here there are eight images and no FastAPI deps yet: each `requirements.txt` is `bottle`, `SQLAlchemy`, `psycopg[binary]`.

What raises impact to medium:

- Integration is built on **Python 3.10**, the other seven on **3.11**. A Pydantic v2 / anyio / httpx pin must land on both interpreters or integration gets a separate set and the split is immediate.
- `trading_shared` is one package, volume-mounted. Two copies (sync for Bottle, async for FastAPI) do not fit that mount. A mixed migration (criteria option 3: books, blotter, trade-generation, monitoring) either leaves the shared package sync and calls it from `def`, or splits the package. Splitting without an estimate line breaks services still on Bottle.
- Pydantic v1 and v2 do not coexist in one process. We have no v1 today, so the risk is pulling v1 transitively via an old dependency more than migrating from v1.

**Mitigation.** One lockfile for the stack; the same `fastapi`, `pydantic`, `httpx`, `anyio` versions on every image that gets them. Integration raised to 3.11 together with the first addition of those libraries, not “later somehow”. Services left on Bottle do not import Pydantic v2 from the shared package while the package has a single import path.

**Warning signal.** `pip freeze` differs across services on pydantic / httpx / anyio. The integration image fails to build on a pin that works on 3.11. A Bottle service dies on import after a change under `shared/`.

## R10. Asyncio: state, `await`, timeouts — M / M

Seven services join background threads to HTTP via locks and `queue.Queue`. That is the model the team knows. There is no asyncio in the code.

Places where a thread–loop bridge bug is concrete, not theoretical:

- pricing and market-data copy dicts **under a lock** in the handler. A lock taken in a thread and awaited on the loop (or the reverse) without `run_in_executor` deadlocks the process: CPU near zero, request hangs.
- trade-action: `queue.Queue(maxsize=100)`, one `trade_action_worker`, `put(..., block=False)` and 503 when full. The provider-mode flag is in process memory, also under a lock. Switching to `asyncio.Queue` while the worker stays a thread needs a bridge; without it 503 disappears or `put` blocks the loop.
- integration: `_quote_cycle_lock`, unbounded `db_queue`, bounded curve queue (1000, overflow is dropped, not blocking). Two different overflow behaviours. One “shared async queue” erases the difference and either drops curves differently than today or starts blocking the poll.
- generator: `ThreadPoolExecutor` with three threads for parallel books / symbols / snapshot fetch. `asyncio.gather` without a per-call timeout waits on the slowest of three, and books still answers on one thread.
- Timeouts are already leaky, so they are easy to lose when rewriting: integration poll with no timeout, blotter → books 2 s, pricing → integration 5 s, generator → trade-action 5 s and 3 attempts, blotter SSE 30 s, curves 15 s, aggregator `PROVIDER_FETCH_TIMEOUT_SECONDS` (compose 1 s, code default 10 s) only on the stream, not on the poll.

**Mitigation.** Review checklist: who holds the lock, who `await`s, what timeout each I/O has, which queue may drop vs return 503. Shared state stays with threads or moves to loop primitives — not a mix on one object.

**Warning signal.** Warning `coroutine was never awaited`. A request hangs with no log line and no CPU growth. The trade-action or valuation queue stalls while `/health` stays UP.

## R11. Half-finished refactor — M / H

Mid-migration means two frameworks in one stack. Some clients call Bottle, some FastAPI, and `trading_shared` is one. Time pressure freezes that state because each next service “already works somehow”.

Criteria option 3 is a **deliberate** partial migration: books, blotter, trade-generation, monitoring (≈ 40–65 h). Pricing, market-data, trade-action, and integration stay on Bottle. That is not a failure if it follows the decision rule (T1 yes, T2 no, or T2 yes and K no). Failure is a quiet mix inside one service: some routes on FastAPI, threads still from `__main__`, DB sometimes sync sometimes async.

Estimate order, smallest first: monitoring (3–4 h), market-data (6–9 h), pricing (7–11 h), blotter (7–10 h), trade-action (7–10 h), books (8–12 h), generator (14–21 h), integration (29–44 h). The decision pilot is books not because it is shortest, but because it is single-threaded and has no background state — the pilot result can transfer to the rest.

**Mitigation.** The order above or explicit option 3, nothing in between. Service done means: inventory routes, green contract tests, threads in `lifespan`, one framework in the process. Stop condition is written: books pilot over 15 h → NO-GO; otherwise the criteria decision rule, not “we keep going because we started”.

**Warning signal.** At half the budget (≈ 60 h) fewer than four services are migrated, or one service has both Bottle and FastAPI in `requirements.txt`. Or `trading_shared` exports both sessions and compose does not say which service uses which.

## R12. Postgres pool and `AuditLogger` — M / H

Not in the starter matrix. The shared database is one critical zone for eight processes.

`create_engine` is called without `pool_size`. SQLAlchemy defaults to 5 connections + 10 overflow per process. Eight processes × 15 = **120**, while Postgres 15 has `max_connections = 100`. On top of that `AuditLogger.write` **always** opens its own `DBSessionManager` and commits on its own. The `db` argument is ignored, so an audit write is a second connection and a second transaction, often while the caller’s session is still open. That happens on every curve tick (write before SSE), on valuation flush every 0.2 s, on trade-action transactions, on quote inserts, and on `GET /snapshot`, which writes audit before returning.

Async SQLAlchemy does not erase that arithmetic. The default async pool is also 5 + 10, and a connection held across `await` lives longer than one released after a sync `commit`. A second audit checkout during a request (trade-action already does this: the HTTP thread holds a connection for quote freshness, the worker holds another) hits the limit faster. The pool waits up to 30 s for a free connection; our clients give up after 2 s, so a missing connection shows up as a timeout, not a pool error.

**Mitigation.** Before any service moves to an async engine: explicit `pool_size` and `pool_timeout` shorter than 2 s, and an `AuditLogger` that writes on the caller’s session or on a connection from the same pool without adding overflow. The logger change sits in the async `trading_shared` cost (16–24 h). Pool sizing is also in criteria option 2 (Bottle optimisations, 12–20 h) and must be set even if migration gets GO — otherwise the new pool inherits the same negative headroom. S4 in the benchmark passes only when FastAPI does not throw pool errors.

**Warning signal.** Postgres log `too many connections`, or application timeouts while DB CPU stays below the bar. Parallel sessions from one process rise above 15. `GET /snapshot` or valuation flush lengthens while `EXPLAIN` on the queries stays short.

---

## What this matrix does not treat as a migration risk

These are inventory facts easy to drag into the matrix that do not depend on the framework. They stay in option 2 or as known state, not as a GO argument.

- Pricing and generator caches see at most 50 active trades (repository default `limit`). Fixing that changes valuation results and CLOSE selection, with or without FastAPI.
- Pricing CPU. Today, with the limit of 50, ~500 valuations/s, about 2.5% of one CPU. After removing the limit at 10 000 trades you would need ~100 000 valuations/s versus ~20 000 estimated on one CPU. Work runs in background threads; async does not speed compute, so changing the HTTP server does not move it.
- Single-threaded books slows blotter, generator, and the monitoring probe. The fix is `ThreadedServer`, not migration.
- Provider quotas (Finnhub, Twelve Data, Alpha Vantage) constrain integration more than the HTTP server.
