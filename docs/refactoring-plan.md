# Refactor plan: service order

Easiest handler first, hardest last. The rank follows what a request does: validation, status codes, blocking I/O, and whether the response stays open as a stream.

The move itself is decided in [adr-bottle-to-fastapi.md](adr-bottle-to-fastapi.md). Hours are in [report_migaration_cost.md](report_migaration_cost.md). That table sorts differently because it also counts tests and background threads.

## Order

| # | Service | What is hard in the handler |
| --- | --- | --- |
| 1 | monitoring | Nothing. Two GETs return a dict. |
| 2 | books | CRUD, hand-set status codes, one shared DB session closed inside `POST`. |
| 3 | blotter | One handler joins books, SQL, and the cache. The rest are reads. |
| 4 | trade-action | The 400 / 202 / 503 contract and a partial batch, before anything hits the queue. |
| 5 | trade-generation | The request calls other services, rolls config back, and starts a thread. |
| 6 | market-data | `GET /stream`: an infinite generator and a subscriber list. |
| 7 | pricing | The same stream, plus a snapshot, two locks, and a hand-built 404. |
| 8 | integration | The stream calls providers. Refresh holds a lock until the cycle finishes. |

## 1. monitoring-service

`GET /health` and `GET /status` return a dict. The request does no validation, no write, and no call to another service. The probe of six health checks lives in a thread, not in a route.

## 2. books-service

Plain CRUD. The thicker spots are `POST /books`, which accepts either an object or a list, and the hand-set 400, 409, and 500 responses.

One SQLAlchemy session is shared by the process, and `POST` closes it in `finally`. On Bottle, books serves one request at a time, so this stays hidden. FastAPI serves requests concurrently, and that session is the first thing that has to be split.

## 3. blotter-service

Trade reads are query filters and a 404. The thick handler is `GET /books/summary`: it calls books-service with a 2 s timeout, loads SQL, and merges that with the in-memory valuation cache.

`GET /trades/<id>/audit-logs` is empty (`pass`). A background thread reads the valuation stream. The route does not.

## 4. trade-action-service

The handlers are short. The contract is not. Validation has to stay 400 with `{"error": ...}`. FastAPI would turn that into 422 and a different body. Success is 202 before the row exists. A full queue is 503. A batch can accept a prefix, return `accepted_so_far`, and keep going after one item fails. Quote freshness is checked synchronously in the request, before anything is enqueued.

## 5. trade-generation-service

The request does the work. `generate-once`, `generate-trade`, and `generate-batch` send an intention to trade-action from the handler. `PUT /config` changes local state, calls trade-action, and rolls the change back on failure, returning 502. `POST /start` and `POST /stop` own a lock and a thread. "Already running" is 400. Errors go in `message`.

These can stay ordinary synchronous functions. Each route has its own path and its own failure.

## 6. market-data-service

`GET /health`, `/symbols`, and `/snapshot` copy state under a lock.

`GET /stream` is a different kind of handler: an infinite generator, a blocking `queue.Queue.get()`, a subscriber added on connect, and that subscriber removed in `finally` on disconnect. A synchronous generator on the event loop stalls the process. The `async` version still has to drop the client while `get()` is blocked.

## 7. pricing-service

The same kind of stream, with a denser generator. `GET /valuation-stream` pushes a full snapshot first, then `event: valuation_update` frames. A serialization error on one valuation must not kill the stream. Two locks (`pricing_lock` and `sse_subscribers_lock`) must not be held across an `await`. The 404 is a hand-built `HTTPResponse` with its own JSON, not `response.status`. Six background threads write the same dicts.

## 8. market-data-service-integration

The stream does not fan out a frame that is already on a queue. `iter_quote_events` calls providers and `time.sleep`s on every lap, once per client. `GET` and `POST` on `/market-data/refresh` and `/curves/refresh` run the same cycle as the poller and do not return until that cycle finishes.

Errors are built as Bottle `HTTPResponse` in `core/helpers.py`, so the framework sits outside the handler. `GET /providers/<provider>/health` is a live probe inside the request.

Symbol lists and snapshots are as simple as monitoring. The stream and the refresh routes are what put this service last.

## 9. Shared DB manager implementation

Handling I/O asynchronously requires async-compatible libraries, which means the shared DB layer has to be rewritten to support them. Otherwise we will block the event-loop.
