# Microservice inventory

Scope: every application service in `backend/docker-compose.yml`.
Code state: 2026-10-14. Unit tests for pricing, blotter, and market-data were run on 2026-10-06.
Postgres, `db-migrations`, and `db-seed` are infrastructure, not application services.

Shared run model:

- one Python process, **Bottle** framework;
- the HTTP server is `wsgiref.simple_server` with `socketserver.ThreadingMixIn` (`custom_server.ThreadedServer`): a daemon thread per request and per open SSE stream, with no cap and no process pool (no Gunicorn / uvicorn). `books-service` **is the exception** — `app.run` without that adapter, so one `wsgiref` thread;
- background threads, where a service has any, start in `if __name__ == "__main__"` before `app.run`. `trade-generation-service` starts its worker only on `POST /start`;
- image: `python:3.11-slim`, except `market-data-service-integration` (`python:3.10-slim`). `CMD ["python", "app.py"]`;
- in Compose, `backend/shared` is mounted at `/app/shared` (the `shared.trading_shared` package is not copied into the image);
- database: PostgreSQL 15, driver **psycopg 3** via SQLAlchemy, URL `postgresql+psycopg://…` from `DATABASE_URL`;
- `create_engine` is called with no `pool_size`, so each process gets the SQLAlchemy default pool: 5 connections plus 10 overflow;
- every service waits for the `db-seed` container to finish.

Bottle serializes dict responses as `application/json`. `pricing-service`, `blotter-service`, and `market-data-service` are GET-only and have no request body. `books-service`, `trade-generation-service`, `trade-action-service`, and `market-data-service-integration` accept a JSON body on `POST` / `PUT`.

`AuditLogger.write` always opens its own `DBSessionManager` and commits. The `db` argument is ignored, so an audit line is a second connection and a second transaction on top of the caller's session.


| Service                         | Port | Responsibility in one sentence                                              | Load                       | Background threads            | Tests       |
| ------------------------------- | ---- | --------------------------------------------------------------------------- | -------------------------- | ----------------------------- | ----------- |
| market-data-service             | 8001 | Simulates ticks and publishes them over SSE and to Postgres.                | mixed, I/O-dominant        | 3                             | 14, passing |
| pricing-service                 | 8002 | Marks active trades and computes book alpha/beta.                           | mixed                      | 6                             | 20, passing |
| monitoring-service              | 8003 | Probes the other services and serves a status snapshot.                     | I/O-bound                  | 1                             | No tests    |
| books-service                   | 8004 | Stores the book catalogue the blotter and the generator read.               | I/O-bound, single-threaded | 0                             | No tests    |
| market-data-service-integration | 8005 | Polls external quote and curve providers and stores the results.            | I/O-bound, external API    | 5                             | No tests    |
| blotter-service                 | 8006 | Serves the trade list and book summary with the latest in-memory valuation. | I/O-bound                  | 1                             | 12, passing |
| trade-generation-service        | 8007 | Builds open/close intentions and posts them to trade-action.                | I/O-bound                  | 0 until `POST /start`, then 1 | No tests    |
| trade-action-service            | 8080 | Accepts open/close actions onto one queue and writes `trades`.              | I/O-bound, single consumer | 1                             | No tests    |


Shared critical zone: one Postgres. Curve commits (every market-data tick, before SSE), valuation flushes (every 0.2 s), trade-action transactions, provider-quote inserts, and `GET /snapshot` audit rows all land there. Eight processes times (5 + 10) is 120 connections at full overflow; Postgres 15 defaults to `max_connections = 100`. The audit connection is extra and can be checked out while the caller's session is still open.

Decision criteria for the benchmark that reads this inventory: [decision_criteria.md](decision_criteria.md).

---



## 1. pricing-service

**Responsibility.** Marks active trades (fair value and PnL) from the simulator stream, provider quotes, and yield curves, and keeps book alpha/beta against `MARKET_INDEX`.

### Endpoints

No query parameters and no request body. The path parameter is a string (the store key is `str(trade_id)`).

#### `GET /health` → 200

```json
{
  "service": "pricing-service",
  "status": "UP",
  "market_data_connection": "DISCONNECTED",
  "received_events": 0,
  "last_market_event_time": null,
  "last_pricing_time": null,
  "last_valuation_symbol": null,
  "last_valuation_asset_class": null
}
```

`market_data_connection`: `CONNECTED`, `DISCONNECTED`, or `RECONNECTING`.

#### `GET /valuations` → 200

A map `{ "<trade_id>": <valuation> }`, a copy of the in-memory `valuations_store`. The map is `{}` until something has been priced.

#### `GET /valuations/<trade_id>`

- **200** — the valuation object with `trade_id` added.
- **404** — `{"error": "Valuation not found for trade_id: <trade_id>"}`.

Fields shared by simulator-path valuations (equity, bond, FX, commodity, futures, option):


| Field                                                              | Type                                   |
| ------------------------------------------------------------------ | -------------------------------------- |
| `trade_id`, `book_id`, `asset_class`, `symbol`, `side`, `currency` | string                                 |
| `fair_value`, `market_value`, `current_price`, `entry_price`       | number                                 |
| `unrealized_pnl`, `realized_pnl`, `total_pnl`                      | number (`realized_pnl` is 0)           |
| `valuation_time`                                                   | string, UTC `YYYY-MM-DDTHH:MM:SS.mmmZ` |


A valuation from a provider quote adds: `market_data_provider`, `market_data_timestamp`, `is_stale` (bool), `market_data_age_seconds` (number or `null`).

An IRS valuation sets `fair_value` / `market_value` / `unrealized_pnl` / `total_pnl` to the swap PV, and adds `market_data_provider`, `market_data_timestamp`, and `pricing_details`:

`fixed_leg_pv`, `floating_leg_pv`, `receive_leg_pv`, `pay_leg_pv`, `direction`, `curve_currency`, `curve_provider`, `curve_name`, `tenors`, `rates`.

#### `GET /book-metrics` → 200

```json
{
  "benchmark": "MARKET_INDEX",
  "books": {
    "<book_id>": {
      "book_id": "<book_id>",
      "alpha": 0.0,
      "beta": 1.0,
      "observations": 2,
      "window": 100,
      "avg_book_return": 0.0,
      "avg_benchmark_return": 0.0,
      "benchmark_symbol": "MARKET_INDEX",
      "updated_at": "2026-10-06T12:00:00+00:00"
    }
  }
}
```

`books` stays empty until a book has at least two observations. `alpha` and `beta` are `null` when the variance of benchmark returns is 0. `window` comes from `BOOK_METRICS_WINDOW` (default 100).

#### `GET /book-metrics/<book_id>`

- **200** — the book metrics object alone (the value stored under `books` above).
- **404** — `{"error": "No metrics available for book_id: <book_id>"}`.



#### `GET /valuation-stream` → 200, `text/event-stream`

The connection stays open. A snapshot of the current store is sent first, then each new valuation:

```text
event: valuation_update
data: {"trade_id":"...","fair_value":1.5}

```



### Dependencies


| Kind          | What                                                                                                                                                                                                                                                                                                                                                                                |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Database      | PostgreSQL, driver `psycopg[binary]`, SQLAlchemy ORM (`DBSessionManager`)                                                                                                                                                                                                                                                                                                           |
| Tables        | `trades` (`ACTIVE` read every 5 s into `active_trades_cache`), `valuations` (batch `INSERT` from the queue), `market_data_curves` (latest `YIELD_CURVE` per currency, excluding the simulator provider), `audit_logs` (`AuditLogger`)                                                                                                                                               |
| Services      | `GET http://market-data-service:8001/stream` — blocking SSE, reconnect every 1 s. `GET http://market-data-service-integration:8005/market-data/quotes/<provider>/<symbol>` — timeout 5 s, provider in the URL is lower-case                                                                                                                                                         |
| Files         | `app.py`, `worker.py`, `calculation_service.py`, `valuation_service.py`, `cache_service.py`, `books/book_metrics_service.py`, `instruments_pricing/option_pricing_service.py`, `instruments_pricing/irs_pricing_service.py`, `custom_server.py`; package `shared/trading_shared` (`db`, `models`, `repositories`, `audit`, `enums`, `logging_config`, `market_data.is_quote_stale`) |
| External APIs | none — Finnhub / Twelve Data / Alpha Vantage / FRED / ECB are called only by the integration service                                                                                                                                                                                                                                                                                |
| Env           | `DATABASE_URL` (required), `BOOK_METRICS_WINDOW` (default `100`), `LOG_LEVEL`                                                                                                                                                                                                                                                                                                       |


`requirements.txt`: `bottle`, `SQLAlchemy`, `psycopg[binary]`.

### Load profile: mixed

The rating comes from the code paths, not from the service name.

- **I/O.** `pricing_worker` blocks on `urlopen` of the SSE stream. `provider_pricing_worker` fetches a quote over HTTP every 5 s. `cache_refresh_worker` reads `trades` every 5 s. `curve_pricing_worker` reads `market_data_curves` every 5 s. `valuation_worker` flushes a valuation batch with one `commit` every 0.2 s.
- **CPU.** After a tick arrives, the same loop computes: bond PV (cash-flow loop), Black-Scholes (`erf` in `option_pricing_service`), IRS discounting with curve interpolation (`irs_pricing_service`), and covariance and variance over the alpha/beta window (`book_metrics_service`). Every tick scans the whole `active_trades_cache`.
- **Critical zone: the cache is one page.** `cache_refresh_worker` calls `get_trades(status=ACTIVE)` with no `limit`. The repository default is `page=1`, `limit=50`, so `active_trades_cache` holds the 50 newest `ACTIVE` rows. `TradeRepository.get_active_trades()` loads the full set and this reload does not call it. A pricing run that assumes N trades marks at most 50 unless the reload changes.
- HTTP handlers only copy dicts under a lock. The weight sits in the background threads.



### Tests

Unit tests, in-process WSGI (`tests/unit/service_loader.py`), database mocked. Run on 2026-10-06 from `backend/.venv` (pytest 9.1.1, Python 3.14): **20 passed**.


| File                                           | Count | Coverage                                                                                                                                |
| ---------------------------------------------- | ----- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `tests/unit/test_pricing_service_endpoints.py` | 6     | `/health`, `/valuations`, valuation 404, `/book-metrics`, metrics 404, snapshot plus one SSE frame                                      |
| `tests/unit/test_pricing_service_worker.py`    | 12    | stream connect/reconnect, SSE parser, metrics, pricing worker, quote HTTP, provider worker, curve worker, valuation flush, cache reload |
| `tests/unit/test_pricing_service_cache.py`     | 2     | cache replacement on success, and keeping the previous cache when the database fails                                                    |


```bash
cd backend
.venv/bin/python -m pytest \
  tests/unit/test_pricing_service_endpoints.py \
  tests/unit/test_pricing_service_worker.py \
  tests/unit/test_pricing_service_cache.py -q
```

There are no integration tests against a live Postgres and stream for this service.

### How it runs

- Process: `python app.py`, listen `0.0.0.0:8002`.
- Worker processes: **1**. Background threads: **6** (`pricing_worker`, `metrics_worker`, `cache_refresh_worker`, `valuation_worker`, `provider_pricing_worker`, `curve_pricing_worker`), all `daemon=True`. Plus one thread per HTTP/SSE connection.
- Container: `backend/services/pricing-service/Dockerfile`, build context is the service directory.
- Compose (`backend/docker-compose.yml`): port `8002:8002`, volume `./shared:/app/shared`, env `DATABASE_URL`, `depends_on: db-seed`.
- Full stack with the frontend: from the repo root, `docker compose up --build`.

---



## 2. blotter-service

**Responsibility.** Returns the trade list and book summary, attaching the latest valuation from memory fed by the pricing-service SSE stream.

### Endpoints



#### `GET /health` → 200

```json
{
  "service": "blotter-service",
  "status": "UP",
  "cache_trade_count": 0,
  "cache_entry_count": 0
}
```

`cache_trade_count` is the number of `trade_id` keys in memory. `cache_entry_count` is the sum of entries (max 20 per trade).

#### `GET /books/summary`

No parameters.

- **200** — the body from `books-service` (`GET /books`, timeout 2 s), each book extended with three fields:

```json
{
  "books": [
    {
      "book_id": "<uuid>",
      "name": "Equity Book",
      "description": null,
      "expected_asset_class": "EQUITY",
      "is_active": true,
      "realized_pnl": 0.0,
      "unrealized_pnl": 0.0,
      "active_trades": 3
    }
  ]
}
```

PnL is the sum of the latest cached valuation per trade. `active_trades` is a `COUNT` on `trades` where `status = ACTIVE`.

- **500** — `{"error": "Failed to fetch book summary"}` when the books-service call or the aggregation raises.



#### `GET /trades` → 200

```json
{ "trades": [ { "<Trade columns>": "...", "valuation": null } ] }
```

`valuation` is the latest cache object, or `null`.

Query parameters (all optional strings):


| Parameter     | When absent             | Meaning                                                                                                                                                                                                                               |
| ------------- | ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `book_id`     | filter off              | `trades.book_id`                                                                                                                                                                                                                      |
| `asset_class` | filter off              | e.g. `EQUITY`                                                                                                                                                                                                                         |
| `status`      | filter off              | `ACTIVE` / `CLOSED` / `CANCELLED`                                                                                                                                                                                                     |
| `symbol`      | filter off              | e.g. `ACME`                                                                                                                                                                                                                           |
| `page`        | repository default `1`  | page, `offset = (page-1)*limit`                                                                                                                                                                                                       |
| `limit`       | repository default `50` | page size                                                                                                                                                                                                                             |
| `first_only`  | `False`                 | a non-empty string is truthy and the repository switches the query to `.first()` instead of a page; the handler still iterates the result as a list, so the list mode (parameter omitted) is the one that returns `{"trades": [...]}` |


Sort: `created_at` descending. `client_request_id` exists on the repository, and this route does not expose it.

Trade fields (`serialize`): `trade_id`, `book_id`, `asset_class`, `instrument_id`, `symbol`, `side`, `quantity`, `trade_price`, `trade_currency`, `trade_date`, `status`, `opened_at`, `closed_at`, `close_price`, `close_reason`, `source`, `market_data_provider`, `reference_price`, `reference_price_timestamp`, `client_request_id`, `metadata_payload`, `created_at`, `updated_at`. UUID as string, `datetime` as ISO-8601, `Decimal` as float.

A database error on this route is not caught — Bottle returns its default 500.

#### `GET /trades/<trade_id>`

- **200**

```json
{
  "trade": { },
  "latest_valuation": null,
  "valuation_history": [],
  "audit_logs": []
}
```

`trade` is as above. `latest_valuation` comes from the cache. `valuation_history` is up to 50 `valuations` rows ordered by `valuation_time` descending (the limit is in code, not in the query string). `audit_logs` is always an empty list.

- **404** — `{"error": "Trade not found"}`.



#### `GET /trades/<trade_id>/valuations` → 200

```json
{ "valuations": [] }
```

The same history as above. A missing trade yields an empty list, not a 404.

`valuations` row fields: `valuation_id`, `trade_id`, `book_id`, `asset_class`, `valuation_time`, `fair_value`, `market_value`, `unrealized_pnl`, `realized_pnl`, `total_pnl`, `currency`, `market_data_provider`, `market_data_timestamp`, `market_data_reference`, `valuation_payload`, `created_at`.

#### `GET /trades/<trade_id>/audit-logs` → 200

The handler is `pass`. Empty body, no JSON. The `audit_logs` table is not read.

### Dependencies


| Kind          | What                                                                                                                                                                                              |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Database      | PostgreSQL, `psycopg[binary]` + SQLAlchemy                                                                                                                                                        |
| Tables        | `trades` (list, detail, `COUNT` of active trades), `valuations` (history, limit 50), `audit_logs` (worker events written by `AuditLogger`; no read)                                               |
| Services      | `GET http://pricing-service:8002/valuation-stream` — SSE, timeout 30 s, backoff 1 s → 2 s → … → 30 s. `GET http://books-service:8004/books` — timeout 2 s, only for `/books/summary`              |
| Files         | `app.py`, `worker.py`, `trades_service.py`, `cache/valuation_cache_service.py`, `custom_server.py`; `shared/trading_shared` (`db`, `models`, `serialization`, `enums`, `audit`, `logging_config`) |
| External APIs | none                                                                                                                                                                                              |
| Env           | `DATABASE_URL`, `LOG_LEVEL`                                                                                                                                                                       |


Cache: module-level dict `valuation_cache`, at most 20 latest valuations per `trade_id`.

### Load profile: I/O-bound

- Handlers wait on Postgres (`SELECT` of trades and valuations, `COUNT` by `book_id`) and, on `/books/summary`, on HTTP to books-service.
- `valuation_worker` blocks on the SSE read, parses JSON, and appends a dict to a list.
- There is no numeric loop, pricing, or portfolio algebra. CPU time is JSON parsing and ORM row serialization.



### Tests

Run on 2026-10-06: **12 passed** (pytest 9.1.1, Python 3.14, `backend/.venv`).


| File                                           | Count | Coverage                                                                                                                              |
| ---------------------------------------------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `tests/unit/test_blotter_service_endpoints.py` | 7     | health from cache, summary 200 and 500, query filters, default `first_only=False`, trade 200/404, valuation history, empty audit-logs |
| `tests/unit/test_blotter_service_worker.py`    | 2     | connect to the valuation stream, cache update and backoff                                                                             |
| `tests/unit/test_blotter_service_cache.py`     | 3     | history per trade, trim to 20, `clear_cache`                                                                                          |


```bash
cd backend
.venv/bin/python -m pytest \
  tests/unit/test_blotter_service_endpoints.py \
  tests/unit/test_blotter_service_worker.py \
  tests/unit/test_blotter_service_cache.py -q
```



### How it runs

- Process: `python app.py`, listen `0.0.0.0:8006`.
- Worker processes: **1**. Background threads: **1** (`valuation_worker`, daemon). Plus one thread per HTTP connection.
- Container: `backend/services/blotter-service/Dockerfile` (`python:3.11-slim`).
- Compose: port `8006:8006`, volume `./shared:/app/shared`, env `DATABASE_URL`, `depends_on: db-seed`.

---



## 3. market-data-service

**Responsibility.** Simulates equity, bond, FX, option, IRS, benchmark, and USD/EUR curve ticks, and publishes them over SSE and to Postgres.

### Endpoints

No query parameters and no request body.

#### `GET /health` → 200

At startup:

```json
{
  "service": "market-data-service",
  "status": "UP",
  "generated_events": 0,
  "last_event_time": null,
  "last_symbol": null,
  "last_asset_type": null,
  "active_subscribers": 0
}
```

After the first `EVENT_GENERATED`, `metric_worker` adds `is_thread_alive` (bool). `active_subscribers` is counted in the handler from the SSE subscriber list. `generated_events` in this object is the metrics counter, not the number of instruments in the snapshot.

#### `GET /symbols` → 200

A map `{ "<asset_class>": ["<symbol>", ...] }` built from the current in-memory snapshot, with no database query. Curves have no `asset_type`, so they are left out of the map.

Example after the first generator cycle:

```json
{
  "OPTION": ["ACME"],
  "IRS": ["USD_IRS"],
  "BENCHMARK": ["MARKET_INDEX"],
  "EQUITY": ["ACME"],
  "BOND": ["GOVT_5Y"],
  "FX": ["EURUSD"]
}
```

The IRS symbol is `{currency}_IRS` from the simulator (for example `USD_IRS`).

#### `GET /snapshot` → 200

A copy of `market_tick_data_state`. Keys: `ACME_OPT`, `IRS`, `MARKET_INDEX`, `ACME`, `GOVT_5Y`, `EURUSD`, `USD_YIELD_CURVE`, `EUR_YIELD_CURVE`.

Side effect: every call writes an `audit_logs` row (`SNAPSHOT_GENERATED`).

Fields depend on the instrument:


| Key                                   | Notable fields                                                                                                                             |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| `ACME`                                | `event_id`, `asset_type=EQUITY`, `timestamp`, `symbol`, `bid`, `ask`, `last`                                                               |
| `GOVT_5Y`                             | `asset_type=BOND`, `symbol`, `yield`                                                                                                       |
| `EURUSD`                              | `asset_type=FX`, `symbol`, `spot`                                                                                                          |
| `ACME_OPT`                            | `asset_type=OPTION`, `symbol=ACME`, `currency`, `spot`, `strike`, `option_right_type`, `maturity_years`, `volatility`                      |
| `IRS`                                 | `asset_type=IRS`, `symbol`, `currency`, `notional`, `fixed_rate`, `maturity_years`, `payments_per_year`, `direction`, `description`        |
| `MARKET_INDEX`                        | `asset_type=BENCHMARK`, `symbol`, `currency`, `last`                                                                                       |
| `USD_YIELD_CURVE` / `EUR_YIELD_CURVE` | `curve_id`, `symbol`, `curve_name`, `curve_type=YIELD_CURVE`, `currency`, `tenors` (`1M`,`3M`,`1Y`,`5Y`), `rates`, `provider`, `timestamp` |




#### `GET /stream` → 200, `text/event-stream`

The connection stays open. There is no initial snapshot — the client receives ticks published after the subscription is stored. Each generator cycle sends eight frames (one per instrument):

```text
data: {"symbol":"ACME","asset_type":"EQUITY","bid":100.0,"ask":100.1,"last":100.05}

```

The `event:` field is not set. The client id is an 8-character UUID, kept only in process memory.

### Dependencies


| Kind                    | What                                                                                                                                                                                                                                                                                                                             |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Database                | PostgreSQL, `psycopg[binary]` + SQLAlchemy                                                                                                                                                                                                                                                                                       |
| Tables                  | `market_data_curves` — synchronous `INSERT` of curves on every `market_worker` cycle, before the tick goes to SSE. `market_data_spot_prices` — batch in `db_worker`, threshold `batch_size = 30`, `source = "GENERATED"`. `audit_logs` — not on connect, but on write errors, a successful spot batch, and every `GET /snapshot` |
| `market_data_snapshots` | `persist_full_snapshot` writes a `FULL` row; the function is tested; the `market_worker` loop does not call it                                                                                                                                                                                                                   |
| Outbound services       | none. Consumers (outside this process): pricing-service reads `GET /stream`                                                                                                                                                                                                                                                      |
| Files                   | `app.py`, `worker.py`, `market_data_simulator.py`, `custom_server.py`; `shared/trading_shared` (`db`, `models`, `enums`, `audit`, `logging_config`)                                                                                                                                                                              |
| External APIs           | none — prices come from `random` in `MarketDataSimulator`                                                                                                                                                                                                                                                                        |
| Env                     | `DATABASE_URL`, `TICK_INTERVAL_MS` (code default `100`; Compose sets `500`), `LOG_LEVEL`                                                                                                                                                                                                                                         |




### Load profile: mixed, I/O-dominant

- **CPU** is cheap: a random walk of eight instruments and `json.dumps` times the subscriber count. This is not Black-Scholes or discounting.
- **I/O dominates the cycle budget.** `persist_curves` commits to Postgres on every tick, before frames go on the stream. `db_worker` adds further spot commits. `GET /snapshot` adds an audit write. The loop then sleeps `TICK_INTERVAL_MS / 1000` (0.5 s in Compose).
- `/health`, `/symbols`, and the snapshot read copy a dict under a lock; the cost shows up on the audit write and on SSE fan-out.



### Tests

Run on 2026-10-06: **14 passed** (pytest 9.1.1, Python 3.14, `backend/.venv`).


| File                                               | Count | Coverage                                                                                                                                                                                             |
| -------------------------------------------------- | ----- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `tests/unit/test_market_data_service_endpoints.py` | 4     | health and subscriber count, symbol grouping, snapshot without touching the spot table, one SSE frame and client unsubscribe                                                                         |
| `tests/unit/test_market_data_service_worker.py`    | 10    | snapshot replacement, fan-out, curve write and database error, `persist_full_snapshot` and its error, one `market_worker` cycle, spot mapping and buffer clear on error, health update from a metric |


```bash
cd backend
.venv/bin/python -m pytest \
  tests/unit/test_market_data_service_endpoints.py \
  tests/unit/test_market_data_service_worker.py -q
```



### How it runs

- Process: `python app.py`, listen `0.0.0.0:8001`.
- Before the threads start, the process generates one tick and puts it only into the snapshot memory (no SSE and no write).
- Worker processes: **1**. Background threads: **3** (`market_worker`, `metric_worker`, `db_worker`), all daemon. Plus one thread per `/stream` subscriber and per other request.
- Container: `backend/services/market-data-service/Dockerfile` (`python:3.11-slim`).
- Compose: port `8001:8001`, volume `./shared:/app/shared`, env `TICK_INTERVAL_MS=500` and `DATABASE_URL`, `depends_on: db-seed`.

---



## 4. monitoring-service

**Responsibility.** Probes the other application services once a second and serves the last status snapshot from memory.

### Endpoints

No request body.

#### `GET /health` → 200

```json
{ "service": "monitoring-service", "status": "UP" }
```

This handler does not probe anyone. It reports the monitoring process itself.

#### `GET /status` → 200

The in-memory `status_payload`: `services`, `environment`, `kpis`, `alerts`. `services` is keyed by the probe name (`book-service` for books, not `books-service`). Each entry carries the probe status, `last_checked`, `response_time_ms`, and a short summary. `kpis` includes `errors_5m` (DOWN transitions inside a 300 s window) plus the last market tick and last valuation copied out of the health bodies of market-data and pricing.

The map stays at its startup shape (`services: {}`) until the first probe loop finishes.

### Dependencies


| Kind          | What                                                                                                                                                                                                |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Database      | none on the probe path. Startup and shutdown call `AuditLogger`, which writes `audit_logs`                                                                                                          |
| Services      | serial `GET` of `/health`, timeout 2 s: market-data `:8001`, pricing `:8002`, trade-generation `:8007`, trade-action `:8080`, books `:8004`, blotter `:8006`. The integration service is not probed |
| Files         | `app.py`, `worker.py`, `custom_server.py`; `shared/trading_shared` (`audit`, `enums`, `logging_config`)                                                                                             |
| External APIs | none                                                                                                                                                                                                |
| Env           | `LOG_LEVEL`                                                                                                                                                                                         |


`requirements.txt`: `bottle`, `SQLAlchemy`, `psycopg[binary]`. SQLAlchemy is pulled in by the shared package; the probe loop does not open a session.

### Load profile: I/O-bound

- **Critical zone: off the trade and tick path.** `monitoring_worker` calls the six health URLs one after another, then sleeps 1 s. Six timeouts make one lap about 12 s. `GET /status` only copies the dict built at the end of the lap.
- The side effect that does reach a critical service: each lap adds one `GET /health` to books-service, which has a single request thread. That probe queues behind blotter `/books/summary` and the generator's `GET /books`.



### Tests

No modules under `backend/tests/unit` for this service.

### How it runs

- Process: `python app.py`, listen `0.0.0.0:8003`.
- Worker processes: **1**. Background threads: **1** (`monitoring_worker`, daemon). Plus one thread per HTTP request.
- Container: `backend/services/monitoring-service/Dockerfile` (`python:3.11-slim`).
- Compose: port `8003:8003`, volume `./shared:/app/shared`, env `DATABASE_URL`, `depends_on: db-seed`.

---



## 5. books-service

**Responsibility.** Stores the book catalogue. The blotter summary and every generated OPEN read it over HTTP.

### Endpoints

`GET` has no body. `POST` and `PUT` read `request.json`.

#### `GET /health` → 200

```json
{ "service": "books-service", "status": "UP" }
```



#### `GET /books` → 200

```json
{ "books": [ { "book_id": "<uuid>", "name": "Equity Book", "description": null, "expected_asset_class": "EQUITY", "is_active": true } ] }
```

Active books only (`is_active = true`). No query parameters.

#### `GET /books/<book_id>`

- **200** — the same book object.
- **404** — `{"error": "Book not found"}`.



#### `POST /books`

Body: one book object, or a list of them. Required fields: `name`, `expected_asset_class`. Optional: `description`.

- **201** — `{"message": "Books created successfully", "book_ids": ["<uuid>"]}`.
- **400** — missing or invalid JSON.
- **409** — duplicate name inside the payload, or a unique-constraint clash.
- **500** — any other error. The handler rolls the session back.

`created_by` is the literal `"API"`.

#### `PUT /books/<book_id>`

Partial update of `name`, `description`, `expected_asset_class`, `is_active`. Changing `expected_asset_class` counts active trades on the book and then still writes the new class.

- **200** — `{"message": "Book with id <book_id> updated successfully"}`.
- **400** / **404** / **409** — invalid JSON, missing book, or a name that already exists.



#### `DELETE /books/<book_id>`

Soft delete: sets `is_active = false`. Refuses while any trade on the book is `ACTIVE`.

- **200** — `{"message": "Book with id <book_id> marked as inactive"}`.
- **404** — book missing.
- **409** — `{"error": "Cannot delete book while <n> active trade(s) remain"}`.



### Dependencies


| Kind          | What                                                                                                                     |
| ------------- | ------------------------------------------------------------------------------------------------------------------------ |
| Database      | PostgreSQL, one module-level `SessionLocal()` held for the process. Not `DBSessionManager`                               |
| Tables        | `books` (CRUD). `trades` is counted on asset-class change and on delete                                                  |
| Services      | none outbound. Callers: blotter `GET /books/summary` (timeout 2 s) and trade-generation OPEN (`GET /books`, timeout 2 s) |
| Files         | `app.py`, `books_manager_service.py`; `shared/trading_shared` (`db`, `models`, `audit`, `enums`, `logging_config`)       |
| External APIs | none                                                                                                                     |
| Env           | `DATABASE_URL`, `LOG_LEVEL`                                                                                              |


`requirements.txt`: `bottle`, `SQLAlchemy`, `psycopg[binary]`.

There is no `custom_server.py` in this service.

### Load profile: I/O-bound, single-threaded

- **Critical zone.** `app.run(host="0.0.0.0", port=8004)` uses Bottle's default server: one thread. Blotter summary, the generator's OPEN, and the monitoring probe share that thread. A stuck request holds every caller until its own timeout (2 s on the two trading callers).
- The queries themselves are small (`books` filtered by `is_active`, a `COUNT` of active trades on delete).
- **The session is process-wide, and create closes it.** `POST /books` calls `db.close()` in `finally` on that shared session. The next request uses a closed session. `GET` and `PUT` do not close it.



### Tests

No modules under `backend/tests/unit` for this service.

### How it runs

- Process: `python app.py`, listen `0.0.0.0:8004`.
- Worker processes: **1**. Background threads: **0**. HTTP is the single `wsgiref` thread.
- Container: `backend/services/books-service/Dockerfile` (`python:3.11-slim`).
- Compose: port `8004:8004`, volume `./shared:/app/shared`, env `DATABASE_URL`, `depends_on: db-seed`.

---



## 6. market-data-service-integration

**Responsibility.** Polls Finnhub, Twelve Data, Alpha Vantage, FRED, ECB, and NBP, and stores quotes and curves for pricing and for provider-mode trade entry.

### Endpoints

Routes are registered from `routes/`. An unknown provider key is **404** `{"error": "Unknown provider: <provider>"}`. `json_error` sets the status named below.

#### `GET /health` → 200

```json
{
  "service": "market-data-service-integration",
  "status": "UP",
  "quote_service": {},
  "curve_service": {}
}
```

`quote_service` and `curve_service` are the in-memory fetch audit (`last_success`, `last_error`, `last_query_time`, `error_count`). No database read.

#### `GET /providers` → 200

`providers`: `finnhub`, `alpha_vantage`, `twelve_data`. `curve_providers`: `fred`, `ecb`, `nbp`.

#### `GET /providers/health` and `GET /providers/<provider>/health`

Live probe of the named provider. A provider that is neither a quote nor a curve provider is an unknown-provider error.

#### `GET /market-data/symbols` → 200

Symbol lists from env (`MARKET_DATA_SYMBOLS`, FX, bond, IRS) plus `FUTURES_CONTRACTS` (`symbol` → `underlying`, `multiplier`). No database read.

#### `GET /market-data/snapshot` → 200

`{"snapshot": [ <latest quote per provider and symbol> ]}`. Reads `market_data_provider_quotes`. Trade-generation calls this on every provider-mode OPEN.

#### `GET /market-data/quotes?symbol=&asset_class=`

- **200** — `{"quotes": [...]}`. `asset_class` defaults to `EQUITY`.
- **400** — missing `symbol`, or an `asset_class` outside the enum.



#### `GET /market-data/quotes/<provider>/<symbol>`

The route pricing calls, timeout 5 s on the pricing side. Provider in the URL is matched lower-case against the provider map; the row lookup upper-cases it.

- **200** — one serialized quote.
- **404** — `Quote not found`.



#### `GET /market-data/fx-quotes?symbol=` and `GET /market-data/curve-quotes?symbol=&asset_class=`

FX quotes for a symbol. Curve quotes require `asset_class` of `BOND` or `IRS` and a symbol present in the env map. **400** otherwise.

#### `GET /curves` and `GET /curves/<provider>`

Latest persisted curve. The list endpoint returns `{"curves": [...]}`. The provider endpoint is **404** `Curve not found` when nothing is stored. These reads do not call FRED or ECB.

#### `GET|POST /market-data/refresh` and `GET|POST /curves/refresh`

Run one quote cycle or one curve cycle immediately and return its summary (`requests`, `success`, `errors`, and for curves `enqueued`, `dropped`, `queue_size`). The HTTP call blocks until the cycle finishes, and it takes the same cycle lock as the background poll.

#### `GET /market-data/stream` and `GET /market-data/stream/<provider>`

`text/event-stream`. `?mock=1` (also `true` / `yes`) or `MARKET_DATA_MOCK_STREAM=true` yields the synthetic generator. Otherwise each lap calls the aggregator, which runs provider fetches on threads with `PROVIDER_FETCH_TIMEOUT_SECONDS` (Compose sets `1.0`; the code default is `10`).

### Dependencies


| Kind          | What                                                                                                                                                                                                                                                                                                                                                                  |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Database      | PostgreSQL, `psycopg[binary]` + SQLAlchemy                                                                                                                                                                                                                                                                                                                            |
| Tables        | `market_data_provider_quotes` and `market_data_snapshots` (one quote insert also writes a `QUOTE` snapshot). `market_data_curves` via the curve persistence worker. `market_data_provider_limits` loaded once at startup into `RequestTracker`. `audit_logs` from the quote `db_worker` (`AuditLogger`, one commit per quote) and from the fetch-audit worker (batch) |
| Services      | none inbound from the other app services except pricing (`GET /market-data/quotes/<provider>/<symbol>`) and trade-generation in provider mode (`/market-data/symbols`, `/market-data/snapshot`)                                                                                                                                                                       |
| Files         | `app.py`, `routes/*`, `core/*` (`quote_service`, `curve_service`, `polling/orchestrator`, `polling/curve_persistence_worker`, `worker`, `audit_worker`, `aggregator`, `stream_hub`), `providers/quota_providers/*`, `providers/curve_providers/*`, `custom_server.py`                                                                                                 |
| External APIs | Finnhub, Twelve Data, Alpha Vantage (quotes). FRED, ECB, NBP (curves and FX). `urllib.request.urlopen`                                                                                                                                                                                                                                                                |
| Env           | `DATABASE_URL`, the five API keys, `MARKET_DATA_POLL_INTERVAL_SECONDS` (default 30), `CURVE_DATA_REFRESH_INTERVAL_SECONDS` (default 3600), symbol lists, `PROVIDER_FETCH_TIMEOUT_SECONDS`, `MARKET_DATA_MOCK_STREAM`, `MARKET_DATA_CURVE_QUEUE_MAXSIZE` (default 1000), curve batch size 20 and flush 2 s, audit batch 10 and flush 5 s                               |


`requirements.txt`: `bottle`, `SQLAlchemy`, `psycopg[binary]`.

### Load profile: I/O-bound, external API

- **Critical zone: the quote poll.** `PollWorkers.run_quote_cycle` walks every poll symbol and every quote provider and calls `fetch_quote` → `urlopen` **with no timeout**. `PROVIDER_FETCH_TIMEOUT_SECONDS` applies only to `MarketDataAggregator`, which the stream uses and the poll does not. One hung provider stalls the cycle. The cycle holds `_quote_cycle_lock`. If the lap runs longer than the interval, the next lap starts immediately.
- **Write path.** Each quote is `put` on an unbounded `db_queue`. `db_worker` commits one quote row and one snapshot row, then `AuditLogger` commits again. A full symbol list times three providers is tens of those pairs per 30 s cycle.
- **Curves.** Separate loop, default 3600 s, HTTP timeout 15 s in the curve provider base. Persistence is a bounded queue (default 1000); overflow is dropped, not blocked. Batches of 20, or a flush every 2 s.
- **Quota.** `RequestTracker.can_request` rejects a fetch before HTTP when the provider limit loaded from `market_data_provider_limits` is spent. That ceiling does not move if the HTTP server changes.
- `GET /market-data/refresh` runs the same locked cycle on a request thread, so a manual refresh waits behind the poll and the poll waits behind the refresh.



### Tests

No modules under `backend/tests/unit` for this service.

### How it runs

- Process: `python app.py`, listen `0.0.0.0:8005`, `server=ThreadedServer`.
- Worker processes: **1**. Background threads: **5**, all daemon — `db_worker`, fetch `audit_worker`, quote poll, curve poll, curve persistence.
- `RequestTracker` is loaded from the database before the poll threads start. An empty limits table means fetches are denied as `unknown_provider`.
- Container: `backend/services/market-data-service-integration/Dockerfile` (`python:3.10-slim`). The other seven images are 3.11.
- Compose: port `8005:8005`, volume `./shared:/app/shared`, provider keys and the env in the dependency table, `depends_on: db-seed`.

---



## 7. trade-generation-service

**Responsibility.** Builds OPEN and CLOSE intentions and posts them to trade-action. It does not write `trades` itself.

### Endpoints



#### `GET /health` → 200

```json
{ "service": "trade-generation-service", "status": "UP" }
```

Does not say whether the generator thread is running. That is `GET /status`.

#### `GET /status` → 200

```json
{ "is_running": false, "total_generated": 0, "expected_rate_per_sec": 0.0 }
```

`expected_rate_per_sec` is `1000 / interval_ms` while running, else `0`.

#### `GET /config` → 200

The live config object: `interval_ms`, `batch_size`, `open_weight_pct`, `close_weight_pct`, `qty_min`, `qty_max`, `price_min`, `price_max`, `use_provider_data`.

#### `PUT /config`

Patches those keys. Unknown or ill-typed values are **400**. `interval_ms` must be `>= 50`. When `use_provider_data` is in the body, the handler also `PUT`s trade-action `/provider-mode`. If that call fails, the local flag is restored and the response is **502**.

#### `POST /start`

Starts `trade_generation_worker` as a daemon thread.

- **200** — `{"message": "Generator started successfully."}`.
- **400** — already running.



#### `POST /stop`

- **200** — stop requested. The worker wakes without finishing the current sleep.
- **400** — `{"message": "No data available."}` when nothing is running.



#### `GET /generate-once`

One intention, sent to trade-action even when the background generator is stopped. **200** with the intention, or `{"message": "No trade intention generated."}`.

#### `POST /generate-trade`

Forwards the JSON body to trade-action as a single intention (the trading ticket uses this).

- **200** — the forwarded body.
- **400** — missing payload, or trade-action rejected it (`ValueError` message).



#### `GET /generate-batch`

Builds `batch_size` intentions (default 10) from one shared books/symbols/snapshot fetch, then one `POST /trade-actions/batch`. **200** with `intentions`, or a message that none were generated.

### Dependencies


| Kind          | What                                                                                                                                                                                                                                                                                                   |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Database      | PostgreSQL, only for CLOSE: `get_trades(status=ACTIVE)`, default page 50                                                                                                                                                                                                                               |
| Services      | `GET books-service:8004/books` (timeout 2 s). Symbols and snapshot from market-data `:8001`, or from the integration service when `use_provider_data` is on. `POST trade-action:8080/trade-actions` and `/trade-actions/batch` (retry, 3 attempts, timeout 5 s). `PUT /provider-mode` on config change |
| Files         | `app.py`, `worker.py`, `generator/` (`config`, `http`, `intentions`, `lifecycle`), `custom_server.py`                                                                                                                                                                                                  |
| External APIs | none — provider HTTP stays inside the integration service                                                                                                                                                                                                                                              |
| Env           | `DATABASE_URL`, `TRADE_GENERATION_INTERVAL_MS` (4000), `TRADE_GENERATION_BATCH_SIZE` (10), open/close weights 70/30, qty 10–1000, price 50–150, `USE_PROVIDER_DATA` (false)                                                                                                                            |


The three context fetches share a `ThreadPoolExecutor` of 3. Provider mode raises the loop interval to at least 15 s. CLOSE in provider mode only picks trades whose `source` is `TRADING_TICKET`.

`requirements.txt`: `bottle`, `SQLAlchemy`, `psycopg[binary]`.

### Load profile: I/O-bound

- **Critical zone: it waits on other services, and the default rate does not fill trade-action.** One intention every 4 s. OPEN blocks on books (single-threaded), symbols, and snapshot. Simulator `GET /snapshot` on market-data writes an audit row before it returns. Provider-mode snapshot reads the integration quote table.
- CLOSE calls `get_trades(status=ACTIVE)` with the repository default `limit=50` and picks at random from that page, not from the whole book.
- `send_to_trade_action_service` retries three times and logs a failure. It does not surface the error on the background loop. `POST /generate-trade` does surface it.
- CPU is a coin flip of OPEN vs CLOSE and a random quantity. The weight is the HTTP and the one indexed read of `trades`.



### Tests

No modules under `backend/tests/unit` for this service.

### How it runs

- Process: `python app.py`, listen `0.0.0.0:8007`, `server=ThreadedServer`.
- Worker processes: **1**. Background threads: **0** at boot. `POST /start` adds one daemon (`trade_generation_worker`).
- Container: `backend/services/trade-generation-service/Dockerfile` (`python:3.11-slim`).
- Compose: port `8007:8007`, volume `./shared:/app/shared`, the env above, `depends_on: db-seed`. Compose does not call `/start`.

---



## 8. trade-action-service

**Responsibility.** The only writer of `OPEN_TRADE` and `CLOSE_TRADE`. The generator and the ticket both end here.

### Endpoints



#### `GET /health` → 200

```json
{ "service": "trade-action-service", "status": "UP" }
```

Does not read the queue. Queue depth and counters are `GET /status`.

#### `GET /status` → 200

`metrics.build_status` for `trade_queue`: processed, errors, rejected, overload (503), duplicates, queue size against `TRADE_ACTION_QUEUE_MAXSIZE`, and recent actions. In-memory only.

#### `PUT /provider-mode`

Body: `{"use_provider_data": <bool>}`.

- **200** — `{"use_provider_data": true|false}`.
- **400** — missing field, or a value that is not a boolean.

The flag is process memory, guarded by a lock. Trade-generation's `PUT /config` is what normally sets it. Compose also seeds it from `USE_PROVIDER_DATA` (default `false`).

#### `POST /trade-actions`

One action. Required: `action_type` (`OPEN_TRADE` or `CLOSE_TRADE`) and `client_request_id`. With `use_provider_data` on, an OPEN also needs `market_data_provider`, `reference_price`, `reference_price_timestamp`, and `source`. A caller that sends a provider is freshness-checked even when the flag is off.

- **202** — `{"message": "Trade action accepted for processing", "client_request_id": "..."}`. The row is not written yet.
- **400** — bad JSON, missing fields, or a stale / missing quote. The body is `{"error": "<reason>"}`.
- **503** — `{"error": "System overloaded, please try again later"}` when the queue is full.



#### `POST /trade-actions/batch`

A non-empty JSON list. Each item is validated and enqueued on its own. A validation failure is recorded and skipped. A full queue stops the loop.

- **202** — `{"message": "Batch processed", "accepted_count": <n>, "errors": ["Item <i>: ..."]}`.
- **400** — body is not a non-empty list, or JSON does not parse.
- **503** — `{"error": "System overloaded, queue is full", "accepted_so_far": <n>}`. Items already queued stay queued.



### Dependencies


| Kind          | What                                                                                                                                                                                                                                                                                                |
| ------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Database      | PostgreSQL. The HTTP thread reads `market_data_provider_quotes` when a provider is present (`get_latest`). The worker writes `trades` (`INSERT ... ON CONFLICT DO NOTHING` on `client_request_id`), may insert `instruments`, and on CLOSE updates the trade and inserts a closing `valuations` row |
| Tables        | `trades`, `instruments`, `valuations`, `market_data_provider_quotes`, `audit_logs`                                                                                                                                                                                                                  |
| Services      | none outbound. Callers: trade-generation, and the ticket via `POST /generate-trade` which forwards here                                                                                                                                                                                             |
| Files         | `app.py`, `worker.py`, `trade_action_service.py`, `metrics.py`, `option_pricing_service.py`, `custom_server.py`                                                                                                                                                                                     |
| External APIs | none — freshness is the quote row the integration service already stored                                                                                                                                                                                                                            |
| Env           | `DATABASE_URL`, `TRADE_ACTION_QUEUE_MAXSIZE` (default 100), `USE_PROVIDER_DATA` (default false), `TRADE_ACTION_MAX_CLOCK_SKEW_SECONDS` (default 5). Compose also sets `TRADE_VALUATION_QUEUE_MAXSIZE`; this service does not read it. Pricing's valuation queue is unbounded                        |


`requirements.txt`: `bottle`, `SQLAlchemy`, `psycopg[binary]`.

### Load profile: I/O-bound, single consumer

- **Critical zone.** `queue.Queue(maxsize=100)` and one `trade_action_worker`. `put(..., block=False)` : a full queue is 503, plus an audit commit. The default generator (one intention every 4 s) does not reach that cap. A throughput run does.
- **Two HTTP shapes.** Simulator mode, no provider on the payload: validation is in memory and the request ends at `put`. Provider mode, or any payload that names a provider: `validate_trade_action` opens a session and reads the latest quote before enqueue. HTTP latency then includes Postgres. A quote older than the asset-class window, or timestamped more than 5 s in the future, is rejected. The handler returns `None` for `QuoteStaleness.STALE` and a string otherwise — a stale quote is accepted, a missing quote is 400.
- **The worker transaction.** OPEN: non-locking idempotency read, optional instrument insert, then `INSERT ... ON CONFLICT DO NOTHING`. CLOSE: `SELECT ... FOR NO KEY UPDATE` (`with_for_update(key_share=True)`), a second quote read when the trade has a provider, status update, closing valuation, `commit`. The row lock is held until that commit and overlaps blotter reads and the pricing cache reload, which also read `trades`.
- After the business `commit`, `audit_logger.info(..., db=db)` still opens a second connection. The passed session is not reused.



### Tests

No modules under `backend/tests/unit` for this service.

### How it runs

- Process: `python app.py`, listen `0.0.0.0:8080`, `server=ThreadedServer`.
- Worker processes: **1**. Background threads: **1** (`trade_action_worker`, daemon). Plus one thread per HTTP request. Those request threads can each hold a pool connection during the freshness read while the worker holds another.
- Container: `backend/services/trade-action-service/Dockerfile` (`python:3.11-slim`).
- Compose: port `8080:8080`, volume `./shared:/app/shared`, env `DATABASE_URL`, `TRADE_ACTION_QUEUE_MAXSIZE`, `TRADE_VALUATION_QUEUE_MAXSIZE`, `USE_PROVIDER_DATA`, `depends_on: db-seed`.

---



## Combined test result

One run of all eight files for pricing, blotter, and market-data, 2026-10-06:

```text
46 passed in 0.19s
```

`monitoring-service`, `books-service`, `market-data-service-integration`, `trade-generation-service`, and `trade-action-service` have no modules under `backend/tests/unit`.

Test interpreter: `backend/.venv` (Python 3.14, pytest 9.1.1). Container images use Python 3.11, except `market-data-service-integration` (`python:3.10-slim`). `tests/unit/conftest.py` sets `DATABASE_URL=sqlite:///:memory:` when the variable is unset; the tests still replace the database session with a mock.