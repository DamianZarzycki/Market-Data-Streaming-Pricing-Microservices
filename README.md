# Market Data & Pricing System

## 1. System Overview
A distributed system that simulates real-time market data, generates and books trades, and prices them. Independent microservices run in Docker Compose and share Postgres; live marks and health are kept in per-process memory (see §8).

## 2. System Architecture
The stack is a set of independent microservices:
* **market-data-service (Port: 8001):** Simulates the market. A background worker emits ticks for EQUITY, BOND, FX, OPTION, IRS, and a synthetic `MARKET_INDEX` benchmark. The live book is an in-memory snapshot (`GET /snapshot` / `GET /stream`).
* **pricing-service (Port: 8002):** Subscribes to the market stream, marks active trades to market (fair value, PnL), and keeps the latest valuations plus book alpha/beta in memory.
* **monitoring-service (Port: 8003):** Probes every service `/health` and serves a combined `GET /status` envelope from its health cache.
* **books-service (Port: 8004):** Book master data (create / update / soft-delete).
* **blotter-service (Port: 8006):** Trade list API. A worker mirrors pricing SSE into an in-memory valuation cache used by list/detail responses.
* **trade-generation-service (Port: 8007):** Builds random OPEN/CLOSE intentions. Runtime knobs live in an in-memory config seeded from env; `PUT /config` updates them until process restart.
* **trade-action-service (Port: 8080):** Accepts intentions, persists trades to Postgres, and computes realized PnL on CLOSE.

## 3. Running the Project
In the main project folder, run this command:
```bash
docker compose up --build
```

## 4. Endpoints Description

### Market Data Service (localhost:8001)
* **GET /health** - Returns the service status and stats about generated events.
* **GET /snapshot** - Returns the current state of all market instruments.
* **GET /stream** - Opens a streaming connection with generated events.

### Pricing Service (localhost:8002)
* **GET /health** - Returns the service status, market stream connection state, and the time of the last pricing.
* **GET /valuations** - Returns all currently calculated instrument prices.
* **GET /valuations/<instrument_id>** - Returns the price of a specific instrument (e.g., EQ_ACME).
* **GET /book-metrics** - Returns the latest alpha/beta metrics for every book (see section 11).
* **GET /book-metrics/<book_id>** - Returns the alpha/beta metrics for a single book.

### Monitoring Service (localhost:8003)
* **GET /health** - Returns the status of the monitoring service itself.
* **GET /status** - Returns an envelope with `services` (probe map), `environment` (rollup + freshness), and `kpis` (`errors_5m`, `last_market_tick`, `last_valuation`).

## 5. Streaming Mechanism
We used SSE (Server-Sent Events). It is a one-way connection from the server to the client. The client connects to the server, and the server keeps sending new events in an endless loop.

## 6. Concurrency in the System
* **Background workers** - each service has a separate task running in the background.
* **ThreadedServer** - a multi-threaded server stops the whole system from getting blocked.
* **Locks** - give us access to safe and consistent data.

## 7. Testing the System
You can test the system using `curl` commands or by opening the links in a web browser.

## 8. In-memory caches

There is no Redis or shared cache process. Each service keeps dicts in its own
process memory. A restart of that container wipes them. They are **not**
persisted to Postgres (except the underlying trades that pricing reloads from
the DB).

| Service | Store | What it holds | How it is refreshed |
| --- | --- | --- | --- |
| market-data | `market_tick_data_state` | Latest tick per instrument | Overwritten every generator cycle; served by `GET /snapshot` and `GET /stream` |
| pricing | `active_trades_cache` | Active `Trade` rows keyed by `trade_id` (includes `quantity`) | Full reload from DB every **5s** (`cache_refresh_worker`) |
| pricing | `valuations_store` | Latest valuation dict per trade | Written on every mark-to-market; `GET /valuations` and `/valuation-stream` |
| pricing | book metrics (`book_returns`, `metrics_store`) | Rolling window of book vs `MARKET_INDEX` returns, latest alpha/beta | Updated on each benchmark tick; window size `BOOK_METRICS_WINDOW` (default 100) |
| blotter | `valuation_cache` | Up to **20** recent live valuations per trade | Worker appends frames from pricing `/valuation-stream`; list/detail APIs attach `latest_valuation` from here |
| monitoring | `health_cache` | Last `/health` probe result per service | Probe loop; `GET /status` reads this map |

**Why `active_trades_cache` exists:** pricing must mark every active trade on
each market tick. Hitting Postgres per tick would stall the SSE loop. Quantity
used in `fair_value` and unrealized PnL comes from the cached `Trade` row, not
from trade-generation at pricing time.

**Known gap:** CLOSE in trade-action-service writes the trade as closed in the
DB but does **not** evict it from `active_trades_cache`. A closed trade can
keep being priced until the next 5s reload. There is a TODO on the close path
for cache invalidation.

**Trade-generation config** is also in-process state (`generator_config`):
seeded from `TRADE_GENERATION_*` env vars at boot, mutated by `PUT /config`.
That is runtime config, not a market-data cache.

## 9. Implementation Challenges
* The biggest architectural challenge was handling concurrency, especially using locks (`threading.Lock`):

* Putting an unnecessary lock on network tasks (like asking other services via HTTP) slowed down the whole system or blocked it completely.

## 11. Book-level Alpha & Beta

The pricing engine computes **alpha** and **beta** at the **book** level (not per
individual trade). These are the classic CAPM-style sensitivity and
excess-return measures of a book against a market benchmark.

### Formulas
For a rolling window of aligned return observations:

```
beta  = covariance(book_returns, benchmark_returns) / variance(benchmark_returns)
alpha = average(book_returns) - beta * average(benchmark_returns)
```

* **beta** — how strongly a book moves with the benchmark (1.0 = moves in line,
  >1 = amplifies market moves, <1 = dampens them, negative = moves against it).
* **alpha** — the average return the book delivers *beyond* what its beta
  exposure to the benchmark would explain (skill / idiosyncratic return).

### The chosen benchmark: `MARKET_INDEX`
The benchmark is a single **synthetic instrument** called **`MARKET_INDEX`**,
generated by the market-data-service under the new `BENCHMARK` asset class
(`synthetic = true` in the `instruments` table).

**Why this benchmark?**
* **Self-contained & deterministic-ish.** The whole stack is a simulation with
  no external market feed, so a self-generated index keeps the system fully
  reproducible and offline — no third-party data dependency.
* **Represents "the market".** `MARKET_INDEX` behaves like a broad equity
  index: it starts at 1000 and follows a random walk with a small positive
  drift (`generate_benchmark_tick`). The drift gives it a non-zero mean return
  so alpha/beta are economically meaningful rather than pure noise.
* **One index for all books.** A single reference index keeps the model simple
  and lets every book be compared on the same yardstick, which is exactly what
  beta is meant to express. (A natural future extension is a per-asset-class
  benchmark.)
* **Persisted as a time series.** Every benchmark tick is stored in
  `market_data_spot_prices` (asset_class `BENCHMARK`), so the reference series
  is auditable just like any other market data.

### How it is computed
1. The market-data-service emits a `MARKET_INDEX` tick every cycle alongside the
   other instruments and persists it.
2. The pricing-service treats each benchmark tick as the **sampling clock**. On
   every benchmark observation it:
   * snapshots each book's current **market value** (the sum of the latest
     `fair_value` across all of that book's trades — `fair_value` already embeds
     quantity and multiplier);
   * converts both the book value and the benchmark level into **simple
     returns** relative to the previous observation;
   * appends the aligned `(book_return, benchmark_return)` pair to a **rolling
     window** (default **100** observations, configurable via
     `BOOK_METRICS_WINDOW`);
   * recomputes alpha and beta from the window.

Sampling both series on the same clock guarantees the book and benchmark return
series are aligned and equal-length, which is required for a valid covariance.
Covariance and variance use the same (population) denominator, so it cancels in
the beta ratio.

### Book value vs. PnL
The assignment allows either a book **value** series or a **PnL** series. We use
the book's **market value** (sum of `fair_value`) because it is always positive
and never crosses zero, making simple percentage returns well-defined — unlike
PnL, which can flip sign and make return ratios blow up.

### Notes / limitations
* Metrics need at least 2 aligned observations before alpha/beta are reported;
  they stay empty until a book has been priced across two benchmark ticks.
* Book market value is aggregated from the in-memory latest valuations, so a
  book only appears once its trades have been priced at least once.
* Metrics live in memory in the pricing-service and are rebuilt from the live
  stream after a restart.

## 10. Migrations

### DB init
* docker run --rm -d --name postgres -e POSTGRES_USER=user -e POSTGRES_PASSWORD=password -e POSTGRES_DB=market_data_db -p 5432:5432 postgres:15

### To run first migration. Thanks to volume of ./db folder we are able to make Alembic to create files for us there
* Creates the initial configuration files on your drive (the migrations folder, env.py, and alembic.ini files). It does not touch the database. You run this only once at the start.

### docker compose run --rm db-migrations alembic init migrations
* alembic.ini is generated and migarions folder with env.py file.

* Start here if db is already there

### docker compose run --rm db-migrations alembic revision --autogenerate -m "message"
* Compares your code (models.py) with the database and generates a new update script file (in the versions folder). The database itself remains unchanged at this point.

### docker compose run --rm db-migrations alembic upgrade head
* Reads the generated scripts from the versions folder and physically executes them in the database. This is the command that actually creates or modifies the tables

docker compose exec postgres psql -U user -d market_data_db