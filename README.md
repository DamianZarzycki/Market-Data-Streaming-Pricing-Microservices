# Market Data & Pricing System

## 1. System Overview
A distributed system that simulates real-time market data, generates and books trades, and prices them. Independent microservices run in Docker Compose and share Postgres; live marks and health are kept in per-process memory (see §8).

## 2. System Architecture
The stack is a set of independent microservices:
* **market-data-service (Port: 8001):** Simulates the market. A background worker emits ticks for EQUITY, BOND, FX, OPTION, IRS, and a synthetic `MARKET_INDEX` benchmark. The live book is an in-memory snapshot (`GET /snapshot` / `GET /stream`). Used to price *generated* trades (no `market_data_provider`).
* **market-data-service-integration (Port: 8005):** The only service that talks to real external providers. Polls Alpha Vantage / Finnhub / Twelve Data (market data) and NBP / ECB / FRED (reference curves) via hand-written HTTP clients, normalizes to one internal quote/curve model, persists to Postgres, and republishes internally over SSE. Backs the trading ticket's provider comparison and the provider-priced valuations. See §12.
* **pricing-service (Port: 8002):** Subscribes to the market stream, marks active trades to market (fair value, PnL), and keeps the latest valuations plus book alpha/beta in memory. Trades opened against a real provider are marked from *that* provider's quotes only (`provider_pricing_worker`), never mixed with the simulator. IRS is marked from persisted `YIELD_CURVE` rows (`curve_pricing_worker`): FRED/ECB when present, otherwise the simulator curve.
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

## 12. Real Market Data Integration (HW5)

This section covers the `market-data-service-integration` service (port 8005),
the trading ticket, and provider-aware pricing. It is the only place the system
talks to external APIs; all other services consume its unified API/SSE.

### 12.1 Providers and the endpoints we call

Integrations are hand-written HTTP clients (no vendor SDKs). Each provider
client implements `fetch_data` / `provider_error` / `normalize_data`.

**Group A — market data (used to price trades):**
* **Alpha Vantage** — `GLOBAL_QUOTE` function; requires `ALPHA_VANTAGE_API_KEY`; free tier is rate-limited (a handful of requests/min). Returns a single last/price point (no live bid/ask).
* **Finnhub** — `/quote` endpoint; requires `FINNHUB_API_KEY`; returns current/last price fields.
* **Twelve Data** — `/quote` (or time-series) endpoint; requires `TWELVE_DATA_API_KEY`.

**Group B — official reference data (used for curves):**
* **NBP** — public FX tables (PLN pairs); no API key.
* **ECB** — ECB Data Portal FX series (EUR reference rates); no API key.
* **FRED** — `series/observations` for US Treasury yields / rates; requires `FRED_API_KEY`.

### 12.2 Normalized quote format and bid/ask mapping

All Group A responses are normalized to one internal model:

```
{ "provider", "asset_class", "symbol", "bid", "ask", "last",
  "currency", "provider_timestamp", "received_at", "raw_payload" }
```

**Bid/ask decision:** not every provider returns a two-sided quote. When only a
last/close price is available, `bid` and `ask` are left `null` and `last`
carries the price. Pricing then falls back to `last` when the side it needs
(bid for a long, ask for a short) is missing (see §12.5).

### 12.3 Trading ticket (frontend) and staleness policy

The ticket (Generate trade modal) shows one row per provider — **Alpha Vantage,
Finnhub, Twelve Data** — for the selected symbol, each tagged:
* **LIVE** — quote age < 20s.
* **STALE** — older than that.
* **MISSING** — no quote on file for that provider/symbol.

**Policy:** a **MISSING** row cannot be selected, so a trade can never be booked
without a price from the chosen source. A **STALE** row *can* be selected (the
UI shows a warning) — we allow it deliberately because free-tier providers and
closed markets routinely make quotes stale, and blocking would make the ticket
unusable. The trade is still priced against the selected provider.

### 12.4 Trade Action Service (create/close)

`POST /trade-actions` (the ticket target) validates that the chosen provider has
a **fresh** quote for the symbol before accepting (per-asset-class age windows,
shared with pricing — see `shared/trading_shared/market_data.py`), then persists
the trade with `market_data_provider`, `reference_price`,
`reference_price_timestamp`, and `source` (`TRADING_TICKET` vs `GENERATED`).
Every create/close writes an AuditLog. On **CLOSE**, the close price is
re-derived from the trade's own provider (bid for closing a long, ask for a
short), realized PnL is written to `Valuations`, and the closing valuation is
pushed to pricing-service so the realized-PnL event reaches the live stream
(§12.6).

### 12.5 PnL — same source as the trade

Trades opened against a provider are priced **only** from that provider's
quotes; the simulator path explicitly skips any trade with a
`market_data_provider`. Marking is side-aware and mirrors the close logic:

```
long  (BUY):  current_price = bid  (fallback: last)
short (SELL): current_price = ask  (fallback: last)
unrealized_pnl = position(current_price) - position(entry_price)
```

The valuation stream event carries `fair_value`, `current_price`, `entry_price`,
`unrealized_pnl`, `realized_pnl`, `market_data_provider`, `market_data_timestamp`,
plus `is_stale` / `market_data_age_seconds` so consumers can show a STALE mark
instead of trusting an old price as live.

### 12.6 Realized PnL

Closing a position happens in trade-action-service, which computes realized PnL
and writes the closing `Valuation` row to Postgres. Realized PnL is therefore
available through the DB-backed APIs (blotter list/detail, `GET /valuations`),
which is what the UI reads. Note: pricing-service's live `/valuation-stream`
reflects a closed trade on its next cache reload rather than instantly — an
accepted trade-off to keep the services decoupled (no cross-service call on
close).

### 12.7 Market Data Service Integration endpoints (8005)

* **GET /health**, **GET /providers**
* **GET /providers/health** — one live probe of Finnhub, Alpha Vantage and Twelve Data (connect vs quota exceeded). Counts against quote quotas; does not persist quotes.
* **GET /providers/<provider>/health** — same probe for one provider (quote or curve).
* **GET /market-data/snapshot** — latest quote per (provider, symbol) on file.
* **GET /market-data/quotes?symbol=AAPL&asset_class=EQUITY**
* **GET /market-data/quotes/<provider>/<symbol>**
* **GET /market-data/stream**, **GET /market-data/stream/<provider>** (SSE)
* **GET /curves**, **GET /curves/<provider>** — latest persisted curve(s) from Postgres (no live provider call).
* **POST /market-data/refresh** — immediate quote fetch (also accepts GET for smoke tests).
* **POST /curves/refresh** — immediate curve fetch + persist.

### 12.8 API keys / environment

Keys are never hard-coded; set them via `.env` (git-ignored) — see
`env.example`. The root `.gitignore` ignores `.env` / `.env.*` (keeping
`env.example`).

```
ALPHA_VANTAGE_API_KEY=replace_me
FINNHUB_API_KEY=replace_me
TWELVE_DATA_API_KEY=replace_me
FRED_API_KEY=replace_me            # NBP and ECB need no key
MARKET_DATA_POLL_INTERVAL_SECONDS=30
CURVE_DATA_REFRESH_INTERVAL_SECONDS=3600
```

### 12.9 Test commands

```bash
docker compose up --build
curl http://localhost:8005/health
curl http://localhost:8005/providers/health
curl http://localhost:8005/market-data/snapshot
curl "http://localhost:8005/market-data/quotes?symbol=AAPL&asset_class=EQUITY"
curl http://localhost:8005/market-data/quotes/finnhub/AAPL
curl -X POST http://localhost:8005/market-data/refresh
curl -X POST http://localhost:8005/curves/refresh
curl http://localhost:8005/curves
curl http://localhost:8002/valuations
```

### 12.10 Known limitations

* **Provider marking** is side-aware (bid for a long, ask for a short, else last).
  EQUITY, FX and BOND are `price * quantity`. FUTURES multiplies by
  `contract_multiplier` stored on the trade. OPTION is a European Black-Scholes
  premium (r = 0) from that spot plus strike, volatility and maturity on the
  trade. IRS is marked from the yield curve, not from this quote path.
* **Provider coverage:** equity quotes are live. FX, bonds and IRS use persisted
  curves. A European option uses the equity spot of its underlying. A futures
  price is the linked ETF quote (`MARKET_DATA_FUTURES_CONTRACTS`), not an
  exchange futures chain. The ticket blocks a trade when that quote is missing.
* **Repricing latency:** `provider_pricing_worker` reprices every 5s from the
  last persisted quote (DB read, no extra provider calls), so a mark is at most
  ~5s + the poll interval behind the provider; staleness is surfaced via
  `is_stale`. `curve_pricing_worker` similarly reprices IRS every 5s from the
  latest persisted `YIELD_CURVE` per currency (FRED/ECB). Simulator yield
  curves are used only when no real curve exists for that currency.
* **Closed-trade cache eviction** (see §8 Known gap) still applies.

### 12.11 Sample ticket values (per asset class)

Book the trade from **Trade Generation → Generate Trade**. The form follows the
book's `expected_asset_class`. Pick a **LIVE** or **STALE** provider row
(STALE asks for confirmation; MISSING cannot be booked). Prices in the table
come from the provider — do not type them except where the table below says so.

Seeded books: `Global Equities`, `FX`, `Fixed Income`, `Interest Rate Swaps`,
`European Options`, `Index Futures`. Re-run `backend/db/seed_books.sh` if a
book is missing.

| Class | Book | Symbol | Side / direction | Size | Contract terms | Provider row |
| --- | --- | --- | --- | --- | --- | --- |
| EQUITY | Global Equities Book | AAPL | BUY | Qty **100** | — | Finnhub (fallback: Twelve Data / Alpha Vantage). Currency USD from the quote. |
| FX | FX Book | EURUSD | BUY | Qty **100000** | — | NBP only. Last is the NBP mid cross. Currency USD. ECB/FRED stay MISSING. |
| BOND | Fixed Income Book | USD_10Y | BUY | Qty **1** | Size is quantity; `last` is the yield, not a dirty price. | FRED. Last ≈ 4.8–5.0% stored as `0.048`–`0.050`. EUR_10Y → ECB. |
| IRS | Interest Rate Swaps Book | USD_5Y_IRS | Receive fixed (booked as BUY) | Notional **1 000 000 USD** | Fixed rate **4.83%** (UI is percent; payload `0.0483`). Maturity **5**. Payments/year **2**. | FRED last ≈ 4.83%. EUR_10Y_IRS → ECB, notional EUR, maturity **10**, rate ≈ 3.5%. Maturity starts at **0** and must be changed. `trade_price` is always 0. |
| OPTION | European Options Book | AAPL (underlying) | BUY | Qty **1** | CALL. Strike = selected spot (e.g. **335**). Maturity **1**. Volatility **0.20** (20%). | Same equity providers as AAPL. Maturity and vol start at **0** and must be changed. Premium is Black-Scholes (r = 0) and becomes `trade_price` / `option_price`. |
| FUTURES | Index Futures Book | ES | BUY | Qty **1** | Multiplier **50**, underlying **SPY** (from `MARKET_DATA_FUTURES_CONTRACTS`). MES = SPY×5, NQ = QQQ×20. | Same providers as the linked ETF. Notional = price × multiplier × qty. NQ needs QQQ on the equity poll list. |

**Worked payloads** (numbers from a typical quote; swap in the row you clicked):

```
EQUITY   AAPL / BUY / 100 / trade_price = last (e.g. 335.00) / FINNHUB / USD
FX       EURUSD / BUY / 100000 / trade_price = NBP last (e.g. 1.17) / NBP / USD
BOND     USD_10Y / BUY / 1 / trade_price = 0.0496 / FRED / USD
IRS      USD_5Y_IRS / BUY / qty 1 / notional 1000000 / fixed_rate 0.0483
         / maturity_years 5 / payments_per_year 2 / RECEIVE_FIXED_PAY_FLOAT
         / trade_price 0 / FRED / USD
OPTION   AAPL / BUY / 1 / CALL / strike 335 / maturity_years 1 / volatility 0.2
         / spot = exit price / option_price = computed premium / FINNHUB / USD
FUTURES  ES / BUY / 1 / contract_multiplier 50 / underlying_symbol SPY
         / trade_price = SPY last / FINNHUB / USD
```

Coverage of symbols and endpoints: `services/market-data-service-integration/ASSET_CLASS_COVERAGE.md`.