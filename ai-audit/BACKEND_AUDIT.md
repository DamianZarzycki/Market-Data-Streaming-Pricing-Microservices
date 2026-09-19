# Backend Audit — Performance, Concurrency & SQL

**Scope:** `backend/` — 7 Bottle microservices, shared SQLAlchemy layer, Alembic migrations, Compose topology.
**Focus:** SQL performance, transaction boundaries (commit/rollback), lock contention and deadlocks, race conditions.
**Date:** 2026-08-14

---

## Executive summary

The architecture is sound (queue-buffered writes, SSE fan-out, cache-in-front-of-DB), but the data layer has
three structural problems that will dominate everything else under load:

1. **There is not a single secondary index in the schema.** Every filter, sort and aggregate in the codebase
  is a sequential scan. See [SQL-1](#sql-1).
2. **The valuation write path commits one row per transaction**, at roughly 150–170 transactions/second, from a
  single thread. See [SQL-2](#sql-2).
3. **`books-service` shares one module-level `Session` across all requests and never ends the read
  transaction**, which pins the global vacuum horizon and bloats the two fastest-growing tables in the
   system. See [TX-1](#tx-1).

On top of that there are two genuine cross-service lock-contention paths ([LOCK-1](#lock-1),
[LOCK-2](#lock-2)), a pool-exhaustion stall caused by nested sessions ([LOCK-3](#lock-3)), several unguarded
shared-mutable-state races ([RACE-1](#race-1)–[RACE-4](#race-4)), and three correctness bugs that silently cap
or disable functionality ([BUG-1](#bug-1)–[BUG-3](#bug-3)).

### Findings by severity


| ID                | Severity | Area         | Finding                                                          |
| ----------------- | -------- | ------------ | ---------------------------------------------------------------- |
| [SQL-1](#sql-1)   | Critical | Indexes      | Zero secondary indexes; every hot query is a seq scan            |
| [SQL-2](#sql-2)   | Critical | Commits      | One transaction per valuation row (~167 commit/s)                |
| [TX-1](#tx-1)     | Critical | Sessions     | `books-service` shared session, transaction never ended          |
| [BUG-1](#bug-1)   | Critical | Correctness  | `/stream` subscriber shape mismatch kills the market-data thread |
| [LOCK-1](#lock-1) | High     | Locking      | `FOR UPDATE` on `trades` blocks FK locks from valuation inserts  |
| [LOCK-2](#lock-2) | High     | Deadlock     | Unordered bulk `trades` update vs. per-row `FOR UPDATE`          |
| [LOCK-3](#lock-3) | High     | Deadlock     | Nested `Session` for audit writes inside an open transaction     |
| [TX-2](#tx-2)     | High     | Transactions | Blocking HTTP calls (up to ~20 s) inside a DB transaction        |
| [SQL-3](#sql-3)   | High     | Growth       | No retention or partitioning on four append-only tables          |
| [BUG-2](#bug-2)   | High     | Correctness  | Pricing cache silently capped at 50 trades                       |
| [BUG-3](#bug-3)   | High     | Correctness  | `persist_curves` never called → IRS pricing never runs           |
| [RACE-1](#race-1) | High     | Race         | `valuation_cache` mutated and iterated without a lock            |
| [SQL-4](#sql-4)   | Medium   | Queries      | `expire_on_commit` causes an extra SELECT per opened trade       |
| [SQL-5](#sql-5)   | Medium   | Queries      | OFFSET pagination, `COUNT(*)` instead of `EXISTS`, N+1 updates   |
| [TX-3](#tx-3)     | Medium   | Pooling      | Default pool sizing risks exceeding Postgres `max_connections`   |
| [TX-4](#tx-4)     | Medium   | Resilience   | No `pool_pre_ping` / `pool_recycle` / statement or lock timeouts |
| [RACE-2](#race-2) | Medium   | Race         | Idempotency check is TOCTOU (`FOR UPDATE` locks nothing)         |
| [RACE-3](#race-3) | Medium   | Race         | `/start` on trade-generation can spawn duplicate workers         |
| [RACE-4](#race-4) | Medium   | Race         | Book name uniqueness check is TOCTOU → 500 instead of 409        |
| [BUG-4](#bug-4)   | Medium   | Correctness  | `move_trades_to_new_book` receives an asset class as `book_id`   |
| [PERF-1](#perf-1) | Medium   | Backpressure | Unbounded SSE queues; no backpressure between services           |
| [PERF-2](#perf-2) | Medium   | Contention   | Locks held across full fan-out loops                             |
| [PERF-3](#perf-3) | Low      | Logging      | Per-row `INFO` logging on the hot path                           |
| [PERF-4](#perf-4) | Low      | Errors       | Batch buffer discarded wholesale on any failure                  |
| [OPS-1](#ops-1)   | Low      | Deps         | Unpinned dependencies                                            |


---



## Load model used throughout

From `backend/docker-compose.yml`, `TICK_INTERVAL_MS=300`, and `generate_market_tick()` emits 8 instrument
entries per tick (6 spot + 2 curves).


| Path                                     | Rate                              | Rows/day    |
| ---------------------------------------- | --------------------------------- | ----------- |
| `market_data_spot_prices` inserts        | ~20 rows/s (batched 30 per txn)   | ~1.7 M      |
| `valuations` inserts                     | up to ~167 rows/s, **1 txn each** | up to ~14 M |
| `audit_logs` inserts                     | 2–4 per trade action, 1 txn each  | ~10⁵–10⁶    |
| `trades` seq scan (pricing cache reload) | every 5 s                         | —           |
| `trades` seq scan (trade generation)     | every 4 s                         | —           |


Two independent tables therefore grow by millions of rows per day, and every query against them is a
sequential scan (see below). This is the compounding factor behind most findings.

---



## Critical



### SQL-1 · No secondary indexes exist anywhere in the schema

**Where:** `backend/db/migrations/versions/0c64ce129af6_initial_schema.py`,
`040aaeb85cd4_instruments_table_added.py`, `662d91ed6853_...py`

The only indexes created by the migrations are primary keys plus two unique constraints
(`books.name`, `trades.client_request_id`). Alembic autogenerate produced the tables verbatim from the models,
and the models declare no `index=True` and no `__table_args__`. PostgreSQL does **not** automatically index
foreign key columns, so `trades.book_id`, `trades.instrument_id`, `valuations.trade_id` and
`valuations.book_id` are all unindexed.

Every hot query in the codebase is consequently a sequential scan, and several add a full sort on top:


| Query                                                                 | Location                                                                          | Missing index                             |
| --------------------------------------------------------------------- | --------------------------------------------------------------------------------- | ----------------------------------------- |
| `WHERE status = 'ACTIVE' ORDER BY created_at DESC`                    | `repositories.py:34` — called every 5 s by pricing, every 4 s by trade generation | `(status, created_at DESC)`               |
| `WHERE trade_id = ? ORDER BY valuation_time DESC`                     | `repositories.py:76` — blotter trade detail                                       | `(trade_id, valuation_time DESC)`         |
| `WHERE asset_class = ? AND symbol = ?`                                | `repositories.py:106`                                                             | `(asset_class, symbol)`                   |
| `WHERE currency = ? AND curve_type = ?`                               | `repositories.py:117`                                                             | `(currency, curve_type, event_time DESC)` |
| `WHERE symbol = ?`                                                    | `repositories.py:91`                                                              | `instruments(symbol)`                     |
| `SELECT book_id, count(*) ... WHERE status='ACTIVE' GROUP BY book_id` | `blotter-service/trades_service.py:62`                                            | `(status, book_id)`                       |
| `WHERE book_id = ? AND status = 'ACTIVE'`                             | `books_manager_service.py:142,152`                                                | `(book_id, status)`                       |
| `WHERE is_active = ?`                                                 | `books_manager_service.py:13`                                                     | `books(is_active)`                        |


The `valuations` and `market_data_spot_prices` scans are the dangerous ones: both tables grow by millions of
rows per day ([SQL-3](#sql-3)), so a query that is instant on an empty database degrades linearly and without
warning. The `MarketDataRepository.get_curve()` / `get_spot_price()` calls are worse than a plain scan —
they use `.first()` with **no** `ORDER BY`, so on an append-only table they return an arbitrary row, not the
latest one. That is a latent correctness bug as well as a performance one.

**Recommendation:** add the indexes in [Appendix A](#appendix-a-index-ddl) as a new Alembic revision, using
`CREATE INDEX CONCURRENTLY` (which requires `op.execute` with autocommit, since Alembic wraps migrations in a
transaction by default). Add explicit `ORDER BY event_time DESC` to the two market-data lookups.

---



### SQL-2 · One transaction per valuation row

**Where:** `backend/services/pricing-service/valuation_service.py:9-34`, called from
`calculation_service.py:185` (`store_and_publish`), which is called once per trade per tick.

```python
def save_valuation(trade, valuation_data):
    with DBSessionManager() as db:     # new Session → new pooled connection
        ...
        db.valuations.add(valuation)
        db.commit()                    # one COMMIT per row
```

Every single valuation is a full round trip: connection checkout, `BEGIN`, one `INSERT`, `COMMIT`, connection
return. At the measured rate (up to ~167 valuations/s) that is ~167 transactions/second from **one** thread,
serialised. Each commit is a separate WAL flush (`fsync`), which is the hard ceiling here — the pricing worker
cannot go faster than the disk can flush, and because it is the same thread that reads the market-data SSE
stream, every millisecond spent committing is added lag on the tick stream ([PERF-1](#perf-1)).

Three separate costs are being paid per row: transaction overhead, WAL flush, and connection churn.

**Recommendation:** batch valuations the way `market-data-service` already batches spot prices — accumulate into
a queue, and have a dedicated writer thread flush with `add_all()` + a single `commit()` every N rows or M
milliseconds. This turns ~167 transactions/s into ~3–5. If ordering per trade must be preserved, key the
buffer by `trade_id`. For an even larger win, `session.bulk_save_objects()` or a Core `insert()` with a list of
dicts avoids ORM instance overhead entirely for what is a pure append.

The same pattern applies to `AuditLogger.write()` (`shared/trading_shared/audit.py:31-33`): every audit line
opens its own session and commits, so a single trade action costs 2–4 extra transactions on top of the
business transaction. Audit writes should go through the same fire-and-forget buffered writer.

---



### TX-1 · `books-service` shares one Session process-wide and never ends the read transaction

**Where:** `backend/services/books-service/app.py:14`

```python
db = SessionLocal()   # module level, one Session for the entire process lifetime
```

Every route uses this one object. Three distinct problems:

**1. The read transaction is never ended.** `GET /books` and `GET /books/<id>` issue a query — which implicitly
opens a transaction — and then return. There is no `commit()`, no `rollback()`, and no `close()` on those
paths. The connection therefore sits in state `idle in transaction` indefinitely. `update_book` and
`delete_book` have the same gap on their failure-free paths after `db.commit()` returns (the next query starts
a fresh transaction that is again never ended); only `create_new_books` has a `finally: db.close()`.

This is the most damaging finding in the audit, because an open transaction holds back the global xmin
horizon: **autovacuum cannot reclaim dead tuples in *any* table while it is open.** Given that `valuations`
and `market_data_spot_prices` are taking millions of writes per day ([SQL-3](#sql-3)), the result is unbounded
table and index bloat across the whole database, progressively slowing every service — not just books.

**2.** `Session` **is not thread-safe.** The service runs `app.run(host, port)` with no `server=` argument, so it
currently uses the default single-threaded `wsgiref` server and requests are serialised. That accidentally
masks the bug. The moment `books-service` gets the same `ThreadedServer` treatment as its six siblings —
which is the obvious next fix, since it is currently a serialisation bottleneck for both
`trade-generation-service` (polls `/books` every 4 s) and `blotter-service` (`/books/summary` with a 2 s
timeout) — concurrent requests will interleave on one `Session` and one connection, producing corrupted
identity-map state and `InvalidRequestError` / partial-commit failures.

**3. Rollback hygiene is inconsistent.** `update_book` and `delete_book` roll back on `ValueError` but not on
any other exception, so an `IntegrityError` leaves the shared session permanently poisoned — every subsequent
request on that session fails with `PendingRollbackError` until the process restarts.

**Recommendation:** delete the module-level session. Use a per-request session with guaranteed teardown, e.g. a
Bottle plugin or an explicit `with DBSessionManager() as db:` per route, matching the pattern the other six
services already use. Then switch the service to `ThreadedServer`. As a defence in depth, set
`idle_in_transaction_session_timeout` on the database role ([TX-4](#tx-4)) so this class of bug cannot silently
degrade the entire cluster again.

---



### BUG-1 · SSE subscriber shape mismatch kills the market-data generator thread

**Where:** `backend/services/market-data-service/app.py:53` vs. `worker.py:209-219`

The route appends a bare `queue.Queue`:

```python
client_queue = queue.Queue()
with worker.data_lock:
    worker.subscribers.append(client_queue)
```

but the publisher indexes subscribers as dicts (matching the documented shape in `worker.py:24`):

```python
subscriber_ids = [subscriber["id"] for subscriber in subscribers]   # TypeError on a Queue
```

As soon as any client connects to `/stream`, the next `publish_tick_to_stream()` raises `TypeError`.
`market_worker()` (`worker.py:288-300`) has **no try/except around its loop**, so the exception terminates the
thread. Tick generation, stream publishing and DB queueing all stop permanently, and because the thread is a
daemon the process stays up and continues reporting `status: UP` on `/health` — a silent total outage of the
system's data source.

**Recommendation:** align the two shapes (register the documented dict, or index by attribute), wrap the
`market_worker` loop body in try/except so a publish failure cannot kill generation, and have `/health` report
worker liveness (`thread.is_alive()`) so this failure mode is visible.

---



## High



### LOCK-1 · `SELECT ... FOR UPDATE` on `trades` blocks foreign-key locks from valuation inserts

**Where:** `backend/services/trade-action-service/worker.py:171-176` and
`shared/trading_shared/repositories.py:52-54`

The close path takes an explicit row lock:

```python
trade_to_close = (
    db.session.query(Trade).filter_by(trade_id=trade_id_to_close)
    .with_for_update().first()
)
```

In PostgreSQL, inserting a child row takes a `FOR KEY SHARE` lock on the referenced parent row to enforce
referential integrity. `valuations` has FKs to both `trades` and `books`, so *every* valuation insert from
`pricing-service` acquires `FOR KEY SHARE` on its `trades` row (and on its `books` row).

`FOR UPDATE` **conflicts** with `FOR KEY SHARE`. So while `trade-action-service` holds `FOR UPDATE` on a trade,
`pricing-service`'s insert for that same trade blocks. That would be brief — except the lock is held across
**two nested audit writes** (`worker.py:162-168` before the lock, and the post-commit ones after), each of
which opens a separate session and commits ([LOCK-3](#lock-3)). Lock hold time becomes network + fsync latency
rather than microseconds, and it blocks the single-threaded pricing writer, which stalls the entire valuation
pipeline behind one trade close.

Note that a plain `UPDATE` that does not touch a key column takes only `FOR NO KEY UPDATE`, which **does not**
conflict with `FOR KEY SHARE`. The explicit `FOR UPDATE` is what creates the contention, and it is stronger
than this code needs — `status`, `close_price`, `close_reason` and `closed_at` are all non-key columns.

**Recommendation:** use `with_for_update(key_share=True)`, which emits `FOR NO KEY UPDATE` on PostgreSQL and
still serialises concurrent closers while no longer blocking FK locks from valuation inserts. Additionally,
move every audit write outside the locked region — emit them after `db.commit()`, or buffer them
([SQL-2](#sql-2)) — so the lock is held for exactly the two statements that need it. Consider
`with_for_update(key_share=True, nowait=True)` (or `skip_locked`) so a contended close fails fast into the
existing retry/error path instead of queueing.

---



### LOCK-2 · Unordered bulk update of `trades` can deadlock against per-row `FOR UPDATE`

**Where:** `backend/services/books-service/books_manager_service.py:86-97`

```python
def move_trades_to_new_book(db, book_id, new_book_id):
    trades = db.query(Trade).filter(Trade.book_id == book_id).all()
    for trade in trades:
        trade.book_id = new_book_id
```

This loads every trade of a book and dirties them, so the flush issues one `UPDATE` per row **in
identity-map order with no** `ORDER BY` — the lock acquisition order is planner-dependent and unstable. Three
consequences:

- **Deadlock against** `trade-action-service`**:** that service locks a single trade row and then inserts a
valuation (taking a `FOR KEY SHARE` lock on the `books` row); `update_book` holds a `FOR NO KEY UPDATE` lock
on the `books` row and then walks the trade rows. The two acquire the same pair of resources in opposite
order, which is textbook `40P01 deadlock detected`.
- **Deadlock against a second concurrent** `update_book`**:** with no `ORDER BY`, two invocations touching
overlapping trade sets can grab rows in different sequences.
- **Long transaction:** all rows are locked from first `UPDATE` until the request's `commit()`, blocking
valuation inserts for every trade in the book ([LOCK-1](#lock-1)).

`update_book` also performs this whole reparenting inside the request thread with no batching or timeout.

**Recommendation:** replace the ORM loop with a single ordered, set-based statement —
`update(Trade).where(Trade.book_id == book_id).values(book_id=new_book_id)` — which is one statement instead of
N and acquires locks in a single scan. Establish a documented, global lock ordering (books before trades before
valuations) and follow it in every writer. Set a `lock_timeout` ([TX-4](#tx-4)) so deadlock victims fail fast,
and add a bounded retry on `40P01` (the existing `shared/trading_shared/decorators.py` `retry` is a good fit,
though it currently retries on *any* exception, which is too broad for DB work).

---



### LOCK-3 · Nested `Session` for audit writes inside an open transaction → pool-exhaustion stall

**Where:** `shared/trading_shared/audit.py:31-33`, called from inside `with DBSessionManager()` blocks
throughout — most notably `trade-action-service/worker.py:35-41, 54-59, 151-156, 162-168`.

`AuditLogger.write()` unconditionally opens its **own** `DBSessionManager` and commits. When called from
inside an already-open business transaction, the calling thread holds **two** pooled connections
simultaneously, and the second checkout happens while the first transaction holds row locks.

With SQLAlchemy's default `QueuePool` (`pool_size=5`, `max_overflow=10` → 15 connections, `pool_timeout=30`),
this is the classic nested-checkout starvation pattern: if 15 threads each hold one connection in an open
transaction and each then requests a second connection for an audit write, none can proceed and all block for
30 s before raising `TimeoutError`. It is not a database deadlock — Postgres sees only idle transactions — so
`pg_locks` shows nothing and it is correspondingly hard to diagnose.

This is currently latent in `trade-action-service` because the queue is drained by a single worker thread
(`worker.py:15-23`). It is **live today** in `trade-generation-service`, whose `/generate-once` and
`/generate-batch` routes run on `ThreadedServer` and call `generate_random_intention()`, which holds a session
across blocking HTTP calls ([TX-2](#tx-2)). Adding a second consumer thread to `trade-action-service` — the
natural response to queue backpressure — would immediately make it live there too.

**Recommendation:** never open a nested session. Either pass the caller's session into the audit logger so the
audit row joins the business transaction (correct for audit semantics: the audit trail should not record work
that rolled back), or push audit entries onto an in-memory queue drained by one dedicated writer thread
(better for throughput, and fixes [SQL-2](#sql-2) too). Whichever is chosen, size the pool explicitly rather
than relying on defaults ([TX-3](#tx-3)).

Note also that audit writes in `except` blocks (`worker.py:293-298`) can themselves raise — if the pool is
exhausted, the audit call inside the error handler throws, replacing the original exception and skipping
`metrics.record_error()`. Error handling should never be able to fail louder than the error it handles.

---



### TX-2 · Blocking HTTP calls inside an open DB transaction

**Where:** `backend/services/trade-generation-service/trade_intentions_service.py:227-331`

The entire body of `generate_random_intention()` — including two outbound HTTP calls — sits inside
`with DBSessionManager() as db:`:

```python
with DBSessionManager() as db:
    ...
    active_trades = db.trades.get_trades(...)          # opens a transaction
    ...
    response = fetch_json("http://books-service:8004/books")        # 5 s timeout, @retry(3)
    symbols_response = fetch_json("http://market-data-service:8001/symbols")
```

`fetch_json` is decorated with `@retry(max_attempts=3)` and a 5 s timeout, with exponential backoff (1 s, 2 s).
Worst case is roughly `3 × 5 s + 1 s + 2 s ≈ 18 s` **per call**, up to ~36 s for both — all while holding a
pooled connection with an open transaction.

The `CLOSE_TRADE`-falls-through-to-`OPEN` path is the one that bites: the `get_trades` query has already opened
the transaction before the HTTP calls run. Combined with `books-service` being single-threaded
([TX-1](#tx-1)), one slow books request pins a connection and the vacuum horizon for tens of seconds. On
`ThreadedServer`, concurrent `/generate-batch` requests multiply this by the number of in-flight requests and
exhaust the pool.

**Recommendation:** do all I/O outside the transaction. Read what is needed from the database, close the
session, then make the HTTP calls, then open a new short session if a write is required. Reduce the HTTP
timeout and drop `@retry` on a call already on a periodic loop — retrying a 4-second-cadence generator is
redundant and only extends lock hold time.

---



### SQL-3 · Four append-only tables with no retention, partitioning, or archival

**Where:** models `MarketDataSpotPrice`, `Valuation`, `AuditLog`, `MarketDataSnapshot`


| Table                     | Insert rate                            | Rows/day    | Ever deleted? |
| ------------------------- | -------------------------------------- | ----------- | ------------- |
| `market_data_spot_prices` | ~20/s                                  | ~1.7 M      | No            |
| `valuations`              | up to ~167/s                           | up to ~14 M | No            |
| `audit_logs`              | 2–4 per trade action                   | ~10⁵–10⁶    | No            |
| `market_data_snapshots`   | (writer never called, [BUG-3](#bug-3)) | 0           | No            |


Every row also carries a `JSONB` `raw_payload` / `valuation_payload` duplicating the whole tick, which
inflates row size and pushes wide values into TOAST storage. Within days these tables reach tens of millions of
rows — at which point the missing indexes ([SQL-1](#sql-1)) turn every blotter query into a multi-second scan,
and the never-ending `books-service` transaction ([TX-1](#tx-1)) prevents vacuum from reclaiming anything.

A comment in `market-data-service/worker.py:305-308` records that this class of problem has already caused a
production incident: *"Querying MarketDataSpotPrice.all() grows without bound and exhausted the SQLAlchemy
pool (blocking trade-generation OPEN intents)."* The workaround removed one query; the underlying growth is
unaddressed.

**Recommendation:** declarative range partitioning by day or week on `event_time` / `valuation_time` /
`created_at`, with a scheduled `DROP`/`DETACH` of old partitions — dropping a partition is instant and
generates no vacuum work, unlike `DELETE`. If partitioning is too heavy for this stage, a periodic
`DELETE ... WHERE created_at < now() - interval '7 days'` in batches is a reasonable stopgap. Separately,
consider whether the full `raw_payload` needs persisting on every row, and add a `latest valuation per trade`
projection table so the blotter never has to sort history at read time.

---



### BUG-2 · Pricing cache silently capped at 50 trades

**Where:** `backend/services/pricing-service/cache_service.py:15` →
`shared/trading_shared/repositories.py:34-63`

```python
trades = db.trades.get_trades(client_request_id=None, status=ACTIVE, symbol=None, first_only=False)
```

`get_trades` defaults to `page=1, limit=50`, so this emits
`ORDER BY created_at DESC OFFSET 0 LIMIT 50`. **The pricing service only ever prices the 50 most recently
created active trades.** Every older active trade stops being valued, silently — no error, no warning, and
`/health` still reports the service as pricing normally. In a P&L system this is a correctness failure, not
just a performance one.

The same default silently caps `trade-generation-service` (`trade_intentions_service.py:234`), so
`random.choice(active_trades)` only ever closes one of the 50 newest trades — older trades are never selected
and accumulate forever, which in turn feeds the growth in [SQL-3](#sql-3).

`TradeRepository.get_active_trades()` (`repositories.py:27-32`) is an unpaginated variant that would return
everything, but it is dead code — nothing calls it.

**Recommendation:** give internal callers an explicit paging contract instead of inheriting an API default
meant for a UI list. For the pricing cache, iterate in keyset order (`WHERE created_at < ?` on the last seen
value) rather than loading everything at once, so cache reload stays bounded as the book grows. Make `limit`
required, or at minimum have the cache log a warning when it fills exactly to the limit.

---



### BUG-3 · `persist_curves` is never called → curve table stays empty → IRS pricing never runs

**Where:** `backend/services/market-data-service/worker.py:222-258` (`persist_curves`), `261-285`
(`persist_full_snapshot`), `288-300` (`market_worker`)

`market_worker` does exactly four things per tick: generate, update snapshot, publish to stream, put on
`db_queue`. Neither `persist_curves` nor `persist_full_snapshot` is called from anywhere in the codebase
(verified by grep). Meanwhile `db_worker` explicitly **skips** curve entries (`worker.py:336-337`, `if "curve_type" in data: continue`) on the documented assumption that `persist_curves` handles them
synchronously.

So `market_data_curves` is never written. Downstream, `calculation_service.update_curve_and_reprice_irs()`
(`calculation_service.py:259-266`) calls `db.market_data.get_curve(...)`, always gets `None`, logs
*"No … curve in MarketDataCurve yet; skipping IRS repricing"* and returns. **No IRS trade is ever priced** —
yet a DB round trip is still paid on every curve tick (2 per 300 ms ≈ 6.7 wasted queries/s against an empty
table) to discover this.

`market_worker` also declares `global _ticks_since_snapshot` (`worker.py:289`), a name that is never assigned
anywhere — leftover from a removed periodic-snapshot mechanism, and the reason `market_data_snapshots` is
always empty.

**Recommendation:** call `persist_curves(new_ticks)` in `market_worker` before `publish_tick_to_stream` (the
ordering its docstring already specifies, so pricing can read the curve immediately), or route curves through
`db_queue` and drop the skip. Reinstate or delete the snapshot path and the dead `global`. Guard the curve
lookup so a missing curve does not cost a query per tick — and once curves are persisted, add the
`(currency, curve_type, event_time DESC)` index and an explicit `ORDER BY` ([SQL-1](#sql-1)), since
`get_curve()` currently returns an arbitrary historical curve rather than the current one.

---



### RACE-1 · `valuation_cache` is mutated and iterated with no synchronisation

**Where:** `backend/services/blotter-service/cache/valuation_cache_service.py` (whole module),
`trades_service.py:20-22, 76-86`

The module-level `valuation_cache` dict has **no lock**. The SSE worker thread writes to it
(`update_cache` → `history.append(...)`, `del history[:-MAX]`) while request threads — the service runs on
`ThreadedServer` — iterate it:

```python
# trades_service.py:76  (GET /books/summary)
for history in valuation_cache_service.valuation_cache.values():
    ...
    latest = history[-1]

# trades_service.py:21  (GET /health)
"cache_entry_count": sum(len(v) for v in cache.values()),
```

If the worker inserts a new `trade_id` key while a request thread is mid-iteration, that thread raises
`RuntimeError: dictionary changed size during iteration` — a 500 on `/books/summary` and `/health`, at a
frequency that scales with tick rate and trade count. `history[-1]` can also raise `IndexError` if the
truncation in `update_cache` interleaves between the emptiness check and the index.

`GET /health` returning 500 has an amplifying effect: `monitoring-service` probes it every second
(`monitoring-service/worker.py:378`) and will report `blotter-service` as DOWN, incrementing the error KPI.

**Recommendation:** guard all access with a single module-level `threading.Lock`, and have readers copy under
the lock (`list(cache.items())`) before doing any work. Storing a per-trade `collections.deque(maxlen=N)`
would also make the truncation atomic and remove the `del history[:-N]` slice entirely.

---



## Medium



### SQL-4 · `expire_on_commit=True` causes an extra SELECT per opened trade

**Where:** `shared/trading_shared/db.py:10-14`; visible at `trade-action-service/worker.py:265-274`

`sessionmaker` is configured with `autocommit=False, autoflush=False` but leaves `expire_on_commit` at its
default `True`. Every attribute of every instance is therefore invalidated on commit, and the next access
re-issues a `SELECT`:

```python
db.commit()
...
f"Trade opened: {new_trade.trade_id}",     # triggers a refresh SELECT
entity_id=str(new_trade.trade_id),         # (already loaded by then)
```

That is one extra round trip per opened trade, purely to log an ID the process just generated. The pattern
recurs wherever an object is read after commit.

**Recommendation:** set `expire_on_commit=False`. The codebase already treats post-commit objects as
detached snapshots (see `cache_service.reload_active_trades_cache`, which closes the session and keeps using
the instances), so this matches actual usage and removes a class of surprise `SELECT`s — and of
`DetachedInstanceError` if an attribute is ever touched after teardown.

Separately, `autoflush=False` deserves a comment beyond the current one: it means a query issued after
`session.add()` will **not** see the pending row. `trade-action-service/worker.py:76` correctly calls
`db.flush()` after adding an instrument for exactly this reason, but the requirement is implicit and easy to
miss in new code.

---



### SQL-5 · Query anti-patterns: OFFSET paging, `COUNT(*)` for existence, N+1 updates

**Where:** `repositories.py:55-63`, `books_manager_service.py:142-158`, `books_manager_service.py:86-97`

1. `OFFSET`**-based pagination** (`repositories.py:58-62`). PostgreSQL must generate and discard every skipped
  row, so page 200 scans 10 000 rows before returning 50 — and with no index on `(status, created_at)` the
   whole filtered set is sorted first. Use keyset pagination (`WHERE created_at < :last_seen ORDER BY  created_at DESC LIMIT :n`).
2. `count_active_trades()` **used only as a boolean** (`books_manager_service.py:166-171`). `delete_book`
  calls it just to compare `> 0`, so a full aggregate is computed where an `EXISTS` (short-circuiting on the
   first hit) would do. `has_active_trades()` right above it already does this correctly — call it.
3. **N+1 updates in** `move_trades_to_new_book` (`books_manager_service.py:86-97`). Loads all trades and emits
  one `UPDATE` per row. See [LOCK-2](#lock-2) for the set-based replacement.
4. `add_all()` **on the market-data batch** (`repositories.py:127-129`). Fine as far as it goes, but the ORM
  path pays instance-construction overhead for a pure append. A Core `insert()` with a list of dicts is
   markedly faster for the ~20 rows/s spot feed.
5. `.first()` **without** `ORDER BY` on append-only tables (`repositories.py:106-125`) — see
  [SQL-1](#sql-1); this is a correctness bug as much as a performance one.

---



### TX-3 · Default pool sizing can exceed Postgres `max_connections`

**Where:** `shared/trading_shared/db.py:8`, `backend/docker-compose.yml`

```python
engine = create_engine(DATABASE_URL)   # pool_size=5, max_overflow=10 → 15 per process
```

Six services open a DB connection (`market-data`, `pricing`, `books`, `blotter`, `trade-generation`,
`trade-action`), each with its own engine and its own pool. Worst case is `6 × 15 = 90` connections, plus the
migrations container, plus any `psql` session — against `postgres:15`'s default `max_connections = 100`. The
headroom is thin enough that a burst produces `FATAL: sorry, too many clients already`, and each idle Postgres
backend costs several MB of RAM regardless.

Nested checkouts ([LOCK-3](#lock-3)) double the per-thread requirement, and `DATABASE_URL` carries no
`?application_name=`, so `pg_stat_activity` cannot attribute a connection to a service — which makes exactly
this problem hard to diagnose.

**Recommendation:** size pools explicitly per service based on its actual concurrency (a single-worker service
needs 2–3, not 15), set `max_connections` deliberately rather than relying on the image default, and add
`?application_name=<service>` to every DSN. Longer term, PgBouncer in transaction-pooling mode decouples
service count from backend count entirely.

---



### TX-4 · No connection validation, recycling, or server-side timeouts

**Where:** `shared/trading_shared/db.py:8`

The engine is created with no resilience options:

- **No** `pool_pre_ping=True` — after a Postgres restart or an idle-timeout kill, every pooled connection is
stale and the next checkout fails with `OperationalError` until the pool churns through them all. Compose
gates startup on the migrations container, so this shows up on *restart*, not first boot.
- **No** `pool_recycle` — connections live indefinitely and can be silently dropped by an intermediary.
- **No** `statement_timeout` — one pathological query (a seq scan over 14 M unindexed `valuations` rows,
[SQL-1](#sql-1)) runs until it finishes, holding its connection.
- **No** `lock_timeout` — a blocked writer waits forever instead of failing into the retry path
([LOCK-1](#lock-1), [LOCK-2](#lock-2)).
- **No** `idle_in_transaction_session_timeout` — nothing bounds the damage from [TX-1](#tx-1).

**Recommendation:**

```python
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=..., max_overflow=...,          # per-service, see TX-3
    connect_args={
        "application_name": SERVICE_NAME,      # see TX-3
        "options": "-c statement_timeout=5000 "
                   "-c lock_timeout=2000 "
                   "-c idle_in_transaction_session_timeout=10000",
    },
)
```

Tune `statement_timeout` per workload — analytical endpoints may legitimately need longer than 5 s.

---



### RACE-2 · Idempotency check is TOCTOU; `FOR UPDATE` cannot lock a non-existent row

**Where:** `trade-action-service/worker.py:44-63`, via `repositories.py:52-54`

The duplicate-`OPEN_TRADE` guard does `SELECT ... WHERE client_request_id = ? FOR UPDATE LIMIT 1`. `FOR UPDATE` **locks nothing when no row matches** — there is no gap locking in PostgreSQL — so two concurrent
handlers for the same `client_request_id` both see "no duplicate" and both insert. One then fails on the
`trades.client_request_id` unique constraint, surfacing as a generic exception and an `ERROR` metric rather
than the intended clean "duplicate ignored" outcome.

The `CLOSE_TRADE` guard (`worker.py:139-160`) is weaker still: it reads
`trade.metadata_payload["close_client_request_id"]` **without any lock**, and nothing in the close path ever
*writes* that key — so the check can never fire. Close idempotency rests entirely on the
`status != "ACTIVE"` check at `worker.py:196`, which is at least performed under `FOR UPDATE` and so is
sound, if accidentally.

Currently masked because a single worker thread drains the queue; becomes live the moment a second consumer is
added.

**Recommendation:** make idempotency a database guarantee rather than a check-then-act: `INSERT ... ON CONFLICT (client_request_id) DO NOTHING` and treat "zero rows affected" as the duplicate case. That is atomic,
lock-free, and correct at any concurrency. Either implement the `close_client_request_id` write the close guard
is looking for, or delete the dead check and document that `status` is the idempotency key.

---



### RACE-3 · `/start` can spawn duplicate generator threads

**Where:** `trade-generation-service/app.py:51-64`

```python
if trade_intentions_service.generator_state["is_running"]:
    return {"message": "Generator is already running!"}
trade_intentions_service.generator_state["is_running"] = True
generator_state["thread"] = threading.Thread(target=worker.trade_generation_worker)
```

Check and set are not atomic, and the service runs on `ThreadedServer`. Two concurrent `POST /start` calls can
both pass the guard and start two workers, doubling generation rate against the single-threaded
`trade-action-service` queue while `generator_state["thread"]` retains only the second — the first becomes
unstoppable and unobservable. `/stop` flips one shared flag, so it would terminate both, but only after each
finishes its current iteration.

**Recommendation:** guard the state transition with a `threading.Lock`, and use a `threading.Event` for the
run flag instead of a plain dict entry (the `TODO` at `trade_intentions_service.py:12` already anticipates
this). An `Event` also lets the worker's `time.sleep(interval)` become `event.wait(interval)`, so `/stop`
takes effect immediately rather than after up to a full interval — the concern raised in the `TODO` at
`app.py:73`.

---



### RACE-4 · Book-name uniqueness check is TOCTOU

**Where:** `books_manager_service.py:62-64`

```python
existing_book = db.query(Book).filter(Book.name == name).first()
if existing_book:
    raise ValueError(f"Book with name '{name}' already exists")
```

Classic check-then-act. Two concurrent creates with the same name both pass, and the second fails on the
`books.name` unique constraint with `IntegrityError` — which `app.py` does not catch as `ValueError`, so the
caller gets a **500 instead of the intended 409**. Also, the pre-check costs a query per book that the unique
index would enforce for free. Currently masked by the single-threaded server ([TX-1](#tx-1)).

**Recommendation:** drop the pre-check and let the constraint do its job, catching `IntegrityError` and mapping
it to 409. Note the batch path builds up `created_books` across the loop and calls `db.commit()` once at the
end — correct all-or-nothing semantics, worth preserving.

---



### BUG-4 · `move_trades_to_new_book` is passed an asset class where a `book_id` is expected

**Where:** `books_manager_service.py:120-123`

```python
if "expected_asset_class" in data and data["expected_asset_class"]:
    active_trades_present = has_active_trades(db, book_id)
    if active_trades_present:
        move_trades_to_new_book(db, book_id, data["expected_asset_class"])   # <-- asset class, not a book_id
```

The third parameter is named `new_book_id` and is assigned to `trade.book_id`, but the caller passes an asset
class string such as `"EQUITY"`. At flush this fails on UUID coercion, or — if it coerced — on the
`trades.book_id → books.book_id` foreign key. Either way `PUT /books/<id>` returns 500 for any book that has
active trades and an `expected_asset_class` change, and the transaction is left needing a rollback that
`update_book`'s handler only performs for `ValueError` ([TX-1](#tx-1), point 3).

The intended behaviour is also ambiguous: silently reparenting existing trades on an asset-class change is a
surprising side effect for a `PUT`.

**Recommendation:** decide the intended semantics — most likely reject the asset-class change while active
trades exist (a 409, consistent with `delete_book`) — and if reparenting really is wanted, take an explicit
target `book_id` in the request body and validate it exists first.

---



### PERF-1 · Unbounded SSE queues and no backpressure between services

**Where:** `market-data-service/app.py:50` and `worker.py:209-219`; `pricing-service/app.py:77` and
`calculation_service.py:197-199`

Every subscriber gets an unbounded `queue.Queue()`, and producers `put()` without limit. The producer/consumer
rates are structurally mismatched:

- `market-data-service` publishes 8 ticks every 300 ms unconditionally.
- `pricing-service` consumes them on **one** thread that, per tick, reprices up to 50 trades and performs one
synchronous `INSERT` + `COMMIT` each ([SQL-2](#sql-2)) — far slower than 300 ms.

The market-data subscriber queue therefore grows without bound: memory climbs until the container is OOM-killed,
and valuation latency grows without limit long before that. Nothing measures the lag, so the only symptom is
stale prices and rising RSS. `pricing → blotter` has the same shape.

**Recommendation:** bound the queues (`maxsize`) and choose an explicit policy on overflow — for market data,
dropping the oldest tick is almost always right, since a stale price has no value and conflation is the
standard approach for a tick feed. Export queue depth on `/health` so lag is observable;
`trade-action-service`'s `metrics.build_status()` already does exactly this and is a good model. Fixing
[SQL-2](#sql-2) removes most of the rate mismatch at the source.

---



### PERF-2 · Locks held across full fan-out loops

**Where:** `market-data-service/worker.py:210-215`, `pricing-service/calculation_service.py:197-199`,
`books-service` snapshot paths

`publish_tick_to_stream` holds `data_lock` while building the subscriber ID list, JSON-serialising all 8 ticks,
**and** pushing to every subscriber queue. `data_lock` also guards `/health`, `/snapshot`, `/symbols` and
`update_snapshot`, so all of them contend with the fan-out 3.3 times per second. Because the queues are
unbounded the `put()` calls do not block, so this is contention rather than deadlock — but it is avoidable, and
it would become a deadlock the moment queues are bounded ([PERF-1](#perf-1)), since a blocking `put()` while
holding `data_lock` would freeze the service.

Lock ordering across the codebase is otherwise clean — `store_and_publish` releases `pricing_lock` before
taking `sse_subscribers_lock`, and `on_benchmark_tick` releases `pricing_lock` (inside
`snapshot_book_values`) before taking `book_metrics_cache_lock` — so there is no lock-order inversion today.
Worth preserving deliberately as the code grows.

**Recommendation:** serialise payloads and copy the subscriber list under the lock, then release it before
pushing to queues. Keep every future `put()` outside the lock, especially once queues become bounded.

---



### PERF-3 · Per-row `INFO` logging on the hot path

**Where:** `calculation_service.py:180-196`, `valuation_service.py:14`, `blotter-service/worker.py:31-37`,
`trade-action-service/worker.py:20, 68-71`

Several `logging.info` calls fire once per priced trade or per SSE message — hundreds of formatted lines per
second at the measured rate, each doing string interpolation (eagerly, via f-strings, so the cost is paid even
when the level is disabled) and a synchronous write to stdout. Under Docker's json-file driver that is a
blocking write plus disk I/O **on the pricing thread**, directly adding to valuation latency.
`trade-action-service/worker.py:20` logs the entire payload of every queue item, and
`calculation_service.py:180` and `196` log twice per priced trade.

**Recommendation:** drop these to `DEBUG`, or aggregate ("priced 50 trades in 43 ms"). Prefer lazy `%s`-style
arguments — as `market-data-service/worker.py:342` already does — over f-strings, so disabled levels cost
nothing.

---



### PERF-4 · Batch buffer discarded wholesale on any failure

**Where:** `market-data-service/worker.py:390-401`

Both the inner and outer exception handlers call `buffer.clear()`, so a single transient failure — a serialisation
error, a pool timeout, one bad row — silently discards up to 30 market-data records. There is no retry, no
dead-letter path, and only a log line. The `finally: db_queue.task_done()` also acknowledges the queue item
regardless of outcome, so the loss is unrecoverable.

Note too that the flush condition `len(buffer) >= batch_size or (db_queue.empty() and len(buffer) > 0)` has no
time bound: if the queue never drains empty and the buffer never reaches 30, rows sit in memory indefinitely.

**Recommendation:** retry the batch with backoff before dropping, and on final failure log the discarded rows'
identifiers so loss is quantifiable. Distinguish transient errors (retry) from permanent ones (drop the
offending row, keep the rest). Add a time-based flush so latency is bounded regardless of queue behaviour.

---



### OPS-1 · Unpinned dependencies

**Where:** `backend/services/*/requirements.txt`, `backend/db/requirements.txt`

Every requirement is unpinned (`SQLAlchemy`, `bottle`, `psycopg[binary]`, `alembic`), so each image rebuild can
pull a different version — including a major SQLAlchemy release with different transaction defaults, which
would interact directly with the findings above. Builds are not reproducible and a regression cannot be
bisected. Several files are also missing a trailing newline, which makes them error-prone to concatenate.

**Recommendation:** pin exact versions and rebuild deliberately. A shared constraints file would keep the
seven services on one SQLAlchemy version, which matters because they share `trading_shared`.

---



## Suggested order of work

Sequenced so each step makes the next one safe rather than by raw severity:

1. **[BUG-1](#bug-1)** — the market-data thread dies on first SSE client; nothing else can be measured until
  this is fixed.
2. **[TX-1](#tx-1)** — the never-ending transaction blocks vacuum globally; leaving it in place undermines
  every other database fix. Fix the session lifecycle *before* switching the service to `ThreadedServer`.
3. **[SQL-1](#sql-1)** — indexes are the single highest-leverage change and are low-risk with
  `CREATE INDEX CONCURRENTLY`.
4. **[SQL-2](#sql-2)** + **[LOCK-3](#lock-3)** — batch the valuation and audit writers together; one change
  addresses the commit storm, the nested-session stall, and most of [PERF-1](#perf-1).
5. **[BUG-2](#bug-2)**, **[BUG-3](#bug-3)** — restore full pricing coverage and IRS pricing. Do this *after*
  step 4: lifting the 50-trade cap multiplies write volume, which is only safe once writes are batched and
   indexed.
6. **[LOCK-1](#lock-1)**, **[LOCK-2](#lock-2)**, **[TX-2](#tx-2)** — lock strength, lock ordering, and I/O
  inside transactions.
7. **[RACE-1](#race-1)**–**[RACE-4](#race-4)** — required before adding threads or replicas anywhere.
8. **[TX-3](#tx-3)**, **[TX-4](#tx-4)**, **[SQL-3](#sql-3)** — pool sizing, timeouts, retention.
9. Remaining medium/low items.

---



## Appendix A · Index DDL

Add as a new Alembic revision. `CREATE INDEX CONCURRENTLY` cannot run inside a transaction, so either set
`transaction_per_migration = False` and use `op.get_bind().execution_options(isolation_level="AUTOCOMMIT")`,
or drop `CONCURRENTLY` if a maintenance window is acceptable (fine at current table sizes).

```sql
-- trades: active-trade scans (pricing cache reload every 5s, trade generation every 4s)
CREATE INDEX CONCURRENTLY idx_trades_status_created_at ON trades (status, created_at DESC);

-- trades: per-book filters and the /books/summary aggregate
CREATE INDEX CONCURRENTLY idx_trades_book_id_status    ON trades (book_id, status);

-- trades: FK column (PostgreSQL does not index FKs automatically)
CREATE INDEX CONCURRENTLY idx_trades_instrument_id     ON trades (instrument_id);

-- trades: blotter symbol / asset-class filters
CREATE INDEX CONCURRENTLY idx_trades_symbol            ON trades (symbol);
CREATE INDEX CONCURRENTLY idx_trades_asset_class       ON trades (asset_class);

-- valuations: history lookup — covers both the query pattern and the FK
CREATE INDEX CONCURRENTLY idx_valuations_trade_time    ON valuations (trade_id, valuation_time DESC);
CREATE INDEX CONCURRENTLY idx_valuations_book_id       ON valuations (book_id);

-- market data: latest-price and latest-curve lookups (add ORDER BY event_time DESC in the queries too)
CREATE INDEX CONCURRENTLY idx_spot_asset_symbol_time   ON market_data_spot_prices (asset_class, symbol, event_time DESC);
CREATE INDEX CONCURRENTLY idx_curves_ccy_type_time     ON market_data_curves (currency, curve_type, event_time DESC);

-- instruments: symbol lookup on every OPEN_TRADE
CREATE INDEX CONCURRENTLY idx_instruments_symbol       ON instruments (symbol);

-- books: active-book listing
CREATE INDEX CONCURRENTLY idx_books_is_active          ON books (is_active);

-- audit logs: correlation-id tracing and time-range queries
CREATE INDEX CONCURRENTLY idx_audit_correlation_id     ON audit_logs (correlation_id);
CREATE INDEX CONCURRENTLY idx_audit_created_at         ON audit_logs (created_at DESC);
```

If `instruments.symbol` should be unique per asset class (the `get_by_symbol` lookup and the
create-if-missing logic in `trade-action-service/worker.py:69-76` both assume it is), enforce it —
otherwise concurrent `OPEN_TRADE`s for a new symbol will create duplicate instruments:

```sql
CREATE UNIQUE INDEX CONCURRENTLY uq_instruments_symbol_asset_class
    ON instruments (symbol, asset_class);
```

---



## Appendix B · How to verify these findings

```sql
-- Confirm SQL-1: list all non-constraint indexes (expect almost nothing)
SELECT tablename, indexname FROM pg_indexes
WHERE schemaname = 'public' ORDER BY tablename;

-- Confirm TX-1: the books-service connection stuck in transaction, and for how long
SELECT pid, state, now() - xact_start AS txn_age, now() - state_change AS idle_for, query
FROM pg_stat_activity
WHERE state = 'idle in transaction' ORDER BY xact_start;

-- Confirm SQL-2: transaction rate and per-table write volume
SELECT xact_commit, xact_rollback FROM pg_stat_database WHERE datname = 'market_data_db';
SELECT relname, n_tup_ins, n_live_tup, n_dead_tup, last_autovacuum
FROM pg_stat_user_tables ORDER BY n_tup_ins DESC;

-- Confirm SQL-1/SQL-3: sequential scans and bloat on the hot tables
SELECT relname, seq_scan, seq_tup_read, idx_scan,
       pg_size_pretty(pg_total_relation_size(relid)) AS total_size
FROM pg_stat_user_tables ORDER BY seq_tup_read DESC;

-- Confirm LOCK-1/LOCK-2: who is blocking whom, right now
SELECT blocked.pid AS blocked_pid, blocked.query AS blocked_query,
       blocking.pid AS blocking_pid, blocking.query AS blocking_query
FROM pg_stat_activity blocked
JOIN pg_stat_activity blocking ON blocking.pid = ANY(pg_blocking_pids(blocked.pid))
WHERE cardinality(pg_blocking_pids(blocked.pid)) > 0;

-- Confirm SQL-1 concretely: the pricing cache reload query plan
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM trades WHERE status = 'ACTIVE'
ORDER BY created_at DESC LIMIT 50 OFFSET 0;
```

Also worth enabling:

- `pg_stat_statements` — ranks queries by `total_exec_time`, which will confirm the ordering of
[SQL-1](#sql-1) and [SQL-2](#sql-2) empirically rather than by inspection.
- `log_lock_waits = on` with `deadlock_timeout = 1s` — surfaces [LOCK-1](#lock-1) / [LOCK-2](#lock-2) in the
Postgres log as they happen.
- `log_min_duration_statement = 500ms` — catches the scans from [SQL-1](#sql-1) as the tables grow.
- `SQLALCHEMY_WARN_20` / `echo_pool="debug"` on one service — makes the nested checkouts in
[LOCK-3](#lock-3) visible.

For **[BUG-1](#bug-1)**, the check is simply: `curl -N localhost:8001/stream`, then watch whether
`generated_events` on `/health` stops advancing. For **[BUG-2](#bug-2)**: create more than 50 active trades and
confirm that `GET /valuations` on the pricing service never returns more than 50 entries. For
**[BUG-3](#bug-3)**: `SELECT count(*) FROM market_data_curves;` stays at 0.