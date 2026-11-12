import asyncio
import sys
import uuid
from datetime import datetime, timezone

import pytest

from service_loader import RecordingDB, StopWorker, load_service, silence_audit


@pytest.fixture(scope="module")
def pricing():
    load_service("pricing-service")
    return {
        "worker": sys.modules["worker"],
        "calculation": sys.modules["calculation_service"],
        "cache": sys.modules["cache_service"],
        "valuation": sys.modules["valuation_service"],
        "book_metrics": sys.modules["books.book_metrics_service"],
    }


@pytest.fixture(autouse=True)
def _reset(pricing):
    worker = pricing["worker"]
    silence_audit(worker.audit_logger)
    worker.connection_status = "DISCONNECTED"
    worker.events_counter = 0
    worker.last_market_event_time = None
    worker.last_pricing_time = None
    worker.last_valuation_symbol = None
    worker.last_valuation_asset_class = None
    worker.http_client = None
    calculation = pricing["calculation"]
    # A queue binds to the loop that first waits on it. Each test gets a new one.
    metrics_queue = asyncio.Queue()
    trade_queue = asyncio.Queue()
    calculation.valuation_state.metrics_queue = metrics_queue
    calculation.valuation_state.trade_valuation_queue = trade_queue
    calculation.valuation_state.valuations_store.clear()
    calculation.valuation_state.sse_subscribers.clear()
    calculation.valuation_state.set_external_yield_currencies(set())
    pricing["cache"].active_trades_cache.clear()
    pricing["cache"].symbol_index.clear()
    pricing["book_metrics"].metrics_store.clear()
    pricing["book_metrics"].last_book_value.clear()
    pricing["book_metrics"].book_returns.clear()
    pricing["book_metrics"].last_benchmark_level["value"] = None
    yield


def test_iter_market_ticks_marks_connected_and_disconnected(pricing):
    worker = pricing["worker"]

    class Response:
        def raise_for_status(self):
            return None

        async def aiter_lines(self):
            yield 'data: {"symbol": "ACME"}\n'
            yield "data: not-json"
            yield "event: tick"

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class Client:
        def stream(self, method, url):
            return Response()

    worker.http_client = Client()

    async def read():
        return [tick async for tick in worker.iter_market_ticks()]

    assert asyncio.run(read()) == [{"symbol": "ACME"}]
    assert worker.connection_status == "CONNECTED"

    class Down:
        def stream(self, method, url):
            raise OSError("down")

    worker.http_client = Down()

    async def fail():
        async for _tick in worker.iter_market_ticks():
            pass

    with pytest.raises(OSError):
        asyncio.run(fail())
    assert worker.connection_status == "DISCONNECTED"


def test_parse_market_data_line_skips_non_data(pricing):
    parse = pricing["worker"].parse_market_data_line
    assert parse('data: {"symbol": "ACME", "last": 1}') == {"symbol": "ACME", "last": 1}
    assert parse("data: not-json") is None
    assert parse("event: tick") is None
    assert parse("") is None


def test_update_market_state_enqueues_tick_received(pricing):
    pricing["worker"].update_market_state({"symbol": "ACME", "timestamp": "t1"})

    message = pricing["calculation"].valuation_state.metrics_queue.get_nowait()
    assert message == {"type": "TICK_RECEIVED", "timestamp": "t1"}


def test_metrics_worker_updates_counters_and_keeps_last_symbol(pricing, monkeypatch):
    worker = pricing["worker"]
    messages = [
        {"type": "TICK_RECEIVED", "timestamp": "t1"},
        {
            "type": "PRICING_DONE",
            "timestamp": "t2",
            "instrument": "i",
            "value": 1,
            "symbol": "ACME",
            "asset_class": "EQUITY",
        },
        {
            "type": "PRICING_DONE",
            "timestamp": "t3",
            "instrument": "i",
            "value": 2,
            "symbol": None,
            "asset_class": "",
        },
    ]
    pending = list(messages)

    async def get():
        if not pending:
            raise StopWorker()
        return pending.pop(0)

    monkeypatch.setattr(pricing["calculation"].valuation_state.metrics_queue, "get", get)
    monkeypatch.setattr(pricing["calculation"].valuation_state.metrics_queue, "task_done", lambda: None)

    with pytest.raises(StopWorker):
        asyncio.run(worker.metrics_worker())

    assert worker.events_counter == 1
    assert worker.last_market_event_time == "t1"
    assert worker.last_pricing_time == "t3"
    assert worker.last_valuation_symbol == "ACME"
    assert worker.last_valuation_asset_class == "EQUITY"


def test_pricing_worker_reprices_ticks_and_survives_a_pricing_error(pricing, monkeypatch):
    worker = pricing["worker"]
    priced = []
    calls = {"n": 0}

    async def ticks():
        calls["n"] += 1
        if calls["n"] > 1:
            raise StopWorker()
        yield {"symbol": "BAD"}
        yield {"symbol": "ACME"}

    def recalculate(tick):
        if tick["symbol"] == "BAD":
            raise RuntimeError("bad tick")
        priced.append(tick)

    monkeypatch.setattr(worker, "iter_market_ticks", ticks)
    monkeypatch.setattr(pricing["calculation"].simulator_pricing, "recalculate", recalculate)

    with pytest.raises(StopWorker):
        asyncio.run(worker.pricing_worker())

    assert priced == [{"symbol": "ACME"}]


def test_pricing_worker_marks_disconnected_when_the_stream_drops(pricing, monkeypatch):
    worker = pricing["worker"]
    worker.connection_status = "CONNECTED"

    async def ticks():
        raise OSError("lost")
        yield None

    async def sleep(seconds):
        assert seconds == 1
        raise StopWorker()

    monkeypatch.setattr(worker, "iter_market_ticks", ticks)
    monkeypatch.setattr(worker.asyncio, "sleep", sleep)

    with pytest.raises(StopWorker):
        asyncio.run(worker.pricing_worker())

    assert worker.connection_status == "DISCONNECTED"


def test_fetch_provider_quote_lowercases_and_quotes_the_symbol(pricing):
    worker = pricing["worker"]
    seen = {}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"bid": 1}

    async def get(url, timeout=5):
        seen["url"] = url
        seen["timeout"] = timeout
        return Response()

    class Client:
        pass

    client = Client()
    client.get = get
    worker.http_client = client

    assert asyncio.run(worker._fetch_provider_quote("FINNHUB", "BRK/B")) == {"bid": 1}
    assert seen["url"].endswith("/market-data/quotes/finnhub/BRK%2FB")
    assert seen["timeout"] == 5

    async def explode(url, timeout=5):
        raise OSError("timeout")

    client.get = explode
    assert asyncio.run(worker._fetch_provider_quote("FINNHUB", "ACME")) is None


def test_provider_pricing_worker_prices_eligible_trades_once_per_pair(pricing, monkeypatch):
    worker = pricing["worker"]
    cache = pricing["cache"]

    class Trade:
        def __init__(self, trade_id, provider, symbol, asset_class):
            self.trade_id = trade_id
            self.market_data_provider = provider
            self.symbol = symbol
            self.asset_class = asset_class

    trades = [
        Trade("a1", "FINNHUB", "ACME", "EQUITY"),
        Trade("a2", "FINNHUB", "ACME", "EQUITY"),
        Trade("bad", "FINNHUB", "BAD", "EQUITY"),
        Trade("miss", "FINNHUB", "MISS", "EQUITY"),
        Trade("err", "FINNHUB", "ERR", "EQUITY"),
        Trade("irs", "FRED", "USD_5Y_IRS", "IRS"),
        Trade("sim", None, "ACME", "EQUITY"),
    ]
    for trade in trades:
        cache.active_trades_cache[trade.trade_id] = trade

    fetched = []
    quotes = {
        ("FINNHUB", "ACME"): {"bid": 10},
        ("FINNHUB", "BAD"): {"bid": 1},
        ("FINNHUB", "MISS"): None,
        ("FINNHUB", "ERR"): {"error": "no quote"},
    }

    async def fetch(provider, symbol):
        fetched.append((provider, symbol))
        return quotes[(provider, symbol)]

    priced = []

    def price(trade, quote):
        priced.append(trade.trade_id)
        if trade.trade_id == "bad":
            raise RuntimeError("price failed")

    async def sleep(seconds):
        raise StopWorker()

    monkeypatch.setattr(worker, "_fetch_provider_quote", fetch)
    monkeypatch.setattr(pricing["calculation"].quote_pricing, "price", price)
    monkeypatch.setattr(worker.asyncio, "sleep", sleep)

    with pytest.raises(StopWorker):
        asyncio.run(worker.provider_pricing_worker())

    assert ("FRED", "USD_5Y_IRS") not in fetched
    assert fetched.count(("FINNHUB", "ACME")) == 1
    assert priced == ["a1", "a2", "bad"]


def test_curve_pricing_worker_calls_reprice_and_logs_errors(pricing, monkeypatch):
    calls = {"n": 0}

    async def reprice():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("curve down")

    monkeypatch.setattr(pricing["calculation"].irs_curve_pricing, "reprice_persisted", reprice)
    sleeps = {"n": 0}

    async def sleep(seconds):
        assert seconds == 5
        sleeps["n"] += 1
        if sleeps["n"] == 2:
            raise StopWorker()

    monkeypatch.setattr(pricing["worker"].asyncio, "sleep", sleep)

    with pytest.raises(StopWorker):
        asyncio.run(pricing["worker"].curve_pricing_worker())

    assert calls["n"] == 2


def _valuation_payload(trade_id=None, book_id=None, valuation_time=None):
    return {
        "trade_id": trade_id or str(uuid.uuid4()),
        "book_id": book_id or str(uuid.uuid4()),
        "asset_class": "EQUITY",
        "symbol": "ACME",
        "fair_value": 1.5,
        "unrealized_pnl": 0.1,
        "realized_pnl": 0,
        "total_pnl": 0.1,
        "currency": "USD",
        "valuation_time": valuation_time,
    }


def test_valuation_worker_does_not_open_a_session_for_an_empty_queue(pricing, monkeypatch):
    worker = pricing["worker"]
    opened = {"n": 0}

    class Guard:
        async def __aenter__(self):
            opened["n"] += 1
            raise AssertionError("empty queue must not open a session")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(worker, "AsyncDBSessionManager", Guard)

    async def scenario():
        task = asyncio.create_task(worker.valuation_worker())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert opened["n"] == 0


def test_valuation_worker_flushes_the_payload_timestamp(pricing, monkeypatch):
    worker = pricing["worker"]
    trade_id = str(uuid.uuid4())
    book_id = str(uuid.uuid4())
    db = RecordingDB()
    monkeypatch.setattr(worker, "AsyncDBSessionManager", lambda: db)

    async def scenario():
        pricing["calculation"].valuation_state.trade_valuation_queue.put_nowait(
            _valuation_payload(trade_id, book_id, "2026-10-06T10:00:00.000Z")
        )
        task = asyncio.create_task(worker.valuation_worker())
        for _ in range(50):
            if db.commits:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

    assert db.commits == 1
    assert len(db.records) == 1
    assert str(db.records[0].trade_id) == trade_id
    assert db.records[0].valuation_time == datetime(2026, 10, 6, 10, 0, tzinfo=timezone.utc)


def test_valuation_worker_retries_a_failed_commit(pricing, monkeypatch):
    worker = pricing["worker"]

    class Flaky(RecordingDB):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        def commit(self):
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("db")
            return super().commit()

    db = Flaky()
    monkeypatch.setattr(worker, "AsyncDBSessionManager", lambda: db)
    real_sleep = asyncio.sleep

    async def no_wait(_seconds):
        return None

    monkeypatch.setattr(worker.asyncio, "sleep", no_wait)

    async def scenario():
        pricing["calculation"].valuation_state.trade_valuation_queue.put_nowait(_valuation_payload())
        task = asyncio.create_task(worker.valuation_worker())
        for _ in range(50):
            if db.commits:
                break
            await real_sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert db.attempts == 2
    assert db.commits == 1


def test_valuation_worker_drops_a_batch_after_repeated_failures(pricing, monkeypatch):
    worker = pricing["worker"]

    class Always(RecordingDB):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        def commit(self):
            self.attempts += 1
            raise RuntimeError("db")

    db = Always()
    monkeypatch.setattr(worker, "AsyncDBSessionManager", lambda: db)
    real_sleep = asyncio.sleep

    async def no_wait(_seconds):
        return None

    monkeypatch.setattr(worker.asyncio, "sleep", no_wait)

    async def scenario():
        pricing["calculation"].valuation_state.trade_valuation_queue.put_nowait(_valuation_payload())
        task = asyncio.create_task(worker.valuation_worker())
        for _ in range(50):
            if db.attempts >= worker.VALUATION_FLUSH_ATTEMPTS:
                break
            await real_sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())
    assert db.attempts == worker.VALUATION_FLUSH_ATTEMPTS
    assert db.commits == 0


def test_cache_refresh_worker_reloads_once_per_wake(pricing, monkeypatch):
    calls = []

    async def reload():
        calls.append("reload")
        return set()

    monkeypatch.setattr(pricing["cache"], "reload_active_trades_cache", reload)

    async def sleep(seconds):
        assert seconds == 5
        raise StopWorker()

    monkeypatch.setattr(pricing["worker"].asyncio, "sleep", sleep)

    with pytest.raises(StopWorker):
        asyncio.run(pricing["worker"].cache_refresh_worker())

    assert calls == ["reload"]


def test_unchanged_mark_is_not_queued(pricing):
    calculation = pricing["calculation"]
    payload = {
        "trade_id": "t1",
        "fair_value": 1.0,
        "current_price": 1.0,
        "unrealized_pnl": 0.0,
        "is_stale": False,
        "valuation_time": "first",
    }
    calculation.valuation_feed.store_and_publish(None, payload)
    again = dict(payload)
    again["valuation_time"] = "second"
    calculation.valuation_feed.store_and_publish(None, again)

    assert calculation.valuation_state.trade_valuation_queue.qsize() == 1
    assert calculation.valuation_state.valuations_store["t1"]["valuation_time"] == "first"


def test_fx_spot_only_tick_is_marked_and_rates_still_forward(pricing):
    cache = pricing["cache"]
    calculation = pricing["calculation"]
    spot_only = cache.CachedTrade(
        trade_id="fx1",
        book_id="b",
        symbol="EURUSD",
        asset_class="FX",
        side="BUY",
        quantity=2,
        trade_price=1.0,
        trade_currency="USD",
        metadata_payload=None,
        market_data_provider=None,
    )
    calculation.simulator_pricing.price_trade({"spot": 1.25}, spot_only, "FX")
    assert calculation.valuation_state.valuations_store["fx1"]["current_price"] == 1.25
    assert calculation.valuation_state.valuations_store["fx1"]["fair_value"] == 2.5

    forwarded = cache.CachedTrade(
        trade_id="fx2",
        book_id="b",
        symbol="EURUSD",
        asset_class="FX",
        side="BUY",
        quantity=1,
        trade_price=1.0,
        trade_currency="USD",
        metadata_payload=None,
        market_data_provider=None,
    )
    calculation.simulator_pricing.price_trade(
        {"spot": 1.0, "domestic_rate": 0.1, "foreign_rate": 0.0, "tenor_years": 1},
        forwarded,
        "FX",
    )
    assert calculation.valuation_state.valuations_store["fx2"]["current_price"] == 1.1


def test_futures_multiplier_comes_from_metadata(pricing):
    cache = pricing["cache"]
    calculation = pricing["calculation"]
    trade = cache.CachedTrade(
        trade_id="fut",
        book_id="b",
        symbol="ES",
        asset_class="FUTURES",
        side="BUY",
        quantity=2,
        trade_price=4,
        trade_currency="USD",
        metadata_payload={"contract_multiplier": 10},
        market_data_provider=None,
    )
    calculation.simulator_pricing.price_trade(
        {"futures_price": 5, "contract_multiplier": 2},
        trade,
        "FUTURES",
    )
    stored = calculation.valuation_state.valuations_store["fut"]
    assert stored["fair_value"] == 100
    assert stored["unrealized_pnl"] == 20


def test_irs_symbol_tick_does_not_raise(pricing):
    pricing["calculation"].simulator_pricing.recalculate(
        {"asset_type": "IRS", "symbol": "USD_IRS"}
    )
    assert pricing["calculation"].valuation_state.valuations_store == {}


def test_evict_inactive_drops_closed_trades_and_book_metrics(pricing):
    calculation = pricing["calculation"]
    books = pricing["book_metrics"]
    calculation.valuation_state.valuations_store["gone"] = {"book_id": "b-gone", "fair_value": 1, "currency": "USD"}
    calculation.valuation_state.valuations_store["stay"] = {"book_id": "b-stay", "fair_value": 2, "currency": "USD"}
    books.metrics_store["b-gone"] = {"alpha": 1}
    books.last_book_value["b-gone"] = ("USD", 1.0)
    books.book_returns["b-gone"].append((0.1, 0.2))

    calculation.valuation_feed.evict_inactive({"stay"})

    assert "gone" not in calculation.valuation_state.valuations_store
    assert "stay" in calculation.valuation_state.valuations_store
    assert "b-gone" not in books.metrics_store
    assert "b-gone" not in books.last_book_value
    assert "b-gone" not in books.book_returns


def test_book_value_uses_the_largest_currency(pricing):
    calculation = pricing["calculation"]
    calculation.valuation_state.valuations_store["usd"] = {
        "book_id": "b",
        "fair_value": 10,
        "currency": "USD",
    }
    calculation.valuation_state.valuations_store["eur"] = {
        "book_id": "b",
        "fair_value": -40,
        "currency": "EUR",
    }

    assert pricing["book_metrics"].snapshot_book_values()["b"] == ("EUR", -40.0)


def test_external_curve_skips_the_simulator_db_read(pricing, monkeypatch):
    calculation = pricing["calculation"]
    calculation.valuation_state.set_external_yield_currencies({"USD"})
    opened = {"n": 0}

    class DB:
        async def __aenter__(self):
            opened["n"] += 1
            raise AssertionError("simulator curve tick must not open a session")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(sys.modules["calculation.irs_curve_pricing"], "AsyncDBSessionManager", DB)
    asyncio.run(
        calculation.irs_curve_pricing.update_from_tick(
            {
                "curve_id": "c1",
                "currency": "USD",
                "curve_type": "YIELD_CURVE",
                "provider": "SIMULATED",
            }
        )
    )
    assert opened["n"] == 0


def test_sse_queue_drops_the_oldest_frame(pricing):
    calculation = pricing["calculation"]
    client_queue = asyncio.Queue(maxsize=2)
    calculation.valuation_feed.offer_sse(client_queue, {"n": 1})
    calculation.valuation_feed.offer_sse(client_queue, {"n": 2})
    calculation.valuation_feed.offer_sse(client_queue, {"n": 3})

    assert client_queue.qsize() == 2
    assert client_queue.get_nowait() == {"n": 2}
    assert client_queue.get_nowait() == {"n": 3}
