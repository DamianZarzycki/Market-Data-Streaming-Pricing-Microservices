import sys
import uuid

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
    calculation = pricing["calculation"]
    _drain(calculation.metrics_queue)
    _drain(calculation.trade_valuation_queue)
    pricing["cache"].active_trades_cache.clear()
    yield


def _drain(queue):
    while not queue.empty():
        queue.get_nowait()
        queue.task_done()


def test_connect_to_market_data_stream_success_and_failure(pricing, monkeypatch):
    worker = pricing["worker"]
    response = object()
    monkeypatch.setattr(worker.request, "urlopen", lambda url: response)

    assert worker.connect_to_market_data_stream() is response
    assert worker.connection_status == "CONNECTED"

    def explode(url):
        raise OSError("down")

    monkeypatch.setattr(worker.request, "urlopen", explode)
    with pytest.raises(OSError):
        worker.connect_to_market_data_stream()
    assert worker.connection_status == "DISCONNECTED"


def test_read_events_parses_data_lines_and_skips_the_rest(pricing):
    stream = [
        b'event: tick\n',
        b'data: {"symbol": "ACME", "last": 1}\n',
        b'data: not-json\n',
        b'\n',
    ]

    assert list(pricing["worker"].read_events(stream)) == [{"symbol": "ACME", "last": 1}]


def test_update_market_state_enqueues_tick_received(pricing):
    pricing["worker"].update_market_state({"symbol": "ACME", "timestamp": "t1"})

    message = pricing["calculation"].metrics_queue.get_nowait()
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

    def get():
        if not pending:
            raise StopWorker()
        return pending.pop(0)

    monkeypatch.setattr(pricing["calculation"].metrics_queue, "get", get)
    monkeypatch.setattr(pricing["calculation"].metrics_queue, "task_done", lambda: None)

    with pytest.raises(StopWorker):
        worker.metrics_worker()

    assert worker.events_counter == 1
    assert worker.last_market_event_time == "t1"
    assert worker.last_pricing_time == "t3"
    assert worker.last_valuation_symbol == "ACME"
    assert worker.last_valuation_asset_class == "EQUITY"


def test_pricing_worker_reprices_ticks_and_survives_a_pricing_error(pricing, monkeypatch):
    worker = pricing["worker"]
    priced = []
    connects = {"n": 0}

    def connect():
        connects["n"] += 1
        if connects["n"] > 1:
            raise StopWorker()
        return [b"unused"]

    def read_events(_stream):
        return iter([{"symbol": "BAD"}, {"symbol": "ACME"}])

    def recalculate(tick):
        if tick["symbol"] == "BAD":
            raise RuntimeError("bad tick")
        priced.append(tick)

    monkeypatch.setattr(worker, "connect_to_market_data_stream", connect)
    monkeypatch.setattr(worker, "read_events", read_events)
    monkeypatch.setattr(pricing["calculation"], "recalculate_valuations", recalculate)

    with pytest.raises(StopWorker):
        worker.pricing_worker()

    assert priced == [{"symbol": "ACME"}]


def test_pricing_worker_marks_disconnected_when_the_stream_drops(pricing, monkeypatch):
    worker = pricing["worker"]
    worker.connection_status = "CONNECTED"

    def connect():
        raise OSError("lost")

    def sleep(seconds):
        assert seconds == 1
        raise StopWorker()

    monkeypatch.setattr(worker, "connect_to_market_data_stream", connect)
    monkeypatch.setattr(worker.time, "sleep", sleep)

    with pytest.raises(StopWorker):
        worker.pricing_worker()

    assert worker.connection_status == "DISCONNECTED"


def test_fetch_provider_quote_lowercases_the_provider(pricing, monkeypatch):
    worker = pricing["worker"]
    seen = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'{"bid": 1}'

    def urlopen(url, timeout=5):
        seen["url"] = url
        seen["timeout"] = timeout
        return Response()

    monkeypatch.setattr(worker.request, "urlopen", urlopen)
    assert worker._fetch_provider_quote("FINNHUB", "ACME") == {"bid": 1}
    assert seen["url"].endswith("/market-data/quotes/finnhub/ACME")

    def explode(url, timeout=5):
        raise OSError("timeout")

    monkeypatch.setattr(worker.request, "urlopen", explode)
    assert worker._fetch_provider_quote("FINNHUB", "ACME") is None


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

    def fetch(provider, symbol):
        fetched.append((provider, symbol))
        return quotes[(provider, symbol)]

    priced = []

    def price(trade, quote):
        priced.append(trade.trade_id)
        if trade.trade_id == "bad":
            raise RuntimeError("price failed")

    monkeypatch.setattr(worker, "_fetch_provider_quote", fetch)
    monkeypatch.setattr(pricing["calculation"], "price_trade_from_provider_quote", price)
    monkeypatch.setattr(worker.time, "sleep", lambda seconds: (_ for _ in ()).throw(StopWorker()))

    with pytest.raises(StopWorker):
        worker.provider_pricing_worker()

    assert ("FRED", "USD_5Y_IRS") not in fetched
    assert fetched.count(("FINNHUB", "ACME")) == 1
    assert priced == ["a1", "a2", "bad"]


def test_curve_pricing_worker_calls_reprice_and_logs_errors(pricing, monkeypatch):
    calls = {"n": 0}

    def reprice():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("curve down")

    monkeypatch.setattr(pricing["calculation"], "reprice_irs_from_persisted_curves", reprice)
    sleeps = {"n": 0}

    def sleep(seconds):
        assert seconds == 5
        sleeps["n"] += 1
        if sleeps["n"] == 2:
            raise StopWorker()

    monkeypatch.setattr(pricing["worker"].time, "sleep", sleep)

    with pytest.raises(StopWorker):
        pricing["worker"].curve_pricing_worker()

    assert calls["n"] == 2


def test_valuation_worker_flushes_a_batch_and_skips_an_empty_queue(pricing, monkeypatch):
    worker = pricing["worker"]
    opened = []

    def sleep(seconds):
        opened.append(("sleep", seconds))
        if opened.count(("sleep", seconds)) > 1:
            raise StopWorker()

    monkeypatch.setattr(worker.time, "sleep", sleep)

    class Guard:
        def __enter__(self):
            raise AssertionError("empty queue must not open a session")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(worker, "DBSessionManager", Guard)
    with pytest.raises(StopWorker):
        worker.valuation_worker()

    trade_id = str(uuid.uuid4())
    book_id = str(uuid.uuid4())
    pricing["calculation"].trade_valuation_queue.put(
        {
            "trade_id": trade_id,
            "book_id": book_id,
            "asset_class": "EQUITY",
            "symbol": "ACME",
            "fair_value": 1.5,
            "unrealized_pnl": 0.1,
            "realized_pnl": 0,
            "total_pnl": 0.1,
            "currency": "USD",
        }
    )
    db = RecordingDB()
    monkeypatch.setattr(worker, "DBSessionManager", lambda: db)
    opened.clear()

    with pytest.raises(StopWorker):
        worker.valuation_worker()

    assert db.commits == 1
    assert len(db.records) == 1
    assert str(db.records[0].trade_id) == trade_id


def test_valuation_worker_logs_a_flush_error(pricing, monkeypatch):
    worker = pricing["worker"]
    sleeps = {"n": 0}

    def sleep(seconds):
        sleeps["n"] += 1
        if sleeps["n"] > 1:
            raise StopWorker()

    monkeypatch.setattr(worker.time, "sleep", sleep)
    monkeypatch.setattr(worker, "DBSessionManager", lambda: RecordingDB(error=RuntimeError("db")))
    pricing["calculation"].trade_valuation_queue.put(
        {
            "trade_id": str(uuid.uuid4()),
            "book_id": str(uuid.uuid4()),
            "asset_class": "EQUITY",
            "symbol": "ACME",
            "fair_value": 1,
            "unrealized_pnl": 0,
            "realized_pnl": 0,
            "total_pnl": 0,
            "currency": "USD",
        }
    )

    with pytest.raises(StopWorker):
        worker.valuation_worker()


def test_cache_refresh_worker_reloads_once_per_wake(pricing, monkeypatch):
    calls = []
    monkeypatch.setattr(
        pricing["cache"],
        "reload_active_trades_cache",
        lambda: calls.append("reload"),
    )

    def sleep(seconds):
        assert seconds == 5
        raise StopWorker()

    monkeypatch.setattr(pricing["worker"].time, "sleep", sleep)

    with pytest.raises(StopWorker):
        pricing["worker"].cache_refresh_worker()

    assert calls == ["reload"]
