import queue
import sys
import uuid

import pytest

from service_loader import RecordingDB, StopWorker, load_service, silence_audit


@pytest.fixture(scope="module")
def market():
    load_service("market-data-service")
    return {"worker": sys.modules["worker"]}


@pytest.fixture(autouse=True)
def _reset(market):
    worker = market["worker"]
    silence_audit(worker.audit_logger)
    worker.subscribers.clear()
    worker.market_tick_data_state = {}
    worker.stats["generated_events"] = 0
    worker.stats["last_event_time"] = None
    _drain(worker.db_queue)
    _drain(worker.metrics_queue)
    yield


def _drain(q):
    while not q.empty():
        q.get_nowait()
        q.task_done()


def test_update_snapshot_replaces_state_and_counts_ticks(market):
    worker = market["worker"]
    worker.stats["generated_events"] = 3
    ticks = {"ACME": {"symbol": "ACME"}, "EURUSD": {"symbol": "EURUSD"}}

    worker.update_snapshot(ticks)

    assert worker.market_tick_data_state == ticks
    assert worker.stats["generated_events"] == 5
    assert worker.stats["last_event_time"] is not None


def test_publish_tick_to_stream_fans_out_and_allows_zero_subscribers(market):
    worker = market["worker"]
    first = queue.Queue()
    second = queue.Queue()
    worker.subscribers.extend(
        [{"id": "a", "queue": first}, {"id": "b", "queue": second}]
    )
    ticks = {"ACME": {"symbol": "ACME", "last": 1}}

    worker.publish_tick_to_stream(ticks)

    message = 'data: {"symbol": "ACME", "last": 1}\n\n'
    assert first.get_nowait() == message
    assert second.get_nowait() == message

    worker.subscribers.clear()
    worker.publish_tick_to_stream({"MSFT": {"symbol": "MSFT"}})


def test_persist_curves_skips_spots_and_writes_curves(market, monkeypatch):
    worker = market["worker"]
    monkeypatch.setattr(
        worker,
        "DBSessionManager",
        lambda: (_ for _ in ()).throw(AssertionError("spots must not open a session")),
    )
    worker.persist_curves({"ACME": {"symbol": "ACME", "last": 1}})

    db = RecordingDB()
    monkeypatch.setattr(worker, "DBSessionManager", lambda: db)
    curve_id = str(uuid.uuid4())
    worker.persist_curves(
        {
            "USD": {
                "curve_id": curve_id,
                "event_id": 4,
                "curve_name": "USD_YIELD_CURVE",
                "curve_type": "YIELD_CURVE",
                "currency": "USD",
                "tenors": ["1Y"],
                "rates": [0.04],
                "provider": "FRED",
            }
        }
    )

    assert db.commits == 1
    assert len(db.records) == 1
    record = db.records[0]
    assert str(record.curve_id) == curve_id
    assert record.curve_name == "USD_YIELD_CURVE"
    assert record.currency == "USD"
    assert record.provider == "FRED"
    assert record.rates == [0.04]


def test_persist_curves_logs_a_database_error(market, monkeypatch):
    worker = market["worker"]
    monkeypatch.setattr(
        worker,
        "DBSessionManager",
        lambda: RecordingDB(commit_error=RuntimeError("db down")),
    )

    worker.persist_curves(
        {
            "USD": {
                "curve_id": str(uuid.uuid4()),
                "curve_name": "USD_YIELD_CURVE",
                "curve_type": "YIELD_CURVE",
                "currency": "USD",
                "tenors": ["1Y"],
                "rates": [0.04],
            }
        }
    )


def test_persist_full_snapshot_writes_one_full_row(market, monkeypatch):
    worker = market["worker"]
    worker.global_event_id = 9
    db = RecordingDB()
    monkeypatch.setattr(worker, "DBSessionManager", lambda: db)
    ticks = {"ACME": {"symbol": "ACME"}}

    worker.persist_full_snapshot(ticks)

    assert db.commits == 1
    snapshot = db.records[0]
    assert snapshot.snapshot_type == "FULL"
    assert snapshot.event_id == 9
    assert snapshot.payload == ticks


def test_persist_full_snapshot_logs_a_database_error(market, monkeypatch):
    worker = market["worker"]
    monkeypatch.setattr(
        worker,
        "DBSessionManager",
        lambda: RecordingDB(error=RuntimeError("db down")),
    )

    worker.persist_full_snapshot({"ACME": {"symbol": "ACME"}})


def test_market_worker_runs_one_tick_then_stops(market, monkeypatch):
    worker = market["worker"]
    ticks = {"ACME": {"symbol": "ACME", "asset_type": "EQUITY", "last": 1}}
    order = []

    monkeypatch.setattr(worker, "generate_market_tick", lambda: (order.append("generate"), ticks)[1])

    def sleep(seconds):
        order.append("sleep")
        raise StopWorker()

    monkeypatch.setattr(worker.time, "sleep", sleep)
    monkeypatch.setattr(worker, "persist_curves", lambda data: order.append("curves"))
    monkeypatch.setattr(worker, "publish_tick_to_stream", lambda data: order.append("publish"))

    with pytest.raises(StopWorker):
        worker.market_worker()

    assert order == ["generate", "curves", "publish", "sleep"]
    assert worker.market_tick_data_state == ticks
    assert worker.db_queue.get_nowait() == ticks


def test_db_worker_maps_spots_skips_curves_and_flushes(market, monkeypatch):
    worker = market["worker"]
    db = RecordingDB()
    monkeypatch.setattr(worker, "DBSessionManager", lambda: db)
    worker.db_queue.put(
        {
            "equity": {
                "event_id": 1,
                "symbol": "ACME",
                "asset_type": "EQUITY",
                "bid": 9,
                "ask": 11,
                "last": 10,
            },
            "fx": {"event_id": 2, "symbol": "EURUSD", "asset_type": "FX", "spot": 1.08},
            "bond": {"event_id": 3, "symbol": "US10Y", "asset_type": "BOND", "yield": 0.04},
            "curve": {"event_id": 4, "symbol": "USD_YIELD_CURVE", "curve_type": "YIELD_CURVE"},
            "blank": {"event_id": 5, "symbol": "", "asset_type": "EQUITY"},
        }
    )
    real_get = worker.db_queue.get
    calls = {"n": 0}

    def get(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] > 1:
            raise StopWorker()
        return real_get(*args, **kwargs)

    monkeypatch.setattr(worker.db_queue, "get", get)

    with pytest.raises(StopWorker):
        worker.db_worker()

    assert db.commits == 1
    by_symbol = {record.symbol: record for record in db.records}
    assert set(by_symbol) == {"ACME", "EURUSD", "US10Y"}
    assert by_symbol["ACME"].bid == 9
    assert by_symbol["ACME"].ask == 11
    assert by_symbol["ACME"].last == 10
    assert by_symbol["EURUSD"].spot == 1.08
    assert by_symbol["US10Y"].last == 0.04


def test_db_worker_clears_the_buffer_when_the_write_fails(market, monkeypatch):
    worker = market["worker"]
    monkeypatch.setattr(
        worker,
        "DBSessionManager",
        lambda: RecordingDB(commit_error=RuntimeError("db down")),
    )
    worker.db_queue.put(
        {"equity": {"event_id": 1, "symbol": "ACME", "asset_type": "EQUITY", "last": 10}}
    )
    real_get = worker.db_queue.get
    calls = {"n": 0}

    def get(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] > 1:
            raise StopWorker()
        return real_get(*args, **kwargs)

    monkeypatch.setattr(worker.db_queue, "get", get)

    with pytest.raises(StopWorker):
        worker.db_worker()


def test_metric_worker_updates_health_from_one_event(market, monkeypatch):
    worker = market["worker"]
    sent = {"done": False}

    def get():
        if not sent["done"]:
            sent["done"] = True
            return {
                "type": "EVENT_GENERATED",
                "timestamp": "t1",
                "symbol": "ACME",
                "asset_type": "EQUITY",
            }
        raise StopWorker()

    monkeypatch.setattr(worker.metrics_queue, "get", get)
    monkeypatch.setattr(worker.metrics_queue, "task_done", lambda: None)

    with pytest.raises(StopWorker):
        worker.metric_worker()

    assert worker.health_stats["generated_events"] == 1
    assert worker.health_stats["last_event_time"] == "t1"
    assert worker.health_stats["last_symbol"] == "ACME"
    assert worker.health_stats["last_asset_type"] == "EQUITY"
    assert worker.health_stats["service"] == "market-data-service"
    assert worker.health_stats["status"] == "UP"
