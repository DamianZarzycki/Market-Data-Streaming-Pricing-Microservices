import json
import sys

import pytest

from service_loader import collect, invoke, load_service, read_json


@pytest.fixture(scope="module")
def pricing():
    app = load_service("pricing-service")
    return {
        "app": app,
        "worker": sys.modules["worker"],
        "calculation": sys.modules["calculation_service"],
        "book_metrics": sys.modules["books.book_metrics_service"],
    }


@pytest.fixture(autouse=True)
def _reset(pricing):
    calculation = pricing["calculation"]
    calculation.valuations_store.clear()
    calculation.sse_subscribers.clear()
    pricing["book_metrics"].metrics_store.clear()
    worker = pricing["worker"]
    worker.connection_status = "DISCONNECTED"
    worker.events_counter = 0
    worker.last_market_event_time = None
    worker.last_pricing_time = None
    worker.last_valuation_symbol = None
    worker.last_valuation_asset_class = None
    yield


def test_health_reports_worker_counters(pricing):
    worker = pricing["worker"]
    worker.connection_status = "CONNECTED"
    worker.events_counter = 4
    worker.last_market_event_time = "2026-10-06T10:00:00Z"
    worker.last_pricing_time = "2026-10-06T10:00:01Z"
    worker.last_valuation_symbol = "ACME"
    worker.last_valuation_asset_class = "EQUITY"

    status, _, body = read_json(pricing["app"].app, "/health")

    assert status == 200
    assert body["service"] == "pricing-service"
    assert body["status"] == "UP"
    assert body["market_data_connection"] == "CONNECTED"
    assert body["received_events"] == 4
    assert body["last_market_event_time"] == "2026-10-06T10:00:00Z"
    assert body["last_pricing_time"] == "2026-10-06T10:00:01Z"
    assert body["last_valuation_symbol"] == "ACME"
    assert body["last_valuation_asset_class"] == "EQUITY"


def test_valuations_returns_a_copy_of_the_store(pricing):
    status, _, body = read_json(pricing["app"].app, "/valuations")
    assert status == 200
    assert body == {}

    pricing["calculation"].valuations_store["t1"] = {"fair_value": 1.5, "symbol": "ACME"}
    status, _, body = read_json(pricing["app"].app, "/valuations")
    assert status == 200
    assert body == {"t1": {"fair_value": 1.5, "symbol": "ACME"}}

    snapshot = pricing["app"].valuations()
    snapshot["extra"] = {"fair_value": 9}
    assert "extra" not in pricing["calculation"].valuations_store


def test_valuation_by_trade_id_found_and_missing(pricing):
    pricing["calculation"].valuations_store["t1"] = {"fair_value": 1.5, "symbol": "ACME"}

    status, _, body = read_json(pricing["app"].app, "/valuations/t1")
    assert status == 200
    assert body["trade_id"] == "t1"
    assert body["fair_value"] == 1.5
    assert body["symbol"] == "ACME"

    status, _, body = read_json(pricing["app"].app, "/valuations/missing")
    assert status == 404
    assert body == {"error": "Valuation not found for trade_id: missing"}


def test_book_metrics_lists_every_book(pricing):
    pricing["book_metrics"].metrics_store["b1"] = {"alpha": 0.1, "beta": 1.2}

    status, _, body = read_json(pricing["app"].app, "/book-metrics")

    assert status == 200
    assert body["benchmark"] == "MARKET_INDEX"
    assert body["books"] == {"b1": {"alpha": 0.1, "beta": 1.2}}


def test_book_metrics_by_id_found_and_missing(pricing):
    pricing["book_metrics"].metrics_store["b1"] = {"alpha": 0.1, "beta": 1.2}

    status, _, body = read_json(pricing["app"].app, "/book-metrics/b1")
    assert status == 200
    assert body == {"alpha": 0.1, "beta": 1.2}

    status, _, body = read_json(pricing["app"].app, "/book-metrics/missing")
    assert status == 404
    assert body == {"error": "No metrics available for book_id: missing"}


def test_valuation_stream_snapshots_then_one_update(pricing, monkeypatch):
    calculation = pricing["calculation"]
    calculation.valuations_store["t1"] = {"trade_id": "t1", "fair_value": 1.5}
    calculation.valuations_store["bad"] = {"trade_id": "bad-id", "blob": {1, 2}}

    live = {"trade_id": "live", "fair_value": 3}

    class ScriptedQueue:
        def __init__(self):
            self.gets = 0

        def get(self):
            self.gets += 1
            if self.gets == 1:
                return live
            raise RuntimeError("stop stream")

        def task_done(self):
            pass

    monkeypatch.setattr(pricing["app"].queue, "Queue", ScriptedQueue)

    status, headers, iterator = invoke(pricing["app"].app, "/valuation-stream")
    assert status == 200
    assert headers["content-type"].startswith("text/event-stream")
    assert len(calculation.sse_subscribers) == 1

    body, error = collect(iterator)

    assert isinstance(error, RuntimeError)
    assert len(calculation.sse_subscribers) == 0
    text = body.decode()
    assert "bad-id" not in text
    frames = [frame for frame in text.split("\n\n") if frame.strip()]
    assert frames[0] == 'event: valuation_update\ndata: ' + json.dumps(
        {"trade_id": "t1", "fair_value": 1.5}
    )
    assert frames[1] == 'event: valuation_update\ndata: ' + json.dumps(live)
