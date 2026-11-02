import asyncio
import json
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import pytest
from fastapi.testclient import TestClient

from service_loader import load_service


@pytest.fixture(scope="module")
def pricing():
    app = load_service("pricing-service")
    return {
        "app": app,
        "worker": sys.modules["worker"],
        "calculation": sys.modules["calculation_service"],
        "book_metrics": sys.modules["books.book_metrics_service"],
    }


@contextmanager
def _client(pricing) -> Iterator[tuple[TestClient, dict]]:
    worker_mod = cast(Any, pricing["worker"])

    async def _idle():
        await asyncio.Event().wait()

    async def _quiet(*args, **kwargs):
        return None

    worker_mod.pricing_worker = _idle
    worker_mod.metrics_worker = _idle
    worker_mod.cache_refresh_worker = _idle
    worker_mod.valuation_worker = _idle
    worker_mod.provider_pricing_worker = _idle
    worker_mod.curve_pricing_worker = _idle
    worker_mod.audit_logger.ainfo = _quiet
    worker_mod.audit_logger.aerror = _quiet
    with TestClient(pricing["app"].app) as client:
        yield client, pricing


@pytest.fixture(autouse=True)
def _reset(pricing):
    calculation = pricing["calculation"]
    calculation.valuation_state.valuations_store.clear()
    calculation.valuation_state.sse_subscribers.clear()
    calculation.valuation_state.set_external_yield_currencies(set())
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

    with _client(pricing) as (client, _loaded):
        response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["service"] == "pricing-service"
    assert body["status"] == "UP"
    assert body["market_data_connection"] == "CONNECTED"
    assert body["received_events"] == 4
    assert body["last_market_event_time"] == "2026-10-06T10:00:00Z"
    assert body["last_pricing_time"] == "2026-10-06T10:00:01Z"
    assert body["last_valuation_symbol"] == "ACME"
    assert body["last_valuation_asset_class"] == "EQUITY"


def test_valuations_returns_a_copy_of_the_store(pricing):
    with _client(pricing) as (client, loaded):
        response = client.get("/valuations")
        assert response.status_code == 200
        assert response.json() == {}

        loaded["calculation"].valuation_state.valuations_store["t1"] = {"fair_value": 1.5, "symbol": "ACME"}
        response = client.get("/valuations")
        assert response.status_code == 200
        assert response.json() == {"t1": {"fair_value": 1.5, "symbol": "ACME"}}

        snapshot = loaded["calculation"].valuation_feed.valuations_view()
        snapshot["extra"] = {"fair_value": 9}
        assert "extra" not in loaded["calculation"].valuation_state.valuations_store


def test_valuation_by_trade_id_found_and_missing(pricing):
    pricing["calculation"].valuation_state.valuations_store["t1"] = {"fair_value": 1.5, "symbol": "ACME"}

    with _client(pricing) as (client, _loaded):
        response = client.get("/valuations/t1")
        assert response.status_code == 200
        body = response.json()
        assert body["trade_id"] == "t1"
        assert body["fair_value"] == 1.5
        assert body["symbol"] == "ACME"

        missing = client.get("/valuations/missing")
        assert missing.status_code == 404
        assert missing.json() == {"error": "Valuation not found for trade_id: missing"}


def test_book_metrics_lists_every_book(pricing):
    pricing["book_metrics"].metrics_store["b1"] = {"alpha": 0.1, "beta": 1.2}

    with _client(pricing) as (client, _loaded):
        response = client.get("/book-metrics")

    assert response.status_code == 200
    body = response.json()
    assert body["benchmark"] == "MARKET_INDEX"
    assert body["books"] == {"b1": {"alpha": 0.1, "beta": 1.2}}


def test_book_metrics_by_id_found_and_missing(pricing):
    pricing["book_metrics"].metrics_store["b1"] = {"alpha": 0.1, "beta": 1.2}

    with _client(pricing) as (client, _loaded):
        found = client.get("/book-metrics/b1")
        assert found.status_code == 200
        assert found.json() == {"alpha": 0.1, "beta": 1.2}

        missing = client.get("/book-metrics/missing")
        assert missing.status_code == 404
        assert missing.json() == {"error": "No metrics available for book_id: missing"}


def test_valuation_stream_snapshots_then_one_update(pricing, monkeypatch):
    calculation = pricing["calculation"]
    calculation.valuation_state.valuations_store["t1"] = {"trade_id": "t1", "fair_value": 1.5}
    calculation.valuation_state.valuations_store["bad"] = {"trade_id": "bad-id", "blob": {1, 2}}
    seen = {}

    class ScriptedQueue(asyncio.Queue):
        def __init__(self, maxsize=0):
            super().__init__(maxsize=maxsize)
            self.gets = 0

        async def get(self):
            self.gets += 1
            if self.gets == 1:
                seen["subscribers"] = len(calculation.valuation_state.sse_subscribers)
                return {"trade_id": "live", "fair_value": 3}
            return None

    monkeypatch.setattr(sys.modules["calculation.valuation_feed"].asyncio, "Queue", ScriptedQueue)

    with _client(pricing) as (client, _loaded):
        response = client.get("/valuation-stream")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert seen["subscribers"] == 1
    assert calculation.valuation_state.sse_subscribers == []
    text = response.text
    assert "bad-id" not in text
    frames = [frame for frame in text.split("\n\n") if frame.strip()]
    assert frames[0] == "event: valuation_update\ndata: " + json.dumps(
        {"trade_id": "t1", "fair_value": 1.5}
    )
    assert frames[1] == "event: valuation_update\ndata: " + json.dumps(
        {"trade_id": "live", "fair_value": 3}
    )
