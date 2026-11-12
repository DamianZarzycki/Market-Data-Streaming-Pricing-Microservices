import asyncio
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

import httpx2
from fastapi.testclient import TestClient

from service_loader import load_service, silence_audit


@contextmanager
def _client() -> Iterator[tuple[TestClient, Any]]:
    app_module = load_service("market-data-service-integration")
    state = cast(Any, sys.modules["core.runtime"])
    worker_mod = cast(Any, sys.modules["core.polling.quote_persistence"])
    quote_mod = cast(Any, sys.modules["core.quotes"])
    curve_mod = cast(Any, sys.modules["core.curve_service"])
    poll_mod = cast(Any, sys.modules["core.polling.workers"])
    tracker_mod = quote_mod

    async def _idle():
        await asyncio.Event().wait()

    async def _startup():
        tracker = tracker_mod.RequestTracker({})
        state.quote_service = quote_mod.QuoteService.create(tracker, worker_mod.db_queue)
        state.curve_service = curve_mod.CurveService.create()
        state.poll_workers = poll_mod.PollWorkers(state.quote_service, state.curve_service)

    app_module.startup = _startup
    app_module.db_worker = _idle
    app_module.audit_worker = _idle
    app_module.quote_poll_worker = _idle
    app_module.curve_poll_worker = _idle
    app_module.curve_db_worker = _idle
    silence_audit(app_module.audit_logger)
    silence_audit(worker_mod.audit_logger)
    with TestClient(app_module.app) as client:
        yield client, state


def test_health_reports_the_in_memory_snapshot():
    with _client() as (client, _state):
        response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["service"] == "market-data-service-integration"
    assert body["status"] == "UP"
    assert body["quote_service"]["error_count"] == 0
    assert body["curve_service"]["error_count"] == 0


def test_symbols_and_providers_read_memory(monkeypatch):
    monkeypatch.setenv("MARKET_DATA_SYMBOLS", "AAPL,MSFT,EURUSD")
    monkeypatch.setenv("MARKET_DATA_FX_SYMBOLS", "EURUSD")
    monkeypatch.setenv("MARKET_DATA_BOND_SYMBOLS", "USD_10Y")
    monkeypatch.setenv("MARKET_DATA_IRS_SYMBOLS", "USD_10Y_IRS")
    monkeypatch.setenv("MARKET_DATA_FUTURES_CONTRACTS", "ES:SPY:50")

    with _client() as (client, _state):
        symbols = client.get("/market-data/symbols")
        providers = client.get("/providers")

    assert symbols.status_code == 200
    catalog = symbols.json()
    assert catalog["EQUITY"] == ["AAPL", "MSFT"]
    assert catalog["FX"] == ["EURUSD"]
    assert catalog["FUTURES_CONTRACTS"]["ES"] == {"underlying": "SPY", "multiplier": 50.0}
    assert providers.status_code == 200
    assert providers.json()["providers"] == ["finnhub", "alpha_vantage", "twelve_data"]
    assert providers.json()["curve_providers"] == ["nbp", "ecb", "fred"]


def test_quotes_requires_a_symbol():
    with _client() as (client, _state):
        response = client.get("/market-data/quotes")

    assert response.status_code == 400
    assert response.json() == {"error": "symbol is required"}


def test_quotes_reads_through_the_async_session(monkeypatch):
    with _client() as (client, _state):
        quote_mod = cast(Any, sys.modules["core.quotes"])
        seen = {}

        class _Quotes:
            async def get_latest_for_symbol_async(self, symbol, asset_class):
                seen["args"] = (symbol, asset_class)
                return ["row"]

        class _DB:
            def __init__(self):
                self.provider_quotes = _Quotes()

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

        monkeypatch.setattr(quote_mod, "AsyncDBSessionManager", lambda: _DB())
        monkeypatch.setattr(
            quote_mod,
            "serialize",
            lambda row: {"provider": "FINNHUB", "symbol": "AAPL", "last": 10.0},
        )

        response = client.get("/market-data/quotes?symbol=aapl&asset_class=equity")

    assert response.status_code == 200
    assert response.json() == {
        "quotes": [{"provider": "FINNHUB", "symbol": "AAPL", "last": 10.0}]
    }
    assert seen["args"] == ("AAPL", "EQUITY")


def test_provider_health_uses_the_async_client(monkeypatch):
    with _client() as (client, state):
        http_mod = cast(Any, sys.modules["core.runtime"])
        tracker_mod = cast(Any, sys.modules["core.quotes"])

        def handler(request: httpx2.Request) -> httpx2.Response:
            host = request.url.host
            if host == "finnhub.io":
                return httpx2.Response(200, json={"c": 10.5, "t": 1_700_000_000})
            if host == "api.twelvedata.com":
                return httpx2.Response(
                    200,
                    json={"close": "11.5", "currency": "USD", "timestamp": 1_700_000_000},
                )
            if host == "www.alphavantage.co":
                return httpx2.Response(
                    200,
                    json={
                        "Global Quote": {
                            "01. symbol": "AAPL",
                            "05. price": "12.5",
                            "07. latest trading day": "2024-01-02",
                        }
                    },
                )
            return httpx2.Response(404, json={"error": host})

        http_mod.http_client = httpx2.AsyncClient(transport=httpx2.MockTransport(handler))
        caps = {"minute": None, "hourly": None, "daily": None, "monthly": None}
        state.quote_service.tracker = tracker_mod.RequestTracker(
            {
                "finnhub": dict(caps),
                "alpha_vantage": dict(caps),
                "twelve_data": dict(caps),
            }
        )

        async def _record(provider, symbol=None):
            return None

        state.quote_service.tracker.record = _record
        for engine in state.quote_service.providers.values():
            engine.api_key = "test-key"

        response = client.get("/providers/health")

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ok"
    assert body["ok"] == 3
    assert set(body["providers"]) == {"finnhub", "alpha_vantage", "twelve_data"}


def test_stream_yields_one_frame_from_the_subscriber_queue(monkeypatch):
    monkeypatch.setenv("MARKET_DATA_MOCK_STREAM", "false")
    with _client() as (client, _state):
        stream_hub = cast(Any, sys.modules["core.stream"])
        message = 'event: quote\ndata: {"symbol": "AAPL"}\n\n'

        class ScriptedQueue(asyncio.Queue):
            async def get(self):
                if not getattr(self, "_sent", False):
                    self._sent = True
                    return message
                return None

        monkeypatch.setattr(stream_hub.asyncio, "Queue", ScriptedQueue)
        response = client.get("/market-data/stream")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.content == b": connected\n\n" + message.encode()
    assert stream_hub.subscribers == []
