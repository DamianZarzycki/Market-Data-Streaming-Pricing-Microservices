import asyncio
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

from fastapi.testclient import TestClient

from service_loader import load_service

_HEALTH = {
    "service": "market-data-service",
    "status": "UP",
    "generated_events": 0,
    "last_event_time": None,
    "last_symbol": None,
    "last_asset_type": None,
}


@contextmanager
def _client() -> Iterator[tuple[TestClient, Any]]:
    app_module = load_service("market-data-service")
    # Loaded by path at runtime, so the static type is a plain module.
    worker_mod = cast(Any, sys.modules["worker"])

    async def _idle():
        await asyncio.Event().wait()

    async def _quiet(*args, **kwargs):
        return None

    worker_mod.market_worker = _idle
    worker_mod.metric_worker = _idle
    worker_mod.db_worker = _idle
    worker_mod.audit_logger.ainfo = _quiet
    worker_mod.audit_logger.aerror = _quiet
    with TestClient(app_module.app) as client:
        worker_mod.subscribers.clear()
        worker_mod.market_tick_data_state = {}
        worker_mod.health_stats = dict(_HEALTH)
        yield client, worker_mod


def test_health_includes_active_subscribers():
    with _client() as (client, worker_mod):
        worker_mod.health_stats["generated_events"] = 7
        worker_mod.health_stats["last_symbol"] = "ACME"
        worker_mod.subscribers.append({"id": "abc", "queue": None})

        response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["service"] == "market-data-service"
    assert body["status"] == "UP"
    assert body["generated_events"] == 7
    assert body["last_symbol"] == "ACME"
    assert body["active_subscribers"] == 1


def test_symbols_groups_snapshot_by_asset_class():
    with _client() as (client, worker_mod):
        worker_mod.market_tick_data_state = {
            "ACME": {"asset_type": "EQUITY", "symbol": "ACME"},
            "MSFT": {"asset_type": "EQUITY", "symbol": "MSFT"},
            "EURUSD": {"asset_type": "FX", "symbol": "EURUSD"},
            "blank-class": {"asset_type": "", "symbol": "NOPE"},
            "blank-symbol": {"asset_type": "BOND", "symbol": ""},
            "dup": {"asset_type": "EQUITY", "symbol": "ACME"},
        }

        response = client.get("/symbols")

    assert response.status_code == 200
    assert response.json() == {"EQUITY": ["ACME", "MSFT"], "FX": ["EURUSD"]}


def test_snapshot_returns_a_copy_and_skips_the_database():
    with _client() as (client, worker_mod):
        seen = []

        async def ainfo(*args, **kwargs):
            seen.append(args)

        worker_mod.audit_logger.ainfo = ainfo
        worker_mod.market_tick_data_state = {"ACME": {"symbol": "ACME", "last": 10}}

        response = client.get("/snapshot")

        assert response.status_code == 200
        assert response.json() == {"ACME": {"symbol": "ACME", "last": 10}}
        assert len(seen) == 1

        view = asyncio.run(worker_mod.snapshot_view())
        view["NEW"] = {"symbol": "NEW"}
        assert "NEW" not in worker_mod.market_tick_data_state


def test_stream_yields_one_message_and_unsubscribes(monkeypatch):
    with _client() as (client, worker_mod):
        message = 'data: {"symbol": "ACME"}\n\n'
        seen = {}

        class ScriptedQueue(asyncio.Queue):
            async def get(self):
                # subscribe() has already stored this queue on the subscriber.
                seen["subscribers"] = len(worker_mod.subscribers)
                if not getattr(self, "_sent", False):
                    self._sent = True
                    return message
                return None

        # Restored after the test so later imports still get asyncio.Queue.
        monkeypatch.setattr(worker_mod.asyncio, "Queue", ScriptedQueue)
        response = client.get("/stream")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    assert response.content == message.encode()
    assert seen["subscribers"] == 1
    assert worker_mod.subscribers == []
