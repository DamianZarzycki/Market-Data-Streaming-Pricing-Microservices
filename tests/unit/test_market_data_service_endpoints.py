import sys

import pytest

from service_loader import collect, invoke, load_service, read_json


@pytest.fixture(scope="module")
def market():
    app = load_service("market-data-service")
    return {"app": app, "worker": sys.modules["worker"]}


@pytest.fixture(autouse=True)
def _reset(market):
    worker = market["worker"]
    worker.subscribers.clear()
    worker.market_tick_data_state = {}
    worker.health_stats = {
        "service": "market-data-service",
        "status": "UP",
        "generated_events": 0,
        "last_event_time": None,
        "last_symbol": None,
        "last_asset_type": None,
    }
    yield


def test_health_includes_active_subscribers(market):
    worker = market["worker"]
    worker.health_stats["generated_events"] = 7
    worker.health_stats["last_symbol"] = "ACME"
    worker.subscribers.append({"id": "abc", "queue": None})

    status, _, body = read_json(market["app"].app, "/health")

    assert status == 200
    assert body["service"] == "market-data-service"
    assert body["status"] == "UP"
    assert body["generated_events"] == 7
    assert body["last_symbol"] == "ACME"
    assert body["active_subscribers"] == 1


def test_symbols_groups_snapshot_by_asset_class(market):
    market["worker"].market_tick_data_state = {
        "ACME": {"asset_type": "EQUITY", "symbol": "ACME"},
        "MSFT": {"asset_type": "EQUITY", "symbol": "MSFT"},
        "EURUSD": {"asset_type": "FX", "symbol": "EURUSD"},
        "blank-class": {"asset_type": "", "symbol": "NOPE"},
        "blank-symbol": {"asset_type": "BOND", "symbol": ""},
        "dup": {"asset_type": "EQUITY", "symbol": "ACME"},
    }

    status, _, body = read_json(market["app"].app, "/symbols")

    assert status == 200
    assert body == {"EQUITY": ["ACME", "MSFT"], "FX": ["EURUSD"]}


def test_snapshot_returns_a_copy_and_skips_the_database(market, monkeypatch):
    created = []

    class SilentAudit:
        def __init__(self, service_name):
            created.append(service_name)

        def info(self, *args, **kwargs):
            return None

    monkeypatch.setattr(market["app"], "AuditLogger", SilentAudit)
    market["worker"].market_tick_data_state = {"ACME": {"symbol": "ACME", "last": 10}}

    status, _, body = read_json(market["app"].app, "/snapshot")
    assert status == 200
    assert body == {"ACME": {"symbol": "ACME", "last": 10}}
    assert created == ["market-data-service"]

    view = market["app"].snapshot()
    view["NEW"] = {"symbol": "NEW"}
    assert "NEW" not in market["worker"].market_tick_data_state


def test_stream_yields_one_message_and_unsubscribes(market, monkeypatch):
    seen = {}

    class ScriptedQueue:
        def get(self):
            seen["subscribers"] = len(market["worker"].subscribers)
            if "sent" not in seen:
                seen["sent"] = True
                return 'data: {"symbol": "ACME"}\n\n'
            raise RuntimeError("stop stream")

    monkeypatch.setattr(market["app"].queue, "Queue", lambda: ScriptedQueue())

    status, headers, iterator = invoke(market["app"].app, "/stream")
    assert status == 200
    assert headers["content-type"].startswith("text/event-stream")
    assert len(market["worker"].subscribers) == 1

    body, error = collect(iterator)

    assert error is None
    assert body == b'data: {"symbol": "ACME"}\n\n'
    assert seen["subscribers"] == 1
    assert market["worker"].subscribers == []
