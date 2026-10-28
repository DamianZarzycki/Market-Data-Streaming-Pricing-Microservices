import sys
import threading
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from service_loader import load_service

TRADE_ID = "11111111-1111-1111-1111-111111111111"
NIL_ID = "00000000-0000-0000-0000-000000000000"


@contextmanager
def _client():
    app_module = load_service("blotter-service")
    worker_mod = sys.modules["worker"]
    trades = sys.modules["trades_service"]
    cache = sys.modules["cache.valuation_cache_service"]
    cache.valuation_cache.clear()
    worker_mod.stop_event.clear()
    started = threading.Event()

    def _wait():
        started.set()
        worker_mod.stop_event.wait()

    worker_mod.valuation_worker = _wait
    with patch.object(app_module.AuditLogger, "info", lambda *args, **kwargs: None):
        with TestClient(app_module.app, raise_server_exceptions=False) as client:
            assert started.wait(2)
            yield client, trades, cache, worker_mod


def test_health_reads_the_in_memory_cache():
    with _client() as (client, _, cache, _):
        cache.valuation_cache["t1"] = [{"fair_value": 1}, {"fair_value": 2}]
        cache.valuation_cache["t2"] = [{"fair_value": 3}]

        response = client.get("/health")

    assert response.status_code == 200
    body = response.json()
    assert body["service"] == "blotter-service"
    assert body["status"] == "UP"
    assert body["cache_trade_count"] == 2
    assert body["cache_entry_count"] == 3


def test_book_summary_returns_payload_or_500():
    with _client() as (client, trades, _, _):
        trades.fetch_books_payload = lambda: {
            "books": [{"book_id": "b1", "name": "Equity"}]
        }
        trades._active_counts = lambda session: {}

        response = client.get("/books/summary")

        assert response.status_code == 200
        assert response.json() == {
            "books": [
                {
                    "book_id": "b1",
                    "name": "Equity",
                    "realized_pnl": 0.0,
                    "unrealized_pnl": 0.0,
                    "active_trades": 0,
                }
            ]
        }

        def explode():
            raise RuntimeError("books-service down")

        trades.fetch_books_payload = explode
        failed = client.get("/books/summary")

    assert failed.status_code == 500
    assert failed.json() == {"error": "Failed to fetch book summary"}


def test_book_summary_turns_an_aggregation_error_into_500():
    with _client() as (client, trades, _, _):
        trades.fetch_books_payload = lambda: {"books": []}

        def explode(payload, session):
            raise RuntimeError("db down")

        trades.build_book_summary = explode
        response = client.get("/books/summary")

    assert response.status_code == 500
    assert response.json() == {"error": "Failed to fetch book summary"}


def test_trades_forwards_query_filters():
    captured = {}

    def fetch_trades(session, **filters):
        captured.update(filters)
        return [{"trade_id": "t1", "valuation": None}]

    with _client() as (client, trades, _, _):
        trades.fetch_trades = fetch_trades
        response = client.get(
            "/trades",
            params={
                "book_id": "b1",
                "asset_class": "EQUITY",
                "status": "ACTIVE",
                "symbol": "ACME",
                "first_only": "yes",
                "page": 2,
                "limit": 5,
            },
        )

    assert response.status_code == 200
    assert response.json() == {"trades": [{"trade_id": "t1", "valuation": None}]}
    assert captured == {
        "book_id": "b1",
        "asset_class": "EQUITY",
        "status": "ACTIVE",
        "symbol": "ACME",
        "first_only": True,
        "page": 2,
        "limit": 5,
    }


def test_trades_without_query_uses_defaults():
    captured = {}

    def fetch_trades(session, **filters):
        captured.update(filters)
        return []

    with _client() as (client, trades, _, _):
        trades.fetch_trades = fetch_trades
        response = client.get("/trades")

    assert response.status_code == 200
    assert response.json() == {"trades": []}
    assert captured["first_only"] is False
    assert captured["book_id"] is None
    assert captured["page"] == 1
    assert captured["limit"] == 50


def test_first_only_false_stays_false_and_the_body_is_a_list():
    captured = {}

    def fetch_trades(session, **filters):
        captured.update(filters)
        return []

    with _client() as (client, trades, _, _):
        trades.fetch_trades = fetch_trades
        response = client.get("/trades", params={"first_only": "false"})

    assert response.status_code == 200
    assert response.json() == {"trades": []}
    assert captured["first_only"] is False


def test_fetch_trades_wraps_one_row_in_a_list(monkeypatch):
    load_service("blotter-service")
    trades = sys.modules["trades_service"]
    seen = {}

    class Repo:
        def __init__(self, session):
            pass

        def get_trades(self, **kwargs):
            seen.update(kwargs)
            return object()

    monkeypatch.setattr(trades, "TradeRepository", Repo)
    monkeypatch.setattr(trades, "serialize", lambda row: {"trade_id": "t1"})

    result = trades.fetch_trades(
        None,
        book_id=None,
        asset_class=None,
        status=None,
        symbol=None,
        first_only=True,
        page=1,
        limit=50,
    )

    assert seen["first_only"] is True
    assert result == [{"trade_id": "t1", "valuation": None}]


def test_invalid_page_or_limit_is_400_json():
    with _client() as (client, _, _, _):
        for params in ({"page": 0}, {"limit": 201}, {"page": "nope"}, {"limit": 0}):
            response = client.get("/trades", params=params)
            assert response.status_code == 400
            assert response.json() == {"error": "Invalid page or limit"}


def test_trade_by_id_found_missing_and_not_a_uuid():
    def fetch_trade_by_id(session, trade_id):
        if trade_id == TRADE_ID:
            return {
                "trade": {"trade_id": trade_id},
                "latest_valuation": None,
                "valuation_history": [],
                "audit_logs": [],
            }
        return None

    with _client() as (client, trades, _, _):
        trades.fetch_trade_by_id = fetch_trade_by_id

        found = client.get("/trades/" + TRADE_ID)
        missing = client.get("/trades/" + NIL_ID)
        invalid = client.get("/trades/not-a-uuid")

    assert found.status_code == 200
    assert found.json()["trade"] == {"trade_id": TRADE_ID}
    assert found.json()["audit_logs"] == []
    assert missing.status_code == 404
    assert missing.json() == {"error": "Trade not found"}
    assert invalid.status_code == 404
    assert invalid.json() == {"error": "Trade not found"}


def test_trade_valuations_wraps_the_list():
    with _client() as (client, trades, _, _):
        trades.fetch_trade_valuations = lambda session, trade_id: [
            {"trade_id": trade_id, "fair_value": 1}
        ]
        response = client.get("/trades/" + TRADE_ID + "/valuations")

    assert response.status_code == 200
    assert response.json() == {
        "valuations": [{"trade_id": TRADE_ID, "fair_value": 1}]
    }


def test_trade_audit_logs_is_an_empty_200():
    with _client() as (client, _, _, _):
        response = client.get("/trades/" + NIL_ID + "/audit-logs")

    assert response.status_code == 200
    assert response.content == b""


def test_lifespan_starts_the_worker_and_closes_the_stream():
    with _client() as (client, _, _, worker_mod):
        response = client.get("/health")
        stream = MagicMock()
        worker_mod.hold_stream(stream)

    assert response.status_code == 200
    stream.close.assert_called_once()
