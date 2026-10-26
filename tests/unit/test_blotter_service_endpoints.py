import sys

import pytest

from service_loader import collect, invoke, load_service, read_json


@pytest.fixture(scope="module")
def blotter():
    app = load_service("blotter-service")
    return {
        "app": app,
        "trades": sys.modules["trades_service"],
        "cache": sys.modules["cache.valuation_cache_service"],
    }


@pytest.fixture(autouse=True)
def _reset(blotter):
    blotter["cache"].valuation_cache.clear()
    yield


def test_health_reads_the_in_memory_cache(blotter):
    blotter["cache"].valuation_cache["t1"] = [{"fair_value": 1}, {"fair_value": 2}]
    blotter["cache"].valuation_cache["t2"] = [{"fair_value": 3}]

    status, _, body = read_json(blotter["app"].app, "/health")

    assert status == 200
    assert body["service"] == "blotter-service"
    assert body["status"] == "UP"
    assert body["cache_trade_count"] == 2
    assert body["cache_entry_count"] == 3


def test_book_summary_returns_payload_or_500(blotter, monkeypatch):
    monkeypatch.setattr(
        blotter["trades"],
        "fetch_book_summary",
        lambda: {"books": [{"book_id": "b1"}]},
    )
    status, _, body = read_json(blotter["app"].app, "/books/summary")
    assert status == 200
    assert body == {"books": [{"book_id": "b1"}]}

    def explode():
        raise RuntimeError("books-service down")

    monkeypatch.setattr(blotter["trades"], "fetch_book_summary", explode)
    status, _, body = read_json(blotter["app"].app, "/books/summary")
    assert status == 500
    assert body == {"error": "Failed to fetch book summary"}


def test_trades_forwards_query_filters(blotter, monkeypatch):
    captured = {}

    def fetch_trades(filters=None):
        captured["filters"] = filters
        return [{"trade_id": "t1"}]

    monkeypatch.setattr(blotter["trades"], "fetch_trades", fetch_trades)
    query = "book_id=b1&asset_class=EQUITY&status=ACTIVE&symbol=ACME&first_only=yes&page=2&limit=5"

    status, _, body = read_json(blotter["app"].app, "/trades", query)

    assert status == 200
    assert body == {"trades": [{"trade_id": "t1"}]}
    assert captured["filters"] == {
        "book_id": "b1",
        "asset_class": "EQUITY",
        "status": "ACTIVE",
        "symbol": "ACME",
        "first_only": "yes",
        "page": "2",
        "limit": "5",
    }


def test_trades_without_query_uses_first_only_false(blotter, monkeypatch):
    captured = {}

    def fetch_trades(filters=None):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(blotter["trades"], "fetch_trades", fetch_trades)

    status, _, body = read_json(blotter["app"].app, "/trades")

    assert status == 200
    assert body == {"trades": []}
    assert captured["filters"]["first_only"] is False
    assert captured["filters"]["book_id"] is None
    assert captured["filters"]["page"] is None
    assert captured["filters"]["limit"] is None


def test_trade_by_id_found_and_missing(blotter, monkeypatch):
    monkeypatch.setattr(
        blotter["trades"],
        "fetch_trade_by_id",
        lambda trade_id: {"trade": {"trade_id": trade_id}} if trade_id == "t1" else None,
    )

    status, _, body = read_json(blotter["app"].app, "/trades/t1")
    assert status == 200
    assert body == {"trade": {"trade_id": "t1"}}

    status, _, body = read_json(blotter["app"].app, "/trades/missing")
    assert status == 404
    assert body == {"error": "Trade not found"}


def test_trade_valuations_wraps_the_list(blotter, monkeypatch):
    monkeypatch.setattr(
        blotter["trades"],
        "fetch_trade_valuations",
        lambda trade_id: [{"trade_id": trade_id, "fair_value": 1}],
    )

    status, _, body = read_json(blotter["app"].app, "/trades/t1/valuations")

    assert status == 200
    assert body == {"valuations": [{"trade_id": "t1", "fair_value": 1}]}


def test_trade_audit_logs_is_an_empty_200(blotter):
    status, _, iterator = invoke(blotter["app"].app, "/trades/t1/audit-logs")
    body, error = collect(iterator)

    assert status == 200
    assert error is None
    assert body == b""
