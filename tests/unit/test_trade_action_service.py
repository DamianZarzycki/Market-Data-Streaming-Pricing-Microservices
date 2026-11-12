import asyncio
import sys
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

BACKEND = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND))

from service_loader import load_service


def _open(client_request_id="req-1", **extra):
    body = {"action_type": "OPEN_TRADE", "client_request_id": client_request_id}
    body.update(extra)
    return body


class _Quotes:
    def __init__(self, quote):
        self.quote = quote
        self.calls = 0

    async def get_latest_freshness_async(self, provider, symbol):
        self.calls += 1
        return self.quote


class _DB:
    def __init__(self, quotes):
        self.provider_quotes = quotes

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@contextmanager
def _client():
    app_module = load_service("trade-action-service")
    worker_mod = sys.modules["worker"]
    metrics_mod = sys.modules["metrics"]
    submit_mod = sys.modules["actions.submit"]
    sys.modules["actions.provider_mode"].set_use_provider_data(False)
    worker_mod.trade_queue = asyncio.Queue(maxsize=metrics_mod.QUEUE_MAXSIZE)

    async def _park():
        try:
            await asyncio.Event().wait()
        finally:
            metrics_mod.set_worker_running(False)

    stack = ExitStack()
    try:
        stack.enter_context(patch.object(worker_mod, "trade_action_worker", _park))
        stack.enter_context(patch.object(app_module.AuditLogger, "info"))
        stack.enter_context(patch.object(app_module.AuditLogger, "warning"))
        stack.enter_context(patch.object(app_module.AuditLogger, "error"))
        stack.enter_context(patch.object(app_module.AuditLogger, "ainfo", new_callable=AsyncMock))
        audit = stack.enter_context(patch.object(submit_mod, "_audit", AsyncMock()))
        client = stack.enter_context(
            TestClient(app_module.app, raise_server_exceptions=False)
        )
        yield client, worker_mod, metrics_mod, audit
    finally:
        stack.close()


def _quote(**overrides):
    now = datetime.now(timezone.utc)
    fields = {
        "asset_class": "EQUITY",
        "received_at": now,
        "provider_timestamp": now,
    }
    fields.update(overrides)
    return SimpleNamespace(**fields)


def _post_with_quote(client, quote, body):
    quotes = _Quotes(quote)
    staleness = sys.modules["actions.staleness"]
    with patch.object(staleness, "AsyncDBSessionManager", return_value=_DB(quotes)):
        response = client.post("/trade-actions", json=body)
    return response, quotes


def test_health_is_up_while_the_worker_runs():
    with _client() as (client, _, metrics_mod, _):
        health = client.get("/health")
        status = client.get("/status")
    assert health.status_code == 200, health.text
    assert health.json() == {"service": "trade-action-service", "status": "UP"}
    assert status.json()["worker_running"] is True
    assert status.json()["service"] == "trade-action-service"
    assert metrics_mod.is_worker_running() is False


def test_health_reports_down_when_the_worker_flag_is_clear():
    with _client() as (client, _, metrics_mod, _):
        metrics_mod.set_worker_running(False)
        health = client.get("/health")
    assert health.status_code == 200, health.text
    assert health.json()["status"] == "DOWN"


def test_open_is_accepted_before_the_row_exists_and_keeps_extra_fields():
    with _client() as (client, worker_mod, _, audit):
        response = client.post(
            "/trade-actions",
            json=_open(strike=100, book_id="book-1", symbol="AAPL"),
        )
        status = client.get("/status")
        queued = worker_mod.trade_queue.get_nowait()
    assert response.status_code == 202, response.text
    assert response.json() == {
        "message": "Trade action accepted for processing",
        "client_request_id": "req-1",
    }
    assert queued["strike"] == 100
    assert queued["book_id"] == "book-1"
    assert "_enqueued_at" in queued
    assert status.json()["queue"]["depth"] == 1
    audit.assert_not_awaited()


def test_close_is_accepted_before_the_trade_is_loaded():
    with _client() as (client, worker_mod, _, _):
        response = client.post(
            "/trade-actions",
            json={"action_type": "CLOSE_TRADE", "client_request_id": "close-1"},
        )
    assert response.status_code == 202, response.text
    assert worker_mod.trade_queue.get_nowait()["action_type"] == "CLOSE_TRADE"


def test_missing_client_request_id_is_400_and_audited():
    with _client() as (client, worker_mod, _, audit):
        response = client.post("/trade-actions", json={"action_type": "OPEN_TRADE"})
    assert response.status_code == 400, response.text
    assert response.json() == {"error": "Missing client_request_id"}
    assert worker_mod.trade_queue.empty()
    audit.assert_awaited_once()


def test_empty_object_and_invalid_json_are_400():
    with _client() as (client, _, _, audit):
        empty = client.post("/trade-actions", json={})
        invalid = client.post(
            "/trade-actions",
            content=b"{",
            headers={"content-type": "application/json"},
        )
        listed = client.post("/trade-actions", json=[_open()])
    assert empty.status_code == 400, empty.text
    assert empty.json() == {"error": "Missing payload data"}
    assert invalid.status_code == 400, invalid.text
    assert invalid.json() == {"error": "Invalid JSON format"}
    assert listed.status_code == 400, listed.text
    assert listed.json() == {"error": "Missing payload data"}
    audit.assert_not_awaited()


def test_full_queue_is_503_without_accepted_so_far():
    with _client() as (client, worker_mod, _, audit):
        worker_mod.trade_queue = asyncio.Queue(maxsize=1)
        worker_mod.trade_queue.put_nowait({"already": True})
        response = client.post("/trade-actions", json=_open())
    assert response.status_code == 503, response.text
    assert response.json() == {"error": "System overloaded, please try again later"}
    audit.assert_awaited_once()


def test_provider_mode_requires_a_real_boolean():
    with _client() as (client, _, _, _):
        enabled = client.put("/provider-mode", json={"use_provider_data": True, "ignored": 1})
        text = client.put("/provider-mode", json={"use_provider_data": "true"})
        missing = client.put("/provider-mode", json={})
        number = client.put("/provider-mode", json={"use_provider_data": 1})
        disabled = client.put("/provider-mode", json={"use_provider_data": False})
    assert enabled.status_code == 200, enabled.text
    assert enabled.json() == {"use_provider_data": True}
    assert text.status_code == 400, text.text
    assert text.json() == {"error": "use_provider_data must be a boolean"}
    assert missing.status_code == 400, missing.text
    assert missing.json() == {"error": "use_provider_data is required"}
    assert number.status_code == 400, number.text
    assert number.json() == {"error": "use_provider_data must be a boolean"}
    assert disabled.status_code == 200, disabled.text
    assert disabled.json() == {"use_provider_data": False}


def test_provider_mode_open_requires_the_quote_fields():
    with _client() as (client, worker_mod, _, audit):
        client.put("/provider-mode", json={"use_provider_data": True})
        response = client.post("/trade-actions", json=_open(symbol="AAPL"))
    assert response.status_code == 400, response.text
    assert response.json() == {"error": "Missing market_data_provider"}
    assert worker_mod.trade_queue.empty()
    audit.assert_awaited_once()


def test_stale_quote_is_rejected_with_its_detail():
    with _client() as (client, worker_mod, _, audit):
        quote = _quote(received_at=datetime.now(timezone.utc) - timedelta(seconds=500))
        response, _ = _post_with_quote(
            client,
            quote,
            _open(symbol="AAPL", market_data_provider="ALPHA"),
        )
    assert response.status_code == 400, response.text
    assert "old" in response.json()["error"]
    assert worker_mod.trade_queue.empty()
    audit.assert_awaited_once()


def test_fresh_quote_is_accepted():
    with _client() as (client, worker_mod, _, _):
        response, _ = _post_with_quote(
            client,
            _quote(),
            _open(symbol="AAPL", market_data_provider="ALPHA"),
        )
    assert response.status_code == 202, response.text
    assert worker_mod.trade_queue.get_nowait()["market_data_provider"] == "ALPHA"


def test_null_provider_timestamp_is_400_not_500():
    with _client() as (client, _, _, _):
        response, _ = _post_with_quote(
            client,
            _quote(provider_timestamp=None),
            _open(symbol="AAPL", market_data_provider="ALPHA"),
        )
    assert response.status_code == 400, response.text
    assert "no timestamp" in response.json()["error"]


def test_null_received_at_is_400():
    with _client() as (client, _, _, _):
        response, _ = _post_with_quote(
            client,
            _quote(received_at=None),
            _open(symbol="AAPL", market_data_provider="ALPHA"),
        )
    assert response.status_code == 400, response.text
    assert "received_at" in response.json()["error"]


def test_missing_quote_is_400():
    with _client() as (client, _, _, _):
        response, quotes = _post_with_quote(
            client,
            None,
            _open(symbol="AAPL", market_data_provider="ALPHA"),
        )
    assert response.status_code == 400, response.text
    assert "No quote on file" in response.json()["error"]
    assert quotes.calls == 1


def test_provider_without_a_symbol_does_not_read_a_quote():
    with _client() as (client, _, _, _):
        response, quotes = _post_with_quote(
            client,
            _quote(),
            _open(market_data_provider="ALPHA"),
        )
    assert response.status_code == 400, response.text
    assert response.json()["error"] == "No provider or symbol specified"
    assert quotes.calls == 0


def test_batch_keeps_accepted_items_when_one_item_is_bad():
    with _client() as (client, worker_mod, _, audit):
        response = client.post(
            "/trade-actions/batch",
            json=[_open("ok-1"), {"action_type": "OPEN_TRADE"}, "nope", _open("ok-2")],
        )
    assert response.status_code == 202, response.text
    body = response.json()
    assert body["accepted_count"] == 2
    assert body["message"] == "Batch processed"
    assert body["errors"] == [
        "Item 1: Missing client_request_id",
        "Item 2: payload must be an object",
    ]
    assert worker_mod.trade_queue.qsize() == 2
    audit.assert_not_awaited()


def test_batch_of_only_invalid_items_is_still_202():
    with _client() as (client, worker_mod, _, _):
        response = client.post("/trade-actions/batch", json=[{}, "x"])
    assert response.status_code == 202, response.text
    assert response.json()["accepted_count"] == 0
    assert len(response.json()["errors"]) == 2
    assert worker_mod.trade_queue.empty()


def test_batch_stops_on_a_full_queue_and_reports_what_was_queued():
    with _client() as (client, worker_mod, _, audit):
        worker_mod.trade_queue = asyncio.Queue(maxsize=1)
        response = client.post(
            "/trade-actions/batch",
            json=[_open("first"), _open("second")],
        )
        queued = worker_mod.trade_queue.get_nowait()
    assert response.status_code == 503, response.text
    assert response.json() == {
        "error": "System overloaded, queue is full",
        "accepted_so_far": 1,
    }
    assert queued["client_request_id"] == "first"
    audit.assert_awaited_once()


def test_batch_over_100_is_400_before_anything_is_queued():
    with _client() as (client, worker_mod, _, audit):
        response = client.post(
            "/trade-actions/batch",
            json=[_open(f"id-{index}") for index in range(101)],
        )
    assert response.status_code == 400, response.text
    assert response.json() == {"error": "Batch exceeds the maximum of 100 trade actions"}
    assert worker_mod.trade_queue.empty()
    audit.assert_not_awaited()


def test_batch_shape_errors_are_400():
    with _client() as (client, _, _, _):
        empty = client.post("/trade-actions/batch", json=[])
        obj = client.post("/trade-actions/batch", json=_open())
    assert empty.status_code == 400, empty.text
    assert empty.json() == {"error": "Payload must be a non-empty list of trade actions"}
    assert obj.status_code == 400, obj.text
    assert obj.json() == {"error": "Payload must be a non-empty list of trade actions"}


def test_request_body_over_the_cap_is_413():
    from shared.trading_shared.http_errors import MAX_BODY_BYTES

    with _client() as (client, _, _, _):
        response = client.post(
            "/trade-actions",
            content=b"x" * (MAX_BODY_BYTES + 1),
            headers={"content-type": "application/json"},
        )
    assert response.status_code == 413, response.text
    assert response.json() == {"error": "Request body is too large"}


def test_image_runs_one_worker_on_8080():
    text = (BACKEND / "services" / "trade-action-service" / "Dockerfile").read_text()
    assert '--workers", "1"' in text
    assert "8080" in text
