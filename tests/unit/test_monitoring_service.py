import asyncio
import os
import sys
import threading
from pathlib import Path
from unittest.mock import patch

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+psycopg://localhost/monitoring_test",
)

BACKEND = Path(__file__).resolve().parents[2]
SERVICE = BACKEND / "services" / "monitoring-service"
sys.path.insert(0, str(BACKEND))
sys.path.insert(0, str(SERVICE))

import httpx
from fastapi.testclient import TestClient

import app
import worker
from shared.trading_shared.enums import ServiceStatus

STARTUP_STATUS = {
    "services": {},
    "environment": {},
    "kpis": {},
    "alerts": [],
}


def _run(handler, url="http://market-data-service:8001/health", name="market-data-service"):
    async def _probe():
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport, timeout=2.0) as client:
            return await worker.check_service_health(client, url, name)

    return asyncio.run(_probe())


def test_probe_json_health_is_up():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"status": "UP", "last_event_time": "2026-10-22T00:00:00.000Z"},
        )

    result = _run(handler)

    assert result["status"] == ServiceStatus.UP.value
    assert result["last_event_time"] == "2026-10-22T00:00:00.000Z"
    assert isinstance(result["response_time_ms"], int)
    assert "error" not in result


def test_probe_non_json_health_is_up():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not-json")

    result = _run(handler)

    assert result["status"] == ServiceStatus.UP.value
    assert "error" not in result


def test_probe_http_error_is_down():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="nope")

    result = _run(handler)

    assert result["status"] == ServiceStatus.DOWN.value
    assert result["error"] == "HTTP Error 500: Internal Server Error"


def test_probe_connect_and_timeout_are_down():
    def connect_error(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("[Errno 111] Connection refused")

    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out")

    refused = _run(connect_error)
    timed_out = _run(timeout)

    assert refused["status"] == ServiceStatus.DOWN.value
    assert refused["error"] == "Connection refused"
    assert timed_out["status"] == ServiceStatus.DOWN.value
    assert timed_out["error"] == "timed out"


def test_lifespan_starts_worker_and_health_does_not_probe():
    entered = threading.Event()

    async def stand_in(client):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            raise

    worker.status_payload = dict(STARTUP_STATUS)

    with patch("app.worker.monitoring_worker", stand_in), patch("app.AuditLogger.info"):
        with TestClient(app.app) as client:
            assert entered.wait(timeout=2)
            health = client.get("/health")
            status_response = client.get("/status")

    assert health.status_code == 200
    assert health.json() == {
        "service": "monitoring-service",
        "status": ServiceStatus.UP.value,
    }
    body = status_response.json()
    assert body["services"] == {}
    assert body["environment"] == {}
    assert body["kpis"] == {}
