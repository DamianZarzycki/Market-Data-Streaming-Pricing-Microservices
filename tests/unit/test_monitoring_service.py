import asyncio
import os
import sys
import threading
import time
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

import httpx2
from fastapi.testclient import TestClient

import app
import worker
from shared.trading_shared.enums import ServiceStatus
from state import PROBE_TTL_SECONDS, CachedProbe, state


def _run(handler, url="http://market-data-service:8001/health", name="market-data-service"):
    async def _probe():
        transport = httpx2.MockTransport(handler)
        async with httpx2.AsyncClient(transport=transport, timeout=2.0) as client:
            return await worker.check_service_health(client, url, name)

    return asyncio.run(_probe())


def test_probe_json_health_is_up():
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(
            200,
            json={"status": "UP", "last_event_time": "2026-10-22T00:00:00.000Z"},
        )

    result = _run(handler)

    assert result["status"] == ServiceStatus.UP.value
    assert result["last_event_time"] == "2026-10-22T00:00:00.000Z"
    assert isinstance(result["response_time_ms"], int)
    assert "error" not in result


def test_probe_non_json_health_is_up():
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, content=b"not-json")

    result = _run(handler)

    assert result["status"] == ServiceStatus.UP.value
    assert "error" not in result


def test_probe_http_error_is_down():
    def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(500, text="nope")

    result = _run(handler)

    assert result["status"] == ServiceStatus.DOWN.value
    assert result["error"] == "HTTP Error 500: Internal Server Error"


def test_probe_connect_and_timeout_are_down():
    def connect_error(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ConnectError("[Errno 111] Connection refused")

    def timeout(request: httpx2.Request) -> httpx2.Response:
        raise httpx2.ReadTimeout("timed out")

    refused = _run(connect_error)
    timed_out = _run(timeout)

    assert refused["status"] == ServiceStatus.DOWN.value
    assert refused["error"] == "Connection refused"
    assert timed_out["status"] == ServiceStatus.DOWN.value
    assert timed_out["error"] == "timed out"


def _hold_worker():
    entered = threading.Event()

    async def stand_in(client):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            raise

    return entered, stand_in


def _probe(status, **extra):
    body = {
        "status": status,
        "last_checked": "2026-10-22T00:00:00.000Z",
        "response_time_ms": 4,
    }
    body.update(extra)
    return body


def test_lifespan_starts_worker_and_health_does_not_probe():
    entered, stand_in = _hold_worker()
    state.reset()

    with patch.object(app.worker, "monitoring_worker", stand_in), patch.object(app.AuditLogger, "info"):
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


def test_expired_probe_is_absent_from_status_and_cache():
    entered, stand_in = _hold_worker()
    state.reset()
    now = time.time()
    state.health_cache["market-data-service"] = CachedProbe(
        probe=_probe(ServiceStatus.UP.value),
        expires_at=now - 1,
    )
    state.health_cache["pricing-service"] = CachedProbe(
        probe=_probe(ServiceStatus.UP.value),
        expires_at=now + PROBE_TTL_SECONDS,
    )

    with patch.object(app.worker, "monitoring_worker", stand_in), patch.object(app.AuditLogger, "info"):
        with TestClient(app.app) as client:
            assert entered.wait(timeout=2)
            body = client.get("/status").json()

    assert "market-data-service" not in state.health_cache
    assert "market-data-service" not in body["services"]
    assert "pricing-service" in state.health_cache
    assert body["services"]["pricing-service"]["status"] == ServiceStatus.UP.value


def test_error_window_keeps_one_recent_down_edge():
    state.reset()
    now = time.time()
    state.record_error("book-service", now - 301)
    down = _probe(ServiceStatus.DOWN.value, error="gone")
    worker.apply_probe("pricing-service", down, now - 5)
    worker.apply_probe("pricing-service", down, now)

    assert state.errors_by_service(now) == {"pricing-service": 1}


def test_expired_previous_status_counts_a_new_down_edge():
    state.reset()
    now = time.time()
    state.remember_status(
        "pricing-service",
        ServiceStatus.DOWN.value,
        now - PROBE_TTL_SECONDS - 1,
    )

    worker.apply_probe("pricing-service", _probe(ServiceStatus.DOWN.value, error="gone"), now)

    assert state.errors_by_service(now) == {"pricing-service": 1}


def test_probe_lap_starts_every_target_before_any_returns():
    state.reset()
    entered = []
    release = asyncio.Event()

    async def fake_check(client, url, service_name):
        entered.append(service_name)
        if len(entered) == len(worker.PROBE_TARGETS):
            release.set()
        await release.wait()
        return _probe(ServiceStatus.UP.value)

    async def scenario():
        with patch.object(worker, "check_service_health", fake_check):
            task = asyncio.create_task(worker.probe_once(None))
            await asyncio.wait_for(release.wait(), timeout=1)
            assert entered == [name for name, _url in worker.PROBE_TARGETS]
            await task

    try:
        asyncio.run(scenario())
    finally:
        state.reset()
