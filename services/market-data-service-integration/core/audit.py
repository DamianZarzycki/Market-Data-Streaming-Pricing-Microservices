import asyncio
import copy
import logging
import threading
from datetime import datetime, timezone

from shared.trading_shared.db import AsyncDBSessionManager
from shared.trading_shared.enums import EventType, ServiceStatus, Severity
from shared.trading_shared.env import env_float, env_int
from shared.trading_shared.models import AuditLog

QUOTE_SERVICE = "quote_service"
CURVE_SERVICE = "curve_service"
SERVICE_NAME = "market-data-service-integration"
BATCH_SIZE = env_int("MARKET_DATA_AUDIT_BATCH_SIZE", 10)
FLUSH_INTERVAL_SECONDS = env_float("MARKET_DATA_AUDIT_FLUSH_SECONDS", 5.0, minimum=0.1)

_lock = threading.Lock()
audit_queue: asyncio.Queue = asyncio.Queue()


def _empty_bucket() -> dict:
    return {
        "last_success": None,
        "last_error": None,
        "last_query_time": None,
        "error_count": 0,
    }


_state = {
    QUOTE_SERVICE: _empty_bucket(),
    CURVE_SERVICE: _empty_bucket(),
}


def enqueue(event: dict) -> None:
    audit_queue.put_nowait(event)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _success_view(now: str, payload: dict) -> dict:
    view = {"time": now, "provider": payload.get("provider")}
    symbol = payload.get("symbol")
    if symbol:
        view["symbol"] = symbol
    return view


def _error_view(now: str, payload: dict) -> dict:
    error = payload.get("error")
    view = {
        "time": now,
        "provider": payload.get("provider"),
        "error": str(error) if error is not None else None,
    }
    symbol = payload.get("symbol")
    if symbol:
        view["symbol"] = symbol
    status = payload.get("status")
    if status:
        view["status"] = status
    return view


def record_query(source: str) -> None:
    now = _now_iso()
    with _lock:
        _state[source]["last_query_time"] = now


def record_success(source: str, payload: dict) -> None:
    now = _now_iso()
    with _lock:
        bucket = _state[source]
        if bucket["last_query_time"] is None:
            bucket["last_query_time"] = now
        bucket["last_success"] = _success_view(now, payload)
        error_count = bucket["error_count"]

    enqueue(
        {
            "event_type": EventType.FETCH_SUCCESS,
            "severity": Severity.INFO,
            "message": f"{source} fetch success: {payload.get('provider')}",
            "entity_type": source,
            "entity_id": payload.get("symbol") or payload.get("provider"),
            "payload": {
                "provider": payload.get("provider"),
                "symbol": payload.get("symbol"),
                "error_count": error_count,
            },
        }
    )


def record_error(source: str, payload: dict) -> None:
    now = _now_iso()
    error = payload.get("error")
    error_text = str(error) if error is not None else None
    with _lock:
        bucket = _state[source]
        if bucket["last_query_time"] is None:
            bucket["last_query_time"] = now
        bucket["error_count"] += 1
        bucket["last_error"] = _error_view(now, payload)
        error_count = bucket["error_count"]

    enqueue(
        {
            "event_type": EventType.FETCH_ERROR,
            "severity": Severity.ERROR,
            "message": f"{source} fetch error: {payload.get('provider')} {error_text}",
            "entity_type": source,
            "entity_id": payload.get("symbol") or payload.get("provider"),
            "payload": {
                "provider": payload.get("provider"),
                "symbol": payload.get("symbol"),
                "error": error_text,
                "status": payload.get("status"),
                "error_count": error_count,
            },
        }
    )


def snapshot() -> dict:
    """Copy the in-memory fetch counters. The lock is short, so this stays synchronous."""
    with _lock:
        return {
            QUOTE_SERVICE: copy.deepcopy(_state[QUOTE_SERVICE]),
            CURVE_SERVICE: copy.deepcopy(_state[CURVE_SERVICE]),
        }


def health_view() -> dict:
    """In-memory health payload. There is no non-blocking I/O."""
    logging.info("Market Data Service Integration health check")
    return {
        "service": "market-data-service-integration",
        "status": ServiceStatus.UP.value,
        **snapshot(),
    }


def _enum_value(value) -> str:
    return value.value if hasattr(value, "value") else value


def _to_audit_log(event: dict) -> AuditLog:
    entity_id = event.get("entity_id")
    return AuditLog(
        service_name=SERVICE_NAME,
        event_type=_enum_value(event["event_type"]),
        severity=_enum_value(event.get("severity") or Severity.INFO),
        message=event["message"],
        entity_type=event.get("entity_type") or None,
        entity_id=str(entity_id) if entity_id is not None else None,
        correlation_id=event.get("correlation_id") or None,
        payload=event.get("payload") or {},
    )


# Async: awaits AsyncSession.commit for the audit batch.
async def _flush(buffer: list[dict]) -> None:
    if not buffer:
        return
    entries = [_to_audit_log(event) for event in buffer]
    try:
        async with AsyncDBSessionManager() as db:
            for entry in entries:
                db.audit.add(entry)
            await db.commit()
        logging.info("Flushed %s fetch audit events", len(entries))
        buffer.clear()
    except Exception:
        logging.exception("Failed to persist fetch audit batch of %s", len(entries))
        buffer.clear()


# Async task: awaits audit_queue.get, then AsyncSession.commit on flush.
async def audit_worker() -> None:
    logging.info(
        "Fetch audit worker started batch_size=%s flush=%ss",
        BATCH_SIZE,
        FLUSH_INTERVAL_SECONDS,
    )
    buffer: list[dict] = []
    try:
        while True:
            try:
                item = await asyncio.wait_for(audit_queue.get(), timeout=FLUSH_INTERVAL_SECONDS)
            except asyncio.TimeoutError:
                await _flush(buffer)
                continue
            try:
                buffer.append(item)
                if len(buffer) >= BATCH_SIZE:
                    await _flush(buffer)
            finally:
                audit_queue.task_done()
    except asyncio.CancelledError:
        await _flush(buffer)
        logging.info("Fetch audit worker stopped")
        raise
