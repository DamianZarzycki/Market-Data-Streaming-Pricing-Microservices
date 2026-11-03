import copy
import logging
import threading
from datetime import datetime, timezone

from core.audit_worker import enqueue
from shared.trading_shared.enums import EventType, ServiceStatus, Severity

QUOTE_SERVICE = "quote_service"
CURVE_SERVICE = "curve_service"

_lock = threading.Lock()


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
