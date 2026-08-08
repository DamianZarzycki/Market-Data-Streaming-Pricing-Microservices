import os
import threading
import time
from collections import deque
from datetime import datetime, timezone

QUEUE_MAXSIZE = int(os.getenv("TRADE_ACTION_QUEUE_MAXSIZE", "100"))
RECENT_ACTIONS_LIMIT = 50
RECENT_REJECTS_LIMIT = 20
THROUGHPUT_WINDOW_S = 60
PRESSURE_FILL_PCT = 70

_lock = threading.Lock()
_state = {
    "processed": 0,
    "errors": 0,
    "rejected_400": 0,
    "overload_503": 0,
    "duplicates": 0,
    "latency_sum_ms": 0.0,
    "latency_count": 0,
    "accepted_timestamps": deque(),
    "recent_actions": deque(maxlen=RECENT_ACTIONS_LIMIT),
    "recent_rejects": deque(maxlen=RECENT_REJECTS_LIMIT),
    "last_db_write": None,
    "worker_running": False,
}


def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def _prune_throughput(now_ts):
    stamps = _state["accepted_timestamps"]
    cutoff = now_ts - THROUGHPUT_WINDOW_S
    while stamps and stamps[0] < cutoff:
        stamps.popleft()


def set_worker_running(running: bool):
    with _lock:
        _state["worker_running"] = running


def record_rejected(action_type, client_request_id, symbol, note, latency_ms=1):
    with _lock:
        _state["rejected_400"] += 1
        entry = {
            "time": _now_iso(),
            "client_request_id": client_request_id or "—",
            "action_type": action_type or "—",
            "symbol": symbol or "—",
            "result": "REJECTED",
            "latency_ms": latency_ms,
            "note": note,
        }
        _state["recent_actions"].appendleft(entry)
        _state["recent_rejects"].appendleft(
            {
                "time": entry["time"],
                "reason": note,
                "client_request_id": entry["client_request_id"],
            }
        )


def record_overload(action_type, client_request_id, symbol):
    with _lock:
        _state["overload_503"] += 1
        entry = {
            "time": _now_iso(),
            "client_request_id": client_request_id or "—",
            "action_type": action_type or "—",
            "symbol": symbol or "—",
            "result": "REJECTED",
            "latency_ms": 1,
            "note": "queue full",
        }
        _state["recent_actions"].appendleft(entry)
        _state["recent_rejects"].appendleft(
            {
                "time": entry["time"],
                "reason": "queue full (503)",
                "client_request_id": entry["client_request_id"],
            }
        )


def record_accepted(action_type, client_request_id, symbol):
    now_ts = time.time()
    with _lock:
        _state["accepted_timestamps"].append(now_ts)
        _prune_throughput(now_ts)
        _state["recent_actions"].appendleft(
            {
                "time": _now_iso(),
                "client_request_id": client_request_id or "—",
                "action_type": action_type or "—",
                "symbol": symbol or "—",
                "result": "QUEUED",
                "latency_ms": None,
                "note": "waiting in buffer",
            }
        )


def _update_action(client_request_id, **fields):
    if not client_request_id:
        return
    for entry in _state["recent_actions"]:
        if entry.get("client_request_id") == client_request_id:
            entry.update(fields)
            entry["time"] = _now_iso()
            return
    entry = {
        "time": _now_iso(),
        "client_request_id": client_request_id,
        "action_type": fields.get("action_type", "—"),
        "symbol": fields.get("symbol", "—"),
        "result": fields.get("result", "PROCESSED"),
        "latency_ms": fields.get("latency_ms"),
        "note": fields.get("note", ""),
    }
    _state["recent_actions"].appendleft(entry)


def _latency_ms(enqueued_at):
    if enqueued_at is None:
        return None
    return max(0, round((time.time() - enqueued_at) * 1000))


def record_processed(action_type, client_request_id, symbol, note, enqueued_at):
    latency = _latency_ms(enqueued_at)
    with _lock:
        _state["processed"] += 1
        if latency is not None:
            _state["latency_sum_ms"] += latency
            _state["latency_count"] += 1
        _state["last_db_write"] = _now_iso()
        _update_action(
            client_request_id,
            action_type=action_type or "—",
            symbol=symbol or "—",
            result="PROCESSED",
            latency_ms=latency,
            note=note,
        )


def record_duplicate(action_type, client_request_id, symbol, enqueued_at):
    latency = _latency_ms(enqueued_at)
    with _lock:
        _state["duplicates"] += 1
        _update_action(
            client_request_id,
            action_type=action_type or "—",
            symbol=symbol or "—",
            result="DUPLICATE",
            latency_ms=latency if latency is not None else 1,
            note="idempotency skip",
        )


def record_error(action_type, client_request_id, symbol, note, enqueued_at):
    latency = _latency_ms(enqueued_at)
    with _lock:
        _state["errors"] += 1
        _update_action(
            client_request_id,
            action_type=action_type or "—",
            symbol=symbol or "—",
            result="ERROR",
            latency_ms=latency if latency is not None else None,
            note=note,
        )


def build_status(queue_obj):
    now_ts = time.time()
    with _lock:
        _prune_throughput(now_ts)
        depth = queue_obj.qsize()
        capacity = QUEUE_MAXSIZE
        fill_pct = round((depth / capacity) * 100) if capacity else 0
        backpressure = depth >= capacity or fill_pct >= PRESSURE_FILL_PCT
        avg_latency = (
            round(_state["latency_sum_ms"] / _state["latency_count"])
            if _state["latency_count"]
            else None
        )
        pipeline = "PRESSURE" if backpressure else "HEALTHY"

        return {
            "service": "trade-action-service",
            "pipeline_status": pipeline,
            "worker_running": _state["worker_running"],
            "queue": {
                "depth": depth,
                "capacity": capacity,
                "fill_pct": fill_pct,
                "backpressure": backpressure,
            },
            "throughput_per_min": len(_state["accepted_timestamps"]),
            "processed": _state["processed"],
            "errors": _state["errors"],
            "avg_latency_ms": avg_latency,
            "rejected_400": _state["rejected_400"],
            "overload_503": _state["overload_503"],
            "duplicates": _state["duplicates"],
            "last_db_write": _state["last_db_write"],
            "recent_actions": list(_state["recent_actions"]),
            "recent_rejects": list(_state["recent_rejects"]),
        }
