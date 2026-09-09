import logging
import os
import queue

from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import Severity
from shared.trading_shared.models import AuditLog

SERVICE_NAME = "market-data-service-integration"
BATCH_SIZE = int(os.getenv("MARKET_DATA_AUDIT_BATCH_SIZE", "10"))
FLUSH_INTERVAL_SECONDS = float(os.getenv("MARKET_DATA_AUDIT_FLUSH_SECONDS", "5"))

STOP = object()
audit_queue: queue.Queue = queue.Queue()


def enqueue(event: dict) -> None:
    audit_queue.put(event)


def stop() -> None:
    audit_queue.put(STOP)


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


def _flush(buffer: list[dict]) -> None:
    if not buffer:
        return
    entries = [_to_audit_log(event) for event in buffer]
    try:
        with DBSessionManager() as db:
            for entry in entries:
                db.audit.add(entry)
            db.commit()
        logging.info("Flushed %s fetch audit events", len(entries))
        buffer.clear()
    except Exception:
        logging.exception("Failed to persist fetch audit batch of %s", len(entries))
        buffer.clear()


def audit_worker() -> None:
    logging.info(
        "Fetch audit worker started batch_size=%s flush=%ss",
        BATCH_SIZE,
        FLUSH_INTERVAL_SECONDS,
    )
    buffer: list[dict] = []
    while True:
        try:
            item = audit_queue.get(timeout=FLUSH_INTERVAL_SECONDS)
        except queue.Empty:
            _flush(buffer)
            continue

        try:
            if item is STOP:
                _flush(buffer)
                logging.info("Fetch audit worker stopped")
                return
            buffer.append(item)
            if len(buffer) >= BATCH_SIZE:
                _flush(buffer)
        finally:
            audit_queue.task_done()
