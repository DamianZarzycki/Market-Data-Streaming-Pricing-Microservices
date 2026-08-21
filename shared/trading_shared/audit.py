import logging

from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import EventType, Severity
from shared.trading_shared.models import AuditLog


class AuditLogger:
    def __init__(self, service_name: str):
        self.service_name = service_name

    def write(
        self,
        event_type: EventType,
        severity: Severity,
        message: str,
        entity_type: str = "",
        entity_id: str = "",
        correlation_id: str = "",
        payload: dict = {},
        db: "DBSessionManager" = None,
    ) -> None:
        audit_entry = AuditLog(
            service_name=self.service_name,
            event_type=event_type.value if isinstance(event_type, EventType) else event_type,
            severity=severity.value if isinstance(severity, Severity) else severity,
            message=message,
            entity_type=entity_type,
            entity_id=str(entity_id) if entity_id is not None else None,
            correlation_id=str(correlation_id) if correlation_id is not None else None,
            payload=payload,
        )


        try:
            with DBSessionManager() as own_db:
                own_db.audit.add(audit_entry)
                own_db.commit()
        except Exception:
            logging.exception("Failed to persist audit log")

    def info(
        self,
        event_type: EventType,
        message: str,
        entity_type: str = "",
        entity_id: str = "",
        correlation_id: str = "",
        payload: dict = "",
        db: "DBSessionManager" = None,
    ) -> None:
        self.write(event_type, Severity.INFO, message, entity_type, entity_id, correlation_id, payload, db=db)

    def warning(
        self,
        event_type: EventType,
        message: str,
        entity_type: str = "",
        entity_id: str = "",
        correlation_id: str = "",
        payload: dict = {},
        db: "DBSessionManager" = None,
    ) -> None:
        self.write(event_type, Severity.WARNING, message, entity_type, entity_id, correlation_id, payload, db=db)

    def error(
        self,
        event_type: EventType,
        message: str,
        entity_type: str = "",
        entity_id: str = "",
        correlation_id: str = "",
        payload: dict = {},
        db: "DBSessionManager" = None,
    ) -> None:
        self.write(event_type, Severity.ERROR, message, entity_type, entity_id, correlation_id, payload, db=db)
