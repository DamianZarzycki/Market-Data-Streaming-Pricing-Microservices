from typing import Optional

from shared.trading_shared.enums import EventType, Severity
from shared.trading_shared.models import AuditLog

from shared.trading_shared.db import DBSessionManager

class AuditLogger:
    def __init__(self, service_name: str):
        self.service_name = service_name

    def write(
        self,
        event_type: EventType,
        severity: Severity,
        message: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        payload: Optional[dict] = None,
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

        with DBSessionManager() as db:
            db.audit.add(audit_entry)
            db.commit()

    def info(
        self,
        event_type: EventType,
        message: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        payload: Optional[dict] = None,
    ) -> None:
        self.write(event_type, Severity.INFO, message, entity_type, entity_id, correlation_id, payload)

    def warning(
        self,
        event_type: EventType,
        message: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        payload: Optional[dict] = None,
    ) -> None:
        self.write(event_type, Severity.WARNING, message, entity_type, entity_id, correlation_id, payload)

    def error(
        self,
        event_type: EventType,
        message: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        payload: Optional[dict] = None,
    ) -> None:
        self.write(event_type, Severity.ERROR, message, entity_type, entity_id, correlation_id, payload)
