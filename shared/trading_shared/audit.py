import logging

from shared.trading_shared.db import DBSessionManager

_DBSessionManager = DBSessionManager
from shared.trading_shared.enums import EventType, Severity
from shared.trading_shared.models import AuditLog

_SEVERITY_NAMES = {
    "info": Severity.INFO.value,
    "warning": Severity.WARNING.value,
    "error": Severity.ERROR.value,
    "INFO": Severity.INFO.value,
    "WARNING": Severity.WARNING.value,
    "ERROR": Severity.ERROR.value,
}


def _event_type(event_type) -> str:
    if isinstance(event_type, EventType):
        return event_type.value
    return str(event_type)


def _severity(severity) -> str:
    if isinstance(severity, Severity):
        return severity.value
    return _SEVERITY_NAMES.get(severity, Severity.ERROR.value)


def _optional_id(value) -> str | None:
    if value is None:
        return None
    return str(value)


def append_audit(
    session,
    event_type,
    message,
    *,
    service_name,
    severity=Severity.INFO,
    entity_type="",
    entity_id="",
    correlation_id="",
    payload=None,
) -> AuditLog:
    """Stage an audit row on `session`. The caller commits or rolls it back.

    This does not open a connection and does not commit. A book change and its
    audit row then share one transaction.
    """
    entry = AuditLog(
        service_name=service_name,
        event_type=_event_type(event_type),
        severity=_severity(severity),
        message=message,
        entity_type=entity_type,
        entity_id=_optional_id(entity_id),
        correlation_id=_optional_id(correlation_id),
        payload=dict(payload) if isinstance(payload, dict) else None,
    )
    session.add(entry)
    return entry


def _target_session(db):
    if isinstance(db, _DBSessionManager):
        return db.session
    return db


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
        if db is not None:
            append_audit(
                _target_session(db),
                event_type,
                message,
                service_name=self.service_name,
                severity=severity,
                entity_type=entity_type,
                entity_id=entity_id,
                correlation_id=correlation_id,
                payload=payload if isinstance(payload, dict) else None,
            )
            return

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
