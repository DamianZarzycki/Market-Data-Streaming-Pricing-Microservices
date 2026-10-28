import logging
import os
import threading
from datetime import datetime, timedelta, timezone

from sqlalchemy import text

from shared.trading_shared.db import SessionLocal
from shared.trading_shared.models import AuditLog

AUDIT_RETENTION_DAYS = int(os.getenv("AUDIT_RETENTION_DAYS", "90"))
PURGE_BATCH_SIZE = 1000
PURGE_INTERVAL_SECONDS = 24 * 60 * 60
# Session-level advisory lock. Commits do not release it, so one purge can
# commit each batch without letting a second process delete at the same time.
AUDIT_RETENTION_LOCK_KEY = 881512090

_stop = threading.Event()
_guard = threading.Lock()
_thread: threading.Thread | None = None


def _try_lock(session):
    """Return True when this connection holds the lock, False when another
    process does, and None when the database has no advisory locks.
    """
    bind = session.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return None
    acquired = session.execute(
        text("SELECT pg_try_advisory_lock(:key)"),
        {"key": AUDIT_RETENTION_LOCK_KEY},
    ).scalar()
    return bool(acquired)


def _unlock(session) -> None:
    session.rollback()
    session.execute(
        text("SELECT pg_advisory_unlock(:key)"),
        {"key": AUDIT_RETENTION_LOCK_KEY},
    )
    session.commit()


def purge_expired(
    session,
    *,
    retention_days=AUDIT_RETENTION_DAYS,
    batch_size=PURGE_BATCH_SIZE,
    now=None,
    stop_event=None,
) -> int:
    """Delete audit rows older than `retention_days`, one batch per commit.

    The caller owns `session`. This function commits each batch. Do not pass a
    request session: a commit here would also commit the caller's pending work.
    """
    cutoff = (now or datetime.now(timezone.utc)) - timedelta(days=retention_days)
    locked = _try_lock(session)
    if locked is False:
        return 0
    try:
        deleted = 0
        while True:
            if stop_event is not None and stop_event.is_set():
                break
            ids = [
                row[0]
                for row in session.query(AuditLog.audit_id)
                .filter(AuditLog.created_at < cutoff)
                .order_by(AuditLog.created_at)
                .limit(batch_size)
                .all()
            ]
            if not ids:
                break
            (
                session.query(AuditLog)
                .filter(AuditLog.audit_id.in_(ids))
                .delete(synchronize_session=False)
            )
            session.commit()
            deleted += len(ids)
            if len(ids) < batch_size:
                break
        return deleted
    finally:
        if locked:
            try:
                _unlock(session)
            except Exception:
                logging.exception("Failed to release the audit retention lock")


def _purge_once() -> None:
    session = SessionLocal()
    try:
        purge_expired(session, stop_event=_stop)
    except Exception:
        logging.exception("Audit retention purge failed")
        try:
            session.rollback()
        except Exception:
            logging.exception("Audit retention rollback failed")
    finally:
        session.close()


def _run() -> None:
    while not _stop.is_set():
        _purge_once()
        if _stop.wait(PURGE_INTERVAL_SECONDS):
            return


def start_retention() -> None:
    global _thread
    with _guard:
        if _thread is not None and _thread.is_alive():
            return
        _stop.clear()
        _thread = threading.Thread(target=_run, name="audit-retention", daemon=False)
        _thread.start()


def stop_retention(timeout=5) -> None:
    global _thread
    with _guard:
        thread = _thread
        if thread is None:
            return
        _stop.set()
    thread.join(timeout)
    with _guard:
        if _thread is thread and not thread.is_alive():
            _thread = None
            _stop.clear()
