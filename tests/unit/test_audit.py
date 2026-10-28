import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

BACKEND = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BACKEND))

from shared.trading_shared.audit import AuditLogger, append_audit
from shared.trading_shared.audit_retention import purge_expired
from shared.trading_shared.db import engine
from shared.trading_shared.enums import EventType, Severity
from shared.trading_shared.models import AuditLog, Trade

MIGRATION = (
    BACKEND
    / "db"
    / "migrations"
    / "versions"
    / "a91c0e7b4d22_merge_heads_audit_log_created_at_index.py"
)
NOW = datetime(2026, 10, 27, tzinfo=timezone.utc)


def test_engine_pings_and_recycles_connections():
    assert engine.pool._pre_ping is True
    assert engine.pool._recycle == 1800


def test_trade_and_audit_indexes_are_declared():
    trade_indexes = {index.name for index in Trade.__table__.indexes}
    audit_indexes = {index.name for index in AuditLog.__table__.indexes}
    assert "ix_trades_book_id_status" in trade_indexes
    assert "ix_audit_logs_created_at" in audit_indexes


def test_audit_index_migration_merges_the_two_heads():
    text = MIGRATION.read_text()
    assert '("f3a7c9e1b204", "c3a91e7b2f10")' in text
    assert "ix_audit_logs_created_at" in text


def test_append_audit_stages_a_row_without_committing():
    session = MagicMock()
    payload = {"performed_by": "API"}
    entry = append_audit(
        session,
        EventType.DB_CREATE,
        "Created 1 new books",
        service_name="books-service",
        severity=Severity.INFO,
        entity_type="BOOK",
        payload=payload,
    )
    session.add.assert_called_once_with(entry)
    session.commit.assert_not_called()
    assert entry.service_name == "books-service"
    assert entry.event_type == EventType.DB_CREATE.value
    assert entry.severity == Severity.INFO.value
    assert entry.payload == {"performed_by": "API"}
    payload["performed_by"] = "changed"
    assert entry.payload == {"performed_by": "API"}


def test_audit_logger_uses_the_caller_session_when_one_is_passed():
    session = MagicMock()
    with patch("shared.trading_shared.audit.DBSessionManager") as manager:
        AuditLogger("books-service").info(
            EventType.DB_UPDATE,
            "Updated book",
            db=session,
        )
    manager.assert_not_called()
    session.add.assert_called_once()
    session.commit.assert_not_called()
    entry = session.add.call_args.args[0]
    assert entry.event_type == EventType.DB_UPDATE.value
    assert entry.service_name == "books-service"


def _sqlite_session():
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "sqlite"
    return session


def test_purge_uses_the_90_day_cutoff():
    session = _sqlite_session()
    session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []
    deleted = purge_expired(session, retention_days=90, now=NOW)
    assert deleted == 0
    expr = session.query.return_value.filter.call_args.args[0]
    assert expr.right.value == NOW - timedelta(days=90)
    session.execute.assert_not_called()


def test_purge_deletes_in_batches_until_a_short_batch():
    session = _sqlite_session()
    batches = [[(object(),)] * 1000, [(object(),), (object(),)]]
    id_query = MagicMock()
    id_query.filter.return_value.order_by.return_value.limit.return_value.all.side_effect = batches
    delete_query = MagicMock()

    def query(target):
        if target is AuditLog.audit_id:
            return id_query
        return delete_query

    session.query.side_effect = query
    deleted = purge_expired(session, retention_days=90, batch_size=1000, now=NOW)
    assert deleted == 1002
    assert session.commit.call_count == 2
    assert delete_query.filter.return_value.delete.call_count == 2


def test_purge_stops_between_batches_when_shutdown_is_requested():
    session = _sqlite_session()
    stop = threading.Event()
    id_query = MagicMock()
    id_query.filter.return_value.order_by.return_value.limit.return_value.all.side_effect = [
        [(object(),)] * 3,
        [(object(),)],
    ]
    delete_query = MagicMock()

    def query(target):
        if target is AuditLog.audit_id:
            return id_query
        return delete_query

    def commit():
        stop.set()

    session.query.side_effect = query
    session.commit.side_effect = commit
    deleted = purge_expired(
        session,
        retention_days=90,
        batch_size=3,
        now=NOW,
        stop_event=stop,
    )
    assert deleted == 3
    assert session.commit.call_count == 1


def test_postgres_purge_skips_the_cycle_when_the_lock_is_held():
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    lock = MagicMock()
    lock.scalar.return_value = False
    session.execute.return_value = lock
    deleted = purge_expired(session, now=NOW)
    assert deleted == 0
    session.query.assert_not_called()
    session.execute.assert_called_once()
    assert "pg_try_advisory_lock" in session.execute.call_args.args[0].text


def test_postgres_purge_releases_the_lock_after_the_cycle():
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    lock = MagicMock()
    lock.scalar.return_value = True
    unlock = MagicMock()
    session.execute.side_effect = [lock, unlock]
    session.query.return_value.filter.return_value.order_by.return_value.limit.return_value.all.return_value = []
    deleted = purge_expired(session, now=NOW)
    assert deleted == 0
    assert "pg_advisory_unlock" in session.execute.call_args.args[0].text
    session.rollback.assert_called()
