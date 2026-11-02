import logging
from collections.abc import Iterator

from fastapi import status
from sqlalchemy.orm import Session

from shared.trading_shared.db import SessionLocal

from api.errors import SUMMARY_FAILED, ApiError
import trades_service


def get_db() -> Iterator[Session]:
    """One transaction per request. Commit on success, rollback and close on failure.

    A session left open stays idle in a transaction and Postgres cannot vacuum.
    """
    db = SessionLocal()
    try:
        yield db
        if db.in_transaction():
            db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def get_summary_db() -> Iterator[tuple[dict, Session]]:
    """Fetch books-service before checking out a connection.

    The catalogue call has a 2 second timeout. Holding a pooled connection
    across that wait would pin a backend for a neighbour that may be down.
    """
    try:
        payload = trades_service.fetch_books_payload()
    except Exception:
        logging.exception("Failed to fetch book summary")
        raise ApiError(status.HTTP_500_INTERNAL_SERVER_ERROR, SUMMARY_FAILED) from None
    db = SessionLocal()
    try:
        yield payload, db
        if db.in_transaction():
            db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
