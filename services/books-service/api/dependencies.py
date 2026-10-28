from collections.abc import Iterator
from uuid import UUID

from sqlalchemy.orm import Session

from shared.trading_shared.db import SessionLocal


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


def get_book_db(book_id: UUID) -> Iterator[Session]:
    """Open a session only after `book_id` has parsed as a UUID.

    FastAPI resolves `book_id` before entering this dependency, so a bad path
    does not check out a connection.
    """
    yield from get_db()
