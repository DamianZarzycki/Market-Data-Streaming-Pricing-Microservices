import logging

from fastapi import status
from shared.trading_shared.audit import append_audit
from shared.trading_shared.enums import EntityType, EventType, Severity, TradeStatus
from shared.trading_shared.models import Book, Trade
from sqlalchemy import exists
from sqlalchemy.exc import IntegrityError

from api.errors import ApiError

SERVICE_NAME = "books-service"

_BOOK_COLUMNS = (
    Book.book_id,
    Book.name,
    Book.description,
    Book.expected_asset_class,
    Book.is_active,
)


def _values(data) -> dict:
    dump = getattr(data, "model_dump", None)
    if dump is not None:
        return dump(exclude_unset=True)
    return dict(data)


def _book_dict(row) -> dict:
    return {
        "book_id": str(row.book_id),
        "name": row.name,
        "description": row.description,
        "expected_asset_class": row.expected_asset_class,
        "is_active": row.is_active,
    }


def _reject(db, message, *, entity_id="", payload=None):
    """Persist a reject audit after the book change has been rolled back.

    The rejected change must not commit. The reject row is a separate fact, so
    it is committed on this same connection before the error is raised.
    """
    db.rollback()
    append_audit(
        db,
        EventType.DB_REJECT,
        message,
        service_name=SERVICE_NAME,
        severity=Severity.WARNING,
        entity_type=EntityType.BOOK.value,
        entity_id=entity_id,
        payload=payload,
    )
    db.commit()
    raise ApiError(status.HTTP_409_CONFLICT, message)


def get_all_books(db, is_active=True):
    logging.debug("Fetching all books with is_active=%s", is_active)
    rows = db.query(*_BOOK_COLUMNS).filter(Book.is_active == is_active).all()
    books = [_book_dict(row) for row in rows]
    logging.debug("Found %s books with is_active=%s", len(books), is_active)
    return books


def get_book_by_id(db, book_id):
    row = (
        db.query(*_BOOK_COLUMNS)
        .filter(Book.book_id == book_id)
        .first()
    )
    if row is None:
        logging.debug("No book found with id %s", book_id)
        raise ApiError(status.HTTP_404_NOT_FOUND, "Book not found")
    return _book_dict(row)


def create_books_batch(db, books_data: list, created_by=None):
    logging.info("Received request to create books, count=%s", len(books_data))
    created_books = []

    for data in books_data:
        values = _values(data)
        name = values.get("name")
        expected_asset_class = values.get("expected_asset_class")
        description = values["description"] if "description" in values else ""

        if any(book.name == name for book in created_books):
            _reject(
                db,
                f"Duplicate book name '{name}' found within the request array",
                payload={"name": name},
            )

        if not name or not expected_asset_class:
            _reject(
                db,
                "Missing required fields (name, expected_asset_class)",
                payload={"name": name},
            )

        new_book = Book(
            name=name,
            description=description,
            expected_asset_class=expected_asset_class,
            created_by=created_by,
            is_active=True,
        )
        db.add(new_book)
        created_books.append(new_book)

    try:
        db.flush()
    except IntegrityError:
        _reject(db, "A book with this name already exists")

    created_ids = [str(book.book_id) for book in created_books]
    append_audit(
        db,
        EventType.DB_CREATE,
        f"Created {len(created_books)} new books",
        service_name=SERVICE_NAME,
        severity=Severity.INFO,
        entity_type=EntityType.BOOK.value,
        payload={"created_books": created_ids, "performed_by": created_by},
    )
    return created_ids


def update_book(db, book_id, data, updated_by=None):
    # wzorzec projektowy repository oraz unit of work - jakie tradeoffy na podstawie tego?
    book = db.query(Book).filter(Book.book_id == book_id).first()
    if not book:
        raise ApiError(status.HTTP_404_NOT_FOUND, "Book not found")

    values = _values(data)
    if "name" in values:
        book.name = values["name"]

    if "description" in values:
        book.description = values["description"]

    if values.get("expected_asset_class"):
        book.expected_asset_class = values["expected_asset_class"]

    if "is_active" in values:
        book.is_active = values["is_active"]

    if updated_by is not None:
        book.updated_by = updated_by

    try:
        db.flush()
    except IntegrityError:
        _reject(
            db,
            f"Book with name '{values.get('name')}' already exists",
            entity_id=str(book_id),
            payload={"name": values.get("name")},
        )

    append_audit(
        db,
        EventType.DB_UPDATE,
        f"Updated book with id {book_id}",
        service_name=SERVICE_NAME,
        severity=Severity.INFO,
        entity_type=EntityType.BOOK.value,
        entity_id=str(book_id),
        payload={"performed_by": updated_by},
    )
    return {"message": f"Book with id {book_id} updated successfully"}


def count_active_trades(db, book_id) -> int:
    return (
        db.query(Trade)
        .filter(
            Trade.book_id == book_id,
            Trade.status == TradeStatus.ACTIVE.value,
        )
        .count()
    )


def has_active_trades(db, book_id) -> bool:
    return db.query(
        exists().where(
            Trade.book_id == book_id,
            Trade.status == TradeStatus.ACTIVE.value,
        )
    ).scalar()


def move_trades_to_new_book(db, book_id, new_book_id):
    """Reparent every trade of `book_id` onto `new_book_id` in one statement.

    A single UPDATE acquires row locks in one scan, so it cannot deadlock
    against a per-row FOR UPDATE closer the way an unordered ORM loop can
    (LOCK-2). Callers must already have the book row in the same transaction
    (lock order: books, then trades, then valuations).
    """
    db.query(Trade).filter(Trade.book_id == book_id).update(
        {Trade.book_id: new_book_id},
        synchronize_session=False,
    )


def delete_book(db, book_id):
    book = db.query(Book).filter(Book.book_id == book_id).first()
    if not book:
        raise ApiError(status.HTTP_404_NOT_FOUND, "Book not found")

    active_trades = count_active_trades(db, book_id)
    if active_trades > 0:
        _reject(
            db,
            f"Cannot delete book while {active_trades} active trade(s) remain",
            entity_id=str(book_id),
            payload={"active_trades": active_trades},
        )

    book.is_active = False
    append_audit(
        db,
        EventType.DB_DELETE,
        f"Book {book_id} marked as inactive",
        service_name=SERVICE_NAME,
        severity=Severity.INFO,
        entity_type=EntityType.BOOK.value,
        entity_id=str(book_id),
    )
    return {"message": f"Book with id {book_id} marked as inactive"}
