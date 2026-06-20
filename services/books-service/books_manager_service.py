from datetime import datetime, timezone
import logging

from shared.trading_shared.models import Book


def get_all_books(db, is_active=True):
    logging.info(f"Fetching all books with is_active={is_active}")
    books = db.query(Book).filter(Book.is_active == is_active).all()
    res = [
        {
            "book_id": str(b.book_id),
            "name": b.name,
            "description": b.description,
            "expected_asset_class": b.expected_asset_class,
            "is_active": b.is_active,
        }
        for b in books
    ]
    logging.info(f"Found {len(res)} books with is_active={res}")

    return res


def get_book_by_id(db, book_id):
    book = db.query(Book).filter(Book.book_id == book_id).first()
    if book is None:
        logging.info(f"No book found with id {book_id}")
        return None

    return {
        "book_id": str(book.book_id),
        "name": book.name,
        "description": book.description,
        "expected_asset_class": book.expected_asset_class,
        "is_active": book.is_active,
    }


def create_books_batch(db, books_data: list, created_by=None):
    created_books = []

    for data in books_data:
        name = data.get("name")
        expected_asset_class = data.get("expected_asset_class")
        description = data.get("description", "")

        if any(b.name == name for b in created_books):
            raise ValueError(
                f"Duplicate book name '{name}' found within the request array"
            )

        if not name or not expected_asset_class:
            raise ValueError(
                f"Missing required fields (name, expected_asset_class) in payload: {data}"
            )

        existing_book = db.query(Book).filter(Book.name == name).first()
        if existing_book:
            raise ValueError(f"Book with name '{name}' already exists")

        new_book = Book(
            name=name,
            description=description,
            expected_asset_class=expected_asset_class,
            created_by=created_by,
            is_active=True,
        )

        db.add(new_book)
        created_books.append(new_book)

    db.commit()
    return [str(b.book_id) for b in created_books]


def update_book(db, book_id, data, updated_by=None):
    # wzorzec projektowy repository oraz unit of work - jakie tradeoffy na podstawie tego?
    book = db.query(Book).filter(Book.book_id == book_id).first()
    if not book:
        return None

    if "name" in data:
        already_existing = (
            db.query(Book)
            .filter(Book.name == data["name"], Book.book_id != book_id)
            .first()
        )
        if already_existing:
            raise ValueError(f"Book with name {data['name']} already exists")
        book.name = data["name"]

    if "description" in data:
        book.description = data["description"]

    if "is_active" in data:
        book.is_active = data["is_active"]

    if updated_by:
        book.updated_by = updated_by
    if updated_by is not None:
        book.updated_by = updated_by

    db.commit()
    return book


def delete_book(db, book_id):
    book = db.query(Book).filter(Book.book_id == book_id).first()
    if not book:
        return {"error": "Book not found"}

    book.is_active = False
    return {"message": f"Book with id {book_id} marked as inactive"}
