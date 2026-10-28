from uuid import UUID

from fastapi import Depends, FastAPI, status
from sqlalchemy.orm import Session

from books import manager
from api.dependencies import get_book_db, get_db
from api.errors import ApiError
from api.schemas import (
    BookCreate,
    BookListResponse,
    BookResponse,
    BookUpdate,
    CreateBooksResponse,
    MessageResponse,
)


def parse_create_books(body: BookCreate | list[BookCreate]) -> list[BookCreate]:
    if isinstance(body, list):
        if not body:
            raise ApiError(status.HTTP_400_BAD_REQUEST, "Invalid JSON payload")
        return body
    return [body]


def register(app: FastAPI) -> None:
    # Sync handler: SQLAlchemy's Session blocks, so this route stays a plain def
    # and runs on the server threadpool.
    @app.get("/books", response_model=BookListResponse, status_code=status.HTTP_200_OK)
    def list_books(db: Session = Depends(get_db)) -> BookListResponse:
        return BookListResponse(books=manager.get_all_books(db))

    # Sync handler: SQLAlchemy's Session blocks, so this route stays a plain def
    # and runs on the server threadpool.
    @app.get(
        "/books/{book_id}",
        response_model=BookResponse,
        status_code=status.HTTP_200_OK,
    )
    def get_book(book_id: UUID, db: Session = Depends(get_book_db)) -> dict:
        return manager.get_book_by_id(db, book_id)

    # Sync handler: SQLAlchemy's Session blocks, so this route stays a plain def
    # and runs on the server threadpool.
    @app.post(
        "/books",
        response_model=CreateBooksResponse,
        status_code=status.HTTP_201_CREATED,
    )
    def create_books(
        books: list[BookCreate] = Depends(parse_create_books),
        db: Session = Depends(get_db),
    ) -> CreateBooksResponse:
        book_ids = manager.create_books_batch(db, books, created_by="API")
        return CreateBooksResponse(
            message="Books created successfully",
            book_ids=book_ids,
        )

    # Sync handler: SQLAlchemy's Session blocks, so this route stays a plain def
    # and runs on the server threadpool.
    @app.put(
        "/books/{book_id}",
        response_model=MessageResponse,
        status_code=status.HTTP_200_OK,
    )
    def update_book(
        book_id: UUID,
        payload: BookUpdate,
        db: Session = Depends(get_book_db),
    ) -> dict:
        return manager.update_book(db, book_id, payload, updated_by="API")

    # Sync handler: SQLAlchemy's Session blocks, so this route stays a plain def
    # and runs on the server threadpool. The response body is part of the contract,
    # so this stays 200 rather than 204.
    @app.delete(
        "/books/{book_id}",
        response_model=MessageResponse,
        status_code=status.HTTP_200_OK,
    )
    def delete_book(book_id: UUID, db: Session = Depends(get_book_db)) -> dict:
        return manager.delete_book(db, book_id)
