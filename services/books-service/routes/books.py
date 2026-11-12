from uuid import UUID

from fastapi import Depends, FastAPI, status
from sqlalchemy.ext.asyncio import AsyncSession

from shared.trading_shared.db import get_async_db

from api.dependencies import get_book_db
from api.errors import ApiError
from api.schemas import (
    BookCreate,
    BookListResponse,
    BookResponse,
    BookUpdate,
    CreateBooksResponse,
    MessageResponse,
)
from books import manager


def parse_create_books(body: BookCreate | list[BookCreate]) -> list[BookCreate]:
    if isinstance(body, list):
        if not body:
            raise ApiError(status.HTTP_400_BAD_REQUEST, "Invalid JSON payload")
        return body
    return [body]


def register(app: FastAPI) -> None:
    # Async handler: awaits the book SELECT on AsyncSession.
    @app.get("/books", response_model=BookListResponse, status_code=status.HTTP_200_OK)
    async def list_books(db: AsyncSession = Depends(get_async_db)) -> BookListResponse:
        return BookListResponse(books=await manager.get_all_books(db))

    # Async handler: awaits the book SELECT on AsyncSession.
    @app.get(
        "/books/{book_id}",
        response_model=BookResponse,
        status_code=status.HTTP_200_OK,
    )
    async def get_book(book_id: UUID, db: AsyncSession = Depends(get_book_db)) -> dict:
        return await manager.get_book_by_id(db, book_id)

    # Async handler: awaits the book INSERT flush on AsyncSession.
    @app.post(
        "/books",
        response_model=CreateBooksResponse,
        status_code=status.HTTP_201_CREATED,
    )
    async def create_books(
        books: list[BookCreate] = Depends(parse_create_books),
        db: AsyncSession = Depends(get_async_db),
    ) -> CreateBooksResponse:
        book_ids = await manager.create_books_batch(db, books, created_by="API")
        return CreateBooksResponse(
            message="Books created successfully",
            book_ids=book_ids,
        )

    # Async handler: awaits the book UPDATE flush on AsyncSession.
    @app.put(
        "/books/{book_id}",
        response_model=MessageResponse,
        status_code=status.HTTP_200_OK,
    )
    async def update_book(
        book_id: UUID,
        payload: BookUpdate,
        db: AsyncSession = Depends(get_book_db),
    ) -> dict:
        return await manager.update_book(db, book_id, payload, updated_by="API")

    # Async handler: awaits the active-trade count and the book update on
    # AsyncSession. The response body is part of the contract, so this stays
    # 200 rather than 204.
    @app.delete(
        "/books/{book_id}",
        response_model=MessageResponse,
        status_code=status.HTTP_200_OK,
    )
    async def delete_book(book_id: UUID, db: AsyncSession = Depends(get_book_db)) -> dict:
        return await manager.delete_book(db, book_id)
