from fastapi import FastAPI, status

from api.schemas import BookMetricsListResponse, BookMetricsResponse
from books import book_metrics_service


def register(app: FastAPI) -> None:
    # Sync handler: copies in-memory book metrics under a short lock. There is
    # no non-blocking I/O, so this stays a plain def and runs on the threadpool.
    @app.get(
        "/book-metrics",
        response_model=BookMetricsListResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    def book_metrics() -> BookMetricsListResponse:
        return BookMetricsListResponse.model_validate(
            {"benchmark": "MARKET_INDEX", "books": book_metrics_service.get_all_metrics()}
        )

    # Sync handler: one dict lookup under a short lock. There is no
    # non-blocking I/O, so this stays a plain def and runs on the threadpool.
    @app.get(
        "/book-metrics/{book_id}",
        response_model=BookMetricsResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    def book_metrics_by_id(book_id: str) -> BookMetricsResponse:
        return BookMetricsResponse.model_validate(book_metrics_service.require_metrics(book_id))
