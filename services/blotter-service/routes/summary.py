import logging

from fastapi import Depends, FastAPI, status
from sqlalchemy.orm import Session

from api.dependencies import get_summary_db
from api.errors import SUMMARY_FAILED, ApiError
from api.schemas import BookSummaryResponse
import trades_service


def register(app: FastAPI) -> None:
    # Sync handler: the books-service call and the SQLAlchemy session both block,
    # so this route stays a plain def and runs on the server threadpool.
    @app.get(
        "/books/summary",
        response_model=BookSummaryResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    def book_summary(
        bundle: tuple[dict, Session] = Depends(get_summary_db),
    ) -> BookSummaryResponse:
        payload, db = bundle
        try:
            return BookSummaryResponse.model_validate(
                trades_service.build_book_summary(payload, db)
            )
        except Exception:
            logging.exception("Failed to fetch book summary")
            raise ApiError(status.HTTP_500_INTERNAL_SERVER_ERROR, SUMMARY_FAILED) from None
