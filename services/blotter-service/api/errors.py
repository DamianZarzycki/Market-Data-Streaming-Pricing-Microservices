from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError

from shared.trading_shared.http_errors import (
    ApiError,
    install_error_handlers as install_shared_error_handlers,
)

SUMMARY_FAILED = "Failed to fetch book summary"
TRADE_NOT_FOUND = "Trade not found"
INVALID_PAGE = "Invalid page or limit"


def classify_validation(exc: RequestValidationError) -> tuple[int, str]:
    """Keep FastAPI's 422 off the wire. A bad trade id is a 404. A bad page is a 400."""
    for err in exc.errors():
        loc = err.get("loc") or ()
        if "trade_id" in loc:
            return status.HTTP_404_NOT_FOUND, TRADE_NOT_FOUND
    return status.HTTP_400_BAD_REQUEST, INVALID_PAGE


def _classify(_request: Request, exc: RequestValidationError) -> tuple[int, str]:
    return classify_validation(exc)


def install_error_handlers(app: FastAPI) -> None:
    install_shared_error_handlers(app, classify=_classify)
