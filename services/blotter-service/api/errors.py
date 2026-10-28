import logging

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

SUMMARY_FAILED = "Failed to fetch book summary"
TRADE_NOT_FOUND = "Trade not found"
INVALID_PAGE = "Invalid page or limit"


class ApiError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


def classify_validation(exc: RequestValidationError) -> tuple[int, str]:
    """Keep FastAPI's 422 off the wire. A bad trade id is a 404. A bad page is a 400."""
    for err in exc.errors():
        loc = err.get("loc") or ()
        if "trade_id" in loc:
            return status.HTTP_404_NOT_FOUND, TRADE_NOT_FOUND
    return status.HTTP_400_BAD_REQUEST, INVALID_PAGE


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def api_error(_request: Request, exc: ApiError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.message},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, exc: RequestValidationError):
        status_code, message = classify_validation(exc)
        return JSONResponse(status_code=status_code, content={"error": message})

    @app.exception_handler(Exception)
    async def unexpected(request: Request, exc: Exception):
        logging.exception("Error handling %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal server error"},
        )
