import logging

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

INVALID_REQUEST = "Invalid request"


class ApiError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


def install_error_handlers(app: FastAPI) -> None:
    # Async handler: the JSON body is built in memory, so this runs on the event loop.
    @app.exception_handler(ApiError)
    async def api_error(_request: Request, exc: ApiError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.message},
        )

    # Async handler: keeps FastAPI's 422 off the wire. This service has no body to validate.
    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, exc: RequestValidationError):
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": INVALID_REQUEST},
        )

    # Async handler: the failure is logged and the response is JSON, so this runs on the event loop.
    @app.exception_handler(Exception)
    async def unexpected(request: Request, exc: Exception):
        logging.exception("Error handling %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal server error"},
        )
