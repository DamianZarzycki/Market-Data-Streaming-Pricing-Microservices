import logging

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

INVALID_JSON = "Invalid JSON format"
MISSING_PAYLOAD = "Missing payload data"
BATCH_SHAPE = "Payload must be a non-empty list of trade actions"
PROVIDER_REQUIRED = "use_provider_data is required"
PROVIDER_BOOL = "use_provider_data must be a boolean"

_TRADE_ACTIONS = "/trade-actions"
_BATCH = "/trade-actions/batch"
_PROVIDER_MODE = "/provider-mode"


class ApiError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


class QueueFull(ApiError):
    """The in-process queue refused a put. Batch responses include how far the loop got."""

    def __init__(self, message: str, accepted_so_far: int | None = None):
        super().__init__(status.HTTP_503_SERVICE_UNAVAILABLE, message)
        self.accepted_so_far = accepted_so_far


def _is_json_invalid(errors) -> bool:
    return any(err.get("type") in {"json_invalid", "json_type"} for err in errors)


def _provider_mode_message(exc: RequestValidationError) -> str:
    for err in exc.errors():
        loc = err.get("loc") or ()
        if "use_provider_data" not in loc:
            continue
        if err.get("type") == "missing":
            return PROVIDER_REQUIRED
        return PROVIDER_BOOL
    return PROVIDER_REQUIRED


def classify_validation(request: Request, exc: RequestValidationError) -> tuple[int, str]:
    """Map FastAPI's 422 onto the trade-action 400 contract."""
    if _is_json_invalid(exc.errors()):
        return status.HTTP_400_BAD_REQUEST, INVALID_JSON
    path = request.url.path
    if path == _PROVIDER_MODE:
        return status.HTTP_400_BAD_REQUEST, _provider_mode_message(exc)
    if path == _BATCH:
        return status.HTTP_400_BAD_REQUEST, BATCH_SHAPE
    return status.HTTP_400_BAD_REQUEST, MISSING_PAYLOAD


def install_error_handlers(app: FastAPI) -> None:
    # Async handler: the response is a JSON object built in memory, so this
    # runs on the event loop.
    @app.exception_handler(ApiError)
    async def api_error(_request: Request, exc: ApiError):
        content = {"error": exc.message}
        if isinstance(exc, QueueFull) and exc.accepted_so_far is not None:
            content["accepted_so_far"] = exc.accepted_so_far
        return JSONResponse(status_code=exc.status_code, content=content)

    # Async handler: the 400 message is chosen in memory and returned as JSON,
    # so this runs on the event loop.
    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        status_code, message = classify_validation(request, exc)
        return JSONResponse(status_code=status_code, content={"error": message})

    # Async handler: the failure is logged and the response is JSON, so this
    # runs on the event loop.
    @app.exception_handler(Exception)
    async def unexpected(request: Request, exc: Exception):
        logging.exception("Error handling %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal server error"},
        )
