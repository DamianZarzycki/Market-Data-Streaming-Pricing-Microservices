import asyncio
import logging

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EntityType, EventType

audit_logger = AuditLogger("books-service")

_REQUIRED_FIELDS = {"name", "expected_asset_class"}
_MISSING_FIELDS = "Missing required fields (name, expected_asset_class)"
_INVALID_JSON = "Invalid JSON payload"
_INVALID_BOOK_ID = "Invalid book id"


class ApiError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(message)


def _is_json_invalid(errors) -> bool:
    return any(
        err.get("type") in {"json_invalid", "json_type"}
        for err in errors
    )


def _is_bad_book_id(errors) -> bool:
    return any("book_id" in (err.get("loc") or ()) for err in errors)


def _is_empty_payload(body) -> bool:
    return body is None or body == {} or body == []


def _missing_required_fields(errors) -> bool:
    for err in errors:
        loc = err.get("loc") or ()
        if not any(part in _REQUIRED_FIELDS for part in loc):
            continue
        if err.get("type") in {"missing", "string_too_short", "string_type"}:
            return True
    return False


def classify_validation(exc: RequestValidationError) -> tuple[int, str, bool]:
    """Map FastAPI's validation error onto the books status contract.

    Returns status, message, and whether the failure is a DB_REJECT. An empty
    body is 400 and is not audited. A body that omits the required book fields
    is 409 and is audited.
    """
    errors = exc.errors()
    if _is_json_invalid(errors):
        return status.HTTP_400_BAD_REQUEST, _INVALID_JSON, False
    if _is_bad_book_id(errors):
        return status.HTTP_400_BAD_REQUEST, _INVALID_BOOK_ID, False
    if _is_empty_payload(exc.body):
        return status.HTTP_400_BAD_REQUEST, _INVALID_JSON, False
    if _missing_required_fields(errors):
        return status.HTTP_409_CONFLICT, _MISSING_FIELDS, True
    return status.HTTP_400_BAD_REQUEST, _INVALID_JSON, False


def _name_from_body(body):
    if isinstance(body, dict):
        return body.get("name")
    if isinstance(body, list):
        for item in body:
            if isinstance(item, dict):
                return item.get("name")
    return None


def _record_reject(message: str, body) -> None:
    # The book row was never staged, so this reject has no request transaction
    # to join. AuditLogger commits it on its own session.
    audit_logger.warning(
        EventType.DB_REJECT,
        message,
        entity_type=EntityType.BOOK.value,
        payload={"name": _name_from_body(body)},
    )


def _db_error_message(request: Request) -> str:
    if request.method == "POST":
        return "Failed to create books"
    if request.method == "PUT":
        return "Failed to update book"
    if request.method == "DELETE":
        return "Failed to delete book"
    return "Failed to write book"


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def api_error(_request: Request, exc: ApiError):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.message},
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error(_request: Request, exc: RequestValidationError):
        status_code, message, audit_reject = classify_validation(exc)
        if audit_reject:
            await asyncio.to_thread(_record_reject, message, exc.body)
        return JSONResponse(status_code=status_code, content={"error": message})

    @app.exception_handler(Exception)
    async def unexpected(request: Request, exc: Exception):
        logging.exception("Error handling %s %s", request.method, request.url.path)
        if request.method in {"POST", "PUT", "DELETE"}:
            entity_id = request.path_params.get("book_id", "")
            await asyncio.to_thread(
                audit_logger.error,
                EventType.DB_ERROR,
                _db_error_message(request),
                entity_type=EntityType.BOOK.value,
                entity_id=str(entity_id) if entity_id else "",
                payload={"error_type": type(exc).__name__},
            )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "Internal server error"},
        )
