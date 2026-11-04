import inspect
import json
import logging

from fastapi import FastAPI, Request, status

from shared.trading_shared.env import env_int
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

MAX_BODY_BYTES = env_int("MAX_BODY_BYTES", 1_048_576)
_SKIP_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_TOO_LARGE = "Request body is too large"
_INTERNAL = "Internal server error"


class ApiError(Exception):
    def __init__(self, status_code: int, message: str, **extra):
        self.status_code = status_code
        self.message = message
        self.extra = extra
        super().__init__(message)


async def _maybe_await(value):
    if inspect.isawaitable(value):
        return await value
    return value


def install_error_handlers(
    app: FastAPI,
    *,
    error_key: str = "error",
    invalid_message: str = "Invalid request",
    classify=None,
    on_api_error=None,
    on_unexpected=None,
    on_validation=None,
) -> None:
    """Register the shared JSON error handlers.

    `classify(request, exc)` returns `(status_code, message)` and replaces the
    default 400. `on_api_error(exc)` returns extra JSON fields. `on_unexpected`
    and `on_validation` run before the response is written. Either hook may be
    a coroutine.
    """

    @app.exception_handler(ApiError)
    async def api_error(_request: Request, exc: ApiError):
        content = {error_key: exc.message}
        if exc.extra:
            content.update(exc.extra)
        if on_api_error is not None:
            extra = await _maybe_await(on_api_error(exc))
            if extra:
                content.update(extra)
        return JSONResponse(status_code=exc.status_code, content=content)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, exc: RequestValidationError):
        if on_validation is not None:
            await _maybe_await(on_validation(request, exc))
        if classify is None:
            status_code, message = status.HTTP_400_BAD_REQUEST, invalid_message
        else:
            status_code, message = await _maybe_await(classify(request, exc))
        return JSONResponse(status_code=status_code, content={error_key: message})

    @app.exception_handler(Exception)
    async def unexpected(request: Request, exc: Exception):
        logging.exception("Error handling %s %s", request.method, request.url.path)
        if on_unexpected is not None:
            await _maybe_await(on_unexpected(request, exc))
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={error_key: _INTERNAL},
        )


def _content_length(scope) -> int | None:
    for key, value in scope.get("headers") or ():
        if key.lower() == b"content-length":
            try:
                return int(value)
            except ValueError:
                return None
    return None


async def _reject(send, error_key: str) -> None:
    body = json.dumps({error_key: _TOO_LARGE}).encode()
    await send(
        {
            "type": "http.response.start",
            "status": status.HTTP_413_CONTENT_TOO_LARGE,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class MaxBodySizeMiddleware:
    """Reject a request body larger than `max_bytes` before the route runs.

    A declared Content-Length above the cap is rejected without reading the
    body. A chunked body is counted and rejected once the cap is crossed.
    """

    def __init__(self, app, max_bytes: int = MAX_BODY_BYTES, error_key: str = "error"):
        self.app = app
        self.max_bytes = max_bytes
        self.error_key = error_key

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("method") in _SKIP_METHODS:
            await self.app(scope, receive, send)
            return

        length = _content_length(scope)
        if length is not None and length > self.max_bytes:
            await _reject(send, self.error_key)
            return

        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            if message["type"] != "http.request":
                await _reject(send, self.error_key)
                return
            chunk = message.get("body", b"") or b""
            if len(body) + len(chunk) > self.max_bytes:
                await _reject(send, self.error_key)
                return
            body.extend(chunk)
            if not message.get("more_body", False):
                break

        payload = bytes(body)
        sent = False

        async def replay():
            nonlocal sent
            if sent:
                return {"type": "http.disconnect"}
            sent = True
            return {"type": "http.request", "body": payload, "more_body": False}

        await self.app(scope, replay, send)
