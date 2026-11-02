import json

from fastapi import status

MAX_BODY_BYTES = 1_048_576

_SKIP_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_TOO_LARGE = {"error": "Request body is too large"}


def _content_length(scope) -> int | None:
    for key, value in scope.get("headers") or ():
        if key.lower() == b"content-length":
            try:
                return int(value)
            except ValueError:
                return None
    return None


# Async: ASGI send is awaited, so the 413 response is written on the event loop.
async def _reject(send) -> None:
    body = json.dumps(_TOO_LARGE).encode()
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

    def __init__(self, app, max_bytes: int = MAX_BODY_BYTES):
        self.app = app
        self.max_bytes = max_bytes

    # Async: ASGI receive and send are awaited, so the body is buffered on the
    # event loop and then replayed to the route.
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("method") in _SKIP_METHODS:
            await self.app(scope, receive, send)
            return

        length = _content_length(scope)
        if length is not None and length > self.max_bytes:
            await _reject(send)
            return

        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            if message["type"] != "http.request":
                await _reject(send)
                return
            chunk = message.get("body", b"") or b""
            if len(body) + len(chunk) > self.max_bytes:
                await _reject(send)
                return
            body.extend(chunk)
            if not message.get("more_body", False):
                break

        payload = bytes(body)
        sent = False

        # Async: the route reads the buffered body back through ASGI receive.
        async def replay():
            nonlocal sent
            if sent:
                return {"type": "http.disconnect"}
            sent = True
            return {"type": "http.request", "body": payload, "more_body": False}

        await self.app(scope, replay, send)
