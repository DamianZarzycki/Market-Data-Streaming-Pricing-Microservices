from fastapi import FastAPI, Query, status
from fastapi.responses import StreamingResponse

from api.schemas import QuoteStreamFrame
from core.stream import open_quote_stream


def register(app: FastAPI) -> None:
    # Async handler: the body awaits asyncio.Queue.get (or asyncio.sleep when
    # mock is on), which suspends this request and does not hold a thread.
    # Each frame is `event: quote` plus one quote. The body is not a JSON document.
    @app.get(
        "/market-data/stream",
        status_code=status.HTTP_200_OK,
        response_class=StreamingResponse,
        responses={
            200: {
                "description": "SSE frames. Each frame is one quote.",
                "model": QuoteStreamFrame,
            }
        },
    )
    async def stream_all(mock: str = Query("")):
        return StreamingResponse(
            open_quote_stream(None, mock),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    # Async handler: the body awaits asyncio.Queue.get for this provider only.
    @app.get(
        "/market-data/stream/{provider}",
        status_code=status.HTTP_200_OK,
        response_class=StreamingResponse,
        responses={
            200: {
                "description": "SSE frames for one provider. Each frame is one quote.",
                "model": QuoteStreamFrame,
            }
        },
    )
    async def stream_provider(provider: str, mock: str = Query("")):
        return StreamingResponse(
            open_quote_stream(provider, mock),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )
