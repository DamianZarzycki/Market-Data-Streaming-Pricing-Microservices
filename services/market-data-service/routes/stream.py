from fastapi import FastAPI, status
from fastapi.responses import StreamingResponse

import worker
from api.schemas import MarketTick


def register(app: FastAPI) -> None:
    # Async handler: the body awaits asyncio.Queue.get, which suspends this
    # request and does not hold a thread while the client is connected.
    # Each frame is `data: ` plus one MarketTick. The body is not a JSON document.
    @app.get(
        "/stream",
        status_code=status.HTTP_200_OK,
        response_class=StreamingResponse,
        responses={
            200: {
                "description": "SSE frames. Each line is `data: ` plus one MarketTick.",
                "model": MarketTick,
            }
        },
    )
    async def stream():
        subscriber = worker.subscribe()
        return StreamingResponse(
            worker.iter_subscriber(subscriber),
            media_type="text/event-stream",
        )
