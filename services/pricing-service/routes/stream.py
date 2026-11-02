from fastapi import FastAPI, status
from fastapi.responses import StreamingResponse

import calculation_service
from api.schemas import ValuationResponse


def register(app: FastAPI) -> None:
    # Async handler: the body awaits asyncio.Queue.get, which suspends this
    # request and does not hold a thread while the client is connected.
    # Each frame is `event: valuation_update` plus one valuation. The body is not a JSON document.
    @app.get(
        "/valuation-stream",
        status_code=status.HTTP_200_OK,
        response_class=StreamingResponse,
        responses={
            200: {
                "description": "SSE frames. Each frame is one valuation update.",
                "model": ValuationResponse,
            }
        },
    )
    async def valuation_stream():
        client_queue = calculation_service.valuation_feed.subscribe()
        return StreamingResponse(
            calculation_service.valuation_feed.iter_events(client_queue),
            media_type="text/event-stream",
        )
