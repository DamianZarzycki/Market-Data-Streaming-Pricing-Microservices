from typing import Any

from fastapi import Body, FastAPI, status

from actions.submit import (
    MAX_BATCH_SIZE,
    batch_too_long,
    submit_trade_action,
    submit_trade_action_batch,
)
from api.errors import BATCH_SHAPE, MISSING_PAYLOAD, ApiError
from api.schemas import AcceptedResponse, BatchAcceptedResponse


def register(app: FastAPI) -> None:
    # Async handler: the quote read and the reject audit await an AsyncSession,
    # and the enqueue is a non-blocking put_nowait, so this route is async.
    @app.post(
        "/trade-actions",
        response_model=AcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def create_trade_action(item: dict[str, Any] = Body(...)) -> AcceptedResponse:
        if not item:
            raise ApiError(status.HTTP_400_BAD_REQUEST, MISSING_PAYLOAD)
        return await submit_trade_action(item)

    # Async handler: the quote read and the reject audit await an AsyncSession,
    # and the enqueue is a non-blocking put_nowait, so this route is async.
    @app.post(
        "/trade-actions/batch",
        response_model=BatchAcceptedResponse,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def create_trade_action_batch(items: list[Any] = Body(...)) -> BatchAcceptedResponse:
        if not items:
            raise ApiError(status.HTTP_400_BAD_REQUEST, BATCH_SHAPE)
        if len(items) > MAX_BATCH_SIZE:
            raise ApiError(status.HTTP_400_BAD_REQUEST, batch_too_long())
        return await submit_trade_action_batch(items)
