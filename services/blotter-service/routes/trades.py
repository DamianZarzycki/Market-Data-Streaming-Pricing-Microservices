from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Path, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from api.errors import TRADE_NOT_FOUND, ApiError
from api.schemas import (
    TradeDetailResponse,
    TradeListQuery,
    TradeListResponse,
    ValuationListResponse,
)
from shared.trading_shared.db import get_async_db
import trades_service


def register(app: FastAPI) -> None:
    # Async handler: awaits the trade SELECT on AsyncSession.
    @app.get(
        "/trades",
        response_model=TradeListResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def list_trades(
        query: Annotated[TradeListQuery, Query()],
        db: AsyncSession = Depends(get_async_db),
    ) -> TradeListResponse:
        return TradeListResponse.model_validate(
            {
                "trades": await trades_service.fetch_trades(
                    db,
                    book_id=query.book_id,
                    asset_class=query.asset_class,
                    status=query.status,
                    symbol=query.symbol,
                    first_only=query.first_only,
                    page=query.page,
                    limit=query.limit,
                )
            }
        )

    # Async handler: awaits the trade and valuation SELECTs on AsyncSession.
    @app.get(
        "/trades/{trade_id}",
        response_model=TradeDetailResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def trade_by_id(
        trade_id: Annotated[UUID, Path()],
        db: AsyncSession = Depends(get_async_db),
    ) -> TradeDetailResponse:
        result = await trades_service.fetch_trade_by_id(db, str(trade_id))
        if result is None:
            raise ApiError(status.HTTP_404_NOT_FOUND, TRADE_NOT_FOUND)
        return TradeDetailResponse.model_validate(result)

    # Async handler: awaits the valuation SELECT on AsyncSession.
    # A missing trade is an empty list.
    @app.get(
        "/trades/{trade_id}/valuations",
        response_model=ValuationListResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    async def trade_valuations(
        trade_id: Annotated[UUID, Path()],
        db: AsyncSession = Depends(get_async_db),
    ) -> ValuationListResponse:
        return ValuationListResponse.model_validate(
            {
                "valuations": await trades_service.fetch_trade_valuations(db, str(trade_id)),
            }
        )

    # The contract is an empty body, not JSON null. This route does not read audit_logs.
    @app.get("/trades/{trade_id}/audit-logs", status_code=status.HTTP_200_OK)
    async def trade_audit_logs(trade_id: Annotated[UUID, Path()]) -> Response:
        return Response(status_code=status.HTTP_200_OK)
