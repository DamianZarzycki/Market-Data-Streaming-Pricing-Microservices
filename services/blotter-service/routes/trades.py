from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Path, Query, Response, status
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.errors import TRADE_NOT_FOUND, ApiError
from api.schemas import (
    TradeDetailResponse,
    TradeListQuery,
    TradeListResponse,
    ValuationListResponse,
)
import trades_service


def register(app: FastAPI) -> None:
    # Sync handler: SQLAlchemy's Session blocks, so this route stays a plain def
    # and runs on the server threadpool.
    @app.get(
        "/trades",
        response_model=TradeListResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    def list_trades(
        query: Annotated[TradeListQuery, Query()],
        db: Session = Depends(get_db),
    ) -> TradeListResponse:
        return TradeListResponse.model_validate(
            {
                "trades": trades_service.fetch_trades(
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

    # Sync handler: SQLAlchemy's Session blocks, so this route stays a plain def
    # and runs on the server threadpool.
    @app.get(
        "/trades/{trade_id}",
        response_model=TradeDetailResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    def trade_by_id(
        trade_id: Annotated[UUID, Path()],
        db: Session = Depends(get_db),
    ) -> TradeDetailResponse:
        result = trades_service.fetch_trade_by_id(db, str(trade_id))
        if result is None:
            raise ApiError(status.HTTP_404_NOT_FOUND, TRADE_NOT_FOUND)
        return TradeDetailResponse.model_validate(result)

    # Sync handler: SQLAlchemy's Session blocks, so this route stays a plain def
    # and runs on the server threadpool. A missing trade is an empty list.
    @app.get(
        "/trades/{trade_id}/valuations",
        response_model=ValuationListResponse,
        response_model_exclude_unset=True,
        status_code=status.HTTP_200_OK,
    )
    def trade_valuations(
        trade_id: Annotated[UUID, Path()],
        db: Session = Depends(get_db),
    ) -> ValuationListResponse:
        return ValuationListResponse.model_validate(
            {
                "valuations": trades_service.fetch_trade_valuations(db, str(trade_id)),
            }
        )

    # The contract is an empty body, not JSON null. This route does not read audit_logs.
    @app.get("/trades/{trade_id}/audit-logs", status_code=status.HTTP_200_OK)
    async def trade_audit_logs(trade_id: Annotated[UUID, Path()]) -> Response:
        return Response(status_code=status.HTTP_200_OK)
