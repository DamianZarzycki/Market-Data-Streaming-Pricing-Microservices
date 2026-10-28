from uuid import UUID

from fastapi import Depends, FastAPI, Query, Response, status
from sqlalchemy.orm import Session

from api.dependencies import get_db
from api.errors import TRADE_NOT_FOUND, ApiError
from api.schemas import (
    TradeDetailResponse,
    TradeListResponse,
    ValuationListResponse,
    parse_first_only,
)
import trades_service


def register(app: FastAPI) -> None:
    # Sync handler: SQLAlchemy's Session blocks, so this route stays a plain def
    # and runs on the server threadpool.
    @app.get("/trades", response_model=TradeListResponse, status_code=status.HTTP_200_OK)
    def list_trades(
        book_id: str | None = None,
        asset_class: str | None = None,
        trade_status: str | None = Query(default=None, alias="status"),
        symbol: str | None = None,
        first_only: str | None = None,
        page: int = Query(default=1, ge=1),
        limit: int = Query(default=50, ge=1, le=200),
        db: Session = Depends(get_db),
    ) -> dict:
        return {
            "trades": trades_service.fetch_trades(
                db,
                book_id=book_id,
                asset_class=asset_class,
                status=trade_status,
                symbol=symbol,
                first_only=parse_first_only(first_only),
                page=page,
                limit=limit,
            )
        }

    # Sync handler: SQLAlchemy's Session blocks, so this route stays a plain def
    # and runs on the server threadpool.
    @app.get(
        "/trades/{trade_id}",
        response_model=TradeDetailResponse,
        status_code=status.HTTP_200_OK,
    )
    def trade_by_id(trade_id: UUID, db: Session = Depends(get_db)) -> dict:
        result = trades_service.fetch_trade_by_id(db, str(trade_id))
        if result is None:
            raise ApiError(status.HTTP_404_NOT_FOUND, TRADE_NOT_FOUND)
        return result

    # Sync handler: SQLAlchemy's Session blocks, so this route stays a plain def
    # and runs on the server threadpool. A missing trade is an empty list.
    @app.get(
        "/trades/{trade_id}/valuations",
        response_model=ValuationListResponse,
        status_code=status.HTTP_200_OK,
    )
    def trade_valuations(trade_id: UUID, db: Session = Depends(get_db)) -> dict:
        return {
            "valuations": trades_service.fetch_trade_valuations(db, str(trade_id)),
        }

    # The contract is an empty body, not JSON null. This route does not read audit_logs.
    @app.get("/trades/{trade_id}/audit-logs", status_code=status.HTTP_200_OK)
    async def trade_audit_logs(trade_id: UUID) -> Response:
        return Response(status_code=status.HTTP_200_OK)
