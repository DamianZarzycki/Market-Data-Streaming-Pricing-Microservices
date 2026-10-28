from typing import Any

from pydantic import BaseModel, ConfigDict

_FIRST_ONLY_TRUE = {"1", "true", "yes"}


def parse_first_only(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in _FIRST_ONLY_TRUE


class HealthResponse(BaseModel):
    service: str
    status: str
    cache_trade_count: int
    cache_entry_count: int


class BookSummaryItem(BaseModel):
    model_config = ConfigDict(extra="allow")

    realized_pnl: float
    unrealized_pnl: float
    active_trades: int


class BookSummaryResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    books: list[BookSummaryItem]


class TradeListItem(BaseModel):
    model_config = ConfigDict(extra="allow")

    valuation: dict[str, Any] | None = None


class TradeListResponse(BaseModel):
    trades: list[TradeListItem]


class TradeDetailResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    trade: dict[str, Any]
    latest_valuation: dict[str, Any] | None = None
    valuation_history: list[dict[str, Any]]
    audit_logs: list[Any]


class ValuationListResponse(BaseModel):
    valuations: list[dict[str, Any]]
