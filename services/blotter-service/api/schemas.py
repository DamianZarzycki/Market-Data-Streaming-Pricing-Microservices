from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

_FIRST_ONLY_TRUE = {"1", "true", "yes"}


class HealthResponse(BaseModel):
    service: str
    status: str
    cache_trade_count: int
    cache_entry_count: int


class TradeListQuery(BaseModel):
    """Filters for GET /trades. `first_only=yes` stays a list of at most one row."""

    model_config = ConfigDict(extra="ignore")

    book_id: str | None = None
    asset_class: str | None = None
    status: str | None = None
    symbol: str | None = None
    first_only: bool = False
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=50, ge=1, le=200)

    @field_validator("first_only", mode="before")
    @classmethod
    def parse_first_only(cls, value):
        if value is None or value is False:
            return False
        if value is True:
            return True
        if isinstance(value, str):
            return value.strip().lower() in _FIRST_ONLY_TRUE
        return False


class PermissiveModel(BaseModel):
    """Stream and row payloads carry columns these models do not name."""

    model_config = ConfigDict(extra="allow")


class TradeRef(PermissiveModel):
    trade_id: str | None = None
    book_id: str | None = None
    asset_class: str | None = None


class Position(PermissiveModel):
    symbol: str | None = None
    side: str | None = None


class PnlAmounts(PermissiveModel):
    realized_pnl: float | None = None
    unrealized_pnl: float | None = None
    total_pnl: float | None = None


class MarketValue(PermissiveModel):
    fair_value: float | None = None
    market_value: float | None = None
    currency: str | None = None


class ProviderStamp(PermissiveModel):
    market_data_provider: str | None = None


class BookFields(PermissiveModel):
    book_id: str | None = None
    name: str | None = None
    description: str | None = None
    expected_asset_class: str | None = None
    is_active: bool | None = None


class BookSummaryItem(BookFields):
    realized_pnl: float
    unrealized_pnl: float
    active_trades: int


class BookSummaryResponse(PermissiveModel):
    books: list[BookSummaryItem]


class LiveValuation(TradeRef, Position, PnlAmounts, MarketValue):
    """Latest tick from the pricing stream. Fields the stream does not send stay unset."""

    current_price: float | None = None
    entry_price: float | None = None
    valuation_time: str | None = None


class ValuationRecord(TradeRef, PnlAmounts, MarketValue, ProviderStamp):
    valuation_id: str | None = None
    valuation_time: str | None = None
    market_data_timestamp: str | None = None
    market_data_reference: str | None = None
    valuation_payload: dict[str, Any] | None = None
    created_at: str | None = None


class TradeRecord(TradeRef, Position, ProviderStamp):
    """One trade after `serialize()`. Money and timestamps are already JSON-safe."""

    instrument_id: str | None = None
    quantity: float | None = None
    trade_price: float | None = None
    trade_currency: str | None = None
    trade_date: str | None = None
    status: str | None = None
    opened_at: str | None = None
    closed_at: str | None = None
    close_price: float | None = None
    close_reason: str | None = None
    source: str | None = None
    reference_price: float | None = None
    reference_price_timestamp: str | None = None
    client_request_id: str | None = None
    metadata_payload: dict[str, Any] | None = None
    created_at: str | None = None
    updated_at: str | None = None
    valuation: LiveValuation | None = None


class TradeListResponse(BaseModel):
    trades: list[TradeRecord]


class TradeDetailResponse(BaseModel):
    trade: TradeRecord
    latest_valuation: LiveValuation | None = None
    valuation_history: list[ValuationRecord]
    audit_logs: list[Any] = Field(default_factory=list)


class ValuationListResponse(BaseModel):
    valuations: list[ValuationRecord]
