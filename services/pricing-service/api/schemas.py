from pydantic import BaseModel, ConfigDict, RootModel


class HealthResponse(BaseModel):
    service: str
    status: str
    market_data_connection: str
    received_events: int
    last_market_event_time: str | None = None
    last_pricing_time: str | None = None
    last_valuation_symbol: str | None = None
    last_valuation_asset_class: str | None = None


class ValuationResponse(BaseModel):
    """One mark. Provider and IRS fields ride along when the pricer set them."""

    model_config = ConfigDict(extra="allow")

    trade_id: str | None = None
    book_id: str | None = None
    asset_class: str | None = None
    symbol: str | None = None
    side: str | None = None
    fair_value: float | None = None
    market_value: float | None = None
    current_price: float | None = None
    entry_price: float | None = None
    unrealized_pnl: float | None = None
    realized_pnl: float | None = None
    total_pnl: float | None = None
    currency: str | None = None
    valuation_time: str | None = None
    market_data_provider: str | None = None
    market_data_timestamp: str | None = None
    is_stale: bool | None = None
    market_data_age_seconds: float | None = None
    pricing_details: dict | None = None


class ValuationMapResponse(RootModel[dict[str, ValuationResponse]]):
    """GET /valuations. trade_id to the latest mark."""


class BookMetricsResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    book_id: str | None = None
    alpha: float | None = None
    beta: float | None = None
    observations: int | None = None
    window: int | None = None
    avg_book_return: float | None = None
    avg_benchmark_return: float | None = None
    benchmark_symbol: str | None = None
    currency: str | None = None
    updated_at: str | None = None


class BookMetricsListResponse(BaseModel):
    benchmark: str
    books: dict[str, BookMetricsResponse]
