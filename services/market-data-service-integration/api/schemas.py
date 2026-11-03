from pydantic import BaseModel, ConfigDict


class FetchEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    time: str | None = None
    provider: str | None = None
    symbol: str | None = None
    error: str | None = None
    status: str | None = None


class FetchBucket(BaseModel):
    last_success: FetchEvent | None = None
    last_error: FetchEvent | None = None
    last_query_time: str | None = None
    error_count: int = 0


class HealthResponse(BaseModel):
    service: str
    status: str
    quote_service: FetchBucket
    curve_service: FetchBucket


class FuturesContractView(BaseModel):
    underlying: str
    multiplier: float


class SymbolMapResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    EQUITY: list[str] = []
    FX: list[str] = []
    BOND: list[str] = []
    IRS: list[str] = []
    FUTURES: list[str] = []
    FUTURES_CONTRACTS: dict[str, FuturesContractView] = {}


class ProvidersResponse(BaseModel):
    providers: list[str]
    curve_providers: list[str]


class QuotaWindow(BaseModel):
    used: int
    limit: int | None = None


class QuotaSnapshot(BaseModel):
    allowed: bool
    block_reason: str | None = None
    usage: dict[str, QuotaWindow]


class ProviderProbe(BaseModel):
    model_config = ConfigDict(extra="allow")

    provider: str
    kind: str
    reachable: bool
    quota_exceeded: bool
    status: str
    error: str | None = None
    quota: QuotaSnapshot | None = None


class ProvidersHealthResponse(BaseModel):
    checked_at: str
    status: str
    ok: int
    failed: int
    quota_exceeded: int
    providers: dict[str, ProviderProbe]


class QuoteResponse(BaseModel):
    """One normalized quote. Persisted rows carry extra columns."""

    model_config = ConfigDict(extra="allow")

    provider: str | None = None
    asset_class: str | None = None
    symbol: str | None = None
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    currency: str | None = None
    provider_timestamp: str | None = None
    received_at: str | None = None


class QuotesResponse(BaseModel):
    quotes: list[QuoteResponse]


class SnapshotResponse(BaseModel):
    snapshot: list[QuoteResponse]


class CurveResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    provider: str | None = None
    curve_name: str | None = None
    curve_type: str | None = None
    currency: str | None = None
    tenors: list | None = None
    rates: list | None = None


class CurvesResponse(BaseModel):
    curves: list[CurveResponse]


class QuoteRefreshResponse(BaseModel):
    time: str
    providers: list[str]
    symbols: list[str]
    requests: int
    success: int
    errors: int


class CurveRefreshResponse(BaseModel):
    time: str
    providers: list[str]
    fetched: int
    enqueued: int
    dropped: int
    queue_size: int
    persisted_total: int
    persist_errors_total: int
    errors: int


class QuoteStreamFrame(BaseModel):
    """One SSE `event: quote` payload. The body is not a JSON document."""

    model_config = ConfigDict(extra="allow")

    provider: str | None = None
    asset_class: str | None = None
    symbol: str | None = None
    last: float | None = None
    currency: str | None = None
