from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel


class HealthResponse(BaseModel):
    service: str
    status: str
    generated_events: int
    last_event_time: str | None = None
    last_symbol: str | None = None
    last_asset_type: str | None = None
    active_subscribers: int
    is_thread_alive: bool | None = None


class SymbolsResponse(RootModel[dict[str, list[str]]]):
    """GET /symbols. Asset class to the symbols in the current snapshot."""


class _Tick(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: int | None = None
    timestamp: str | None = None
    symbol: str | None = None


class EquityTick(_Tick):
    asset_type: Literal["EQUITY"]
    bid: float | None = None
    ask: float | None = None
    last: float | None = None


class BondTick(_Tick):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    asset_type: Literal["BOND"]
    bond_yield: float | None = Field(default=None, alias="yield")


class FxTick(_Tick):
    asset_type: Literal["FX"]
    spot: float | None = None


class OptionTick(_Tick):
    asset_type: Literal["OPTION"]
    currency: str | None = None
    spot: float | None = None
    strike: float | None = None
    option_right_type: str | None = None
    maturity_years: float | None = None
    volatility: float | None = None


class IrsTick(_Tick):
    asset_type: Literal["IRS"]
    currency: str | None = None
    notional: float | None = None
    fixed_rate: float | None = None
    maturity_years: float | None = None
    payments_per_year: int | None = None
    direction: str | None = None
    description: str | None = None


class BenchmarkTick(_Tick):
    asset_type: Literal["BENCHMARK"]
    currency: str | None = None
    last: float | None = None


class YieldCurveTick(_Tick):
    curve_id: str | None = None
    curve_name: str | None = None
    curve_type: Literal["YIELD_CURVE"]
    currency: str | None = None
    tenors: list[str] | None = None
    rates: list[float] | None = None
    provider: str | None = None


class MarketTick(_Tick):
    """One tick. GET /stream sends this JSON after `data: `. A partial tick uses this model too."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    asset_type: str | None = None
    currency: str | None = None
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    spot: float | None = None
    strike: float | None = None
    option_right_type: str | None = None
    maturity_years: float | None = None
    volatility: float | None = None
    notional: float | None = None
    fixed_rate: float | None = None
    payments_per_year: int | None = None
    direction: str | None = None
    description: str | None = None
    curve_id: str | None = None
    curve_name: str | None = None
    curve_type: str | None = None
    tenors: list[str] | None = None
    rates: list[float] | None = None
    provider: str | None = None
    bond_yield: float | None = Field(default=None, alias="yield")


Tick = Annotated[
    YieldCurveTick
    | OptionTick
    | IrsTick
    | BondTick
    | FxTick
    | EquityTick
    | BenchmarkTick
    | MarketTick,
    Field(union_mode="left_to_right"),
]


class SnapshotResponse(RootModel[dict[str, Tick]]):
    """GET /snapshot. Instrument key to the latest tick."""
