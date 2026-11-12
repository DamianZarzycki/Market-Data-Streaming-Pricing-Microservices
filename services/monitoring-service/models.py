from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    service: str
    status: str


class ProbeReading(BaseModel):
    status: str | None = None
    last_checked: str | None = None
    response_time_ms: int | None = None
    error: str | None = None


class ServiceEntry(ProbeReading):
    errors_5m: int | None = None
    last_event: str | None = None
    last_event_at: str | None = None
    summary: str | None = None
    message: str | None = None
    last_event_detail: str | None = None


class ActivitySnapshot(BaseModel):
    last_market_tick: str | None = None
    last_market_tick_detail: str | None = None
    last_valuation: str | None = None
    last_valuation_detail: str | None = None


class ErrorWindow(BaseModel):
    errors_5m: int | None = None


class Kpis(ActivitySnapshot, ErrorWindow):
    errors_detail: str | None = None


class EnvironmentStatus(ActivitySnapshot, ErrorWindow):
    status: str | None = None
    message: str | None = None
    services_up: int | None = None
    services_total: int | None = None
    data_window: str | None = None
    errors_by_service: dict[str, int] | None = None
    last_market_tick_symbol: str | None = None


class StatusResponse(BaseModel):
    services: dict[str, ServiceEntry] = Field(default_factory=dict)
    environment: EnvironmentStatus = Field(default_factory=EnvironmentStatus)
    kpis: Kpis = Field(default_factory=Kpis)
    alerts: list = Field(default_factory=list)
