from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict

from shared.trading_shared.enums import ActionType

_VALID_ACTION_TYPES = {ActionType.OPEN_TRADE.value, ActionType.CLOSE_TRADE.value}


class ServiceIdentity(BaseModel):
    service: str


class HealthResponse(ServiceIdentity):
    status: str


class QueueStats(BaseModel):
    depth: int
    capacity: int
    fill_pct: int
    backpressure: bool


class ActionNote(BaseModel):
    time: str
    client_request_id: str


class RecentReject(ActionNote):
    reason: str


class RecentAction(ActionNote):
    action_type: str
    symbol: str
    result: str
    latency_ms: int | None = None
    note: str | None = None


class StatusResponse(ServiceIdentity):
    pipeline_status: str
    worker_running: bool
    queue: QueueStats
    throughput_per_min: int
    processed: int
    errors: int
    avg_latency_ms: int | None = None
    rejected_400: int
    overload_503: int
    duplicates: int
    last_db_write: str | None = None
    recent_actions: list[RecentAction]
    recent_rejects: list[RecentReject]


class ProviderModeUpdate(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)

    use_provider_data: bool


class ProviderModeResponse(BaseModel):
    use_provider_data: bool


class ActionEnvelope(BaseModel):
    """Identity and provider fields shared by open and close.

    Extra keys stay on the model so option, IRS, and futures parameters still
    reach the worker. Those checks run after 202.
    """

    model_config = ConfigDict(extra="allow")

    action_type: Any = None
    client_request_id: Any = None
    market_data_provider: Any = None
    reference_price: Any = None
    reference_price_timestamp: Any = None
    source: Any = None
    symbol: Any = None

    def identity_error(self) -> str | None:
        if not self.action_type or self.action_type not in _VALID_ACTION_TYPES:
            return f"Missing or invalid action_type ('{self.action_type}')"
        if not self.client_request_id:
            return "Missing client_request_id"
        return None


class OpenTradeAction(ActionEnvelope):
    _REQUIRED_PROVIDER_FIELDS: ClassVar[tuple[str, ...]] = (
        "market_data_provider",
        "reference_price",
        "reference_price_timestamp",
        "source",
    )

    def missing_provider_field(self, require_provider: bool) -> str | None:
        if not require_provider:
            return None
        for field in self._REQUIRED_PROVIDER_FIELDS:
            if getattr(self, field) in (None, ""):
                return f"Missing {field}"
        return None


class CloseTradeAction(ActionEnvelope):
    trade_id: Any = None
    close_price: Any = None
    close_reason: Any = None


class AcceptedResponse(BaseModel):
    message: str
    client_request_id: Any


class BatchAcceptedResponse(BaseModel):
    message: str
    accepted_count: int
    errors: list[str]


def parse_action(raw: dict) -> OpenTradeAction | CloseTradeAction:
    action_type = raw.get("action_type")
    model = CloseTradeAction if action_type == ActionType.CLOSE_TRADE.value else OpenTradeAction
    return model.model_validate(raw)
