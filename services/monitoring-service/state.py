import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone

from shared.trading_shared.enums import ServiceStatus
from shared.trading_shared.env import env_int

from models import (
    ActivitySnapshot,
    EnvironmentStatus,
    Kpis,
    ServiceEntry,
    StatusResponse,
)

# A lap is at most the 2s client timeout plus the 1s sleep. 10s is about three missed laps.
PROBE_TTL_SECONDS = env_int("MONITORING_PROBE_TTL_SECONDS", 10)
# History for errors_5m. This is not the liveness of a probe reading.
ERROR_WINDOW_SECONDS = env_int("MONITORING_ERROR_WINDOW_SECONDS", 300)
DATA_WINDOW_LABEL = "last 5 min"

SHORT_LABELS = {
    "market-data-service": "market data",
    "pricing-service": "pricing",
    "trade-generation-service": "trade gen",
    "trade-action-service": "trade action",
    "book-service": "books",
    "blotter-service": "blotter",
}

_UP_VALUES = frozenset(
    (ServiceStatus.UP.value, "OK", "HEALTHY", "LIVE")
)


@dataclass
class CachedProbe:
    probe: dict
    expires_at: float


def _utc_now():
    return datetime.now(timezone.utc)


def _iso_now():
    return _utc_now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_iso(value):
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


_AGO_STEPS = (
    (60, 1, "s"),
    (3600, 60, "m"),
    (86400, 3600, "h"),
)


def _format_ago(iso_ts, now=None):
    parsed = _parse_iso(iso_ts)
    if parsed is None:
        return None
    now = now or _utc_now()
    delta = max(0, int((now - parsed).total_seconds()))
    return _ago_from_seconds(delta)


def _ago_from_seconds(delta: int) -> str:
    for limit, divisor, suffix in _AGO_STEPS:
        if delta < limit:
            return f"{delta // divisor}{suffix} ago"
    return f"{delta // 86400}d ago"


def _dot_join(*bits) -> str | None:
    text = " · ".join(bit for bit in bits if bit)
    return text or None


def _space_join(*bits) -> str:
    return " ".join(bit for bit in bits if bit)


def _errors_detail(by_service):
    if not by_service:
        return None
    parts = [
        f"{count} {SHORT_LABELS.get(service_name, service_name)}"
        for service_name, count in sorted(
            by_service.items(), key=lambda item: (-item[1], item[0])
        )
    ]
    return " · ".join(parts)


def _note_event(entry, kind, happened_at):
    if not happened_at:
        return
    entry["last_event"] = kind
    entry["last_event_at"] = happened_at


def _note_summary(entry, summary):
    entry["summary"] = summary
    entry["message"] = summary


def _enrich_market_data(entry, probe, now):
    happened_at = probe.get("last_event_time")
    _note_event(entry, "tick", happened_at)
    if probe.get("status") == ServiceStatus.UP.value:
        _note_summary(entry, "Streaming normally")
    detail = _market_tick_detail(probe, now)
    if detail and (probe.get("last_symbol") or probe.get("last_asset_type")):
        entry["last_event_detail"] = detail


def _enrich_pricing(entry, probe, now):
    _note_event(entry, "valuation", probe.get("last_pricing_time"))
    if probe.get("status") == ServiceStatus.UP.value:
        _note_summary(entry, "Pricing normally")


_SERVICE_ENRICHERS = {
    "market-data-service": _enrich_market_data,
    "pricing-service": _enrich_pricing,
}


def _enrich_service_entry(service_name, probe, errors_5m, now):
    entry = {
        "status": probe.get("status"),
        "last_checked": probe.get("last_checked"),
        "response_time_ms": probe.get("response_time_ms"),
        "errors_5m": errors_5m,
    }
    if probe.get("error"):
        entry["error"] = probe["error"]
    enrich = _SERVICE_ENRICHERS.get(service_name)
    if enrich is not None:
        enrich(entry, probe, now)
    if probe.get("status") == ServiceStatus.DOWN.value:
        _note_summary(entry, probe.get("error") or "Unreachable")
    return entry


def _is_up(probe) -> bool:
    return str(probe.get("status", "")).upper() in _UP_VALUES


def _environment_verdict(up_count: int, total: int) -> tuple[str, str]:
    down_count = total - up_count
    if down_count == 0:
        return "HEALTHY", "All probed services look healthy"
    if up_count == 0:
        return "DOWN", "All probed services are down"
    noun = "service" if down_count == 1 else "services"
    return "DEGRADED", f"Attention needed · {down_count} {noun} down"


def _market_tick_detail(probe, now) -> str | None:
    ago = _format_ago(probe.get("last_event_time"), now)
    label = _space_join(probe.get("last_symbol"), probe.get("last_asset_type"))
    if not label and not ago:
        return None
    return _dot_join(label, ago)


def _valuation_detail(probe, now) -> str | None:
    priced_at = probe.get("last_pricing_time")
    symbol = probe.get("last_valuation_symbol")
    if not priced_at and not symbol:
        return None
    ago = _format_ago(priced_at, now)
    if symbol:
        label = _space_join(symbol, probe.get("last_valuation_asset_class"))
        return _dot_join(label, ago)
    return _dot_join("Portfolio mark", ago)


def _activity_from_probes(services_probes, now):
    market = services_probes.get("market-data-service") or {}
    pricing = services_probes.get("pricing-service") or {}
    activity = ActivitySnapshot(
        last_market_tick=market.get("last_event_time"),
        last_market_tick_detail=_market_tick_detail(market, now),
        last_valuation=pricing.get("last_pricing_time"),
        last_valuation_detail=_valuation_detail(pricing, now),
    )
    return activity, market.get("last_symbol")


def _service_entries(services_probes, by_service, now):
    return {
        service_name: ServiceEntry.model_validate(
            _enrich_service_entry(
                service_name,
                probe,
                by_service.get(service_name, 0),
                now,
            )
        )
        for service_name, probe in services_probes.items()
    }


def _empty_status() -> StatusResponse:
    return StatusResponse(
        services={},
        environment=EnvironmentStatus(),
        kpis=Kpis(),
        alerts=[],
    )


def build_status_payload(services_probes, now_ts, by_service) -> StatusResponse:
    now = datetime.fromtimestamp(now_ts, tz=timezone.utc)
    up_count = sum(1 for probe in services_probes.values() if _is_up(probe))
    env_status, env_message = _environment_verdict(up_count, len(services_probes))
    activity, last_market_symbol = _activity_from_probes(services_probes, now)
    activity_fields = activity.model_dump()
    total_errors = sum(by_service.values())
    return StatusResponse(
        services=_service_entries(services_probes, by_service, now),
        environment=EnvironmentStatus(
            status=env_status,
            message=env_message,
            services_up=up_count,
            services_total=len(services_probes),
            data_window=DATA_WINDOW_LABEL,
            errors_5m=total_errors,
            errors_by_service=by_service,
            last_market_tick_symbol=last_market_symbol,
            **activity_fields,
        ),
        kpis=Kpis(
            errors_5m=total_errors,
            errors_detail=_errors_detail(by_service),
            **activity_fields,
        ),
        alerts=[],
    )


class MonitoringState:
    def __init__(self):
        self.health_cache: dict[str, CachedProbe] = {}
        self.previous_states: dict[str, tuple[str, float]] = {}
        self._error_events: deque[tuple[float, str]] = deque()

    def reset(self) -> None:
        self.health_cache.clear()
        self.previous_states.clear()
        self._error_events.clear()

    def sweep(self, now_ts: float | None = None) -> float:
        now_ts = time.time() if now_ts is None else now_ts
        expired = [
            name
            for name, entry in self.health_cache.items()
            if entry.expires_at <= now_ts
        ]
        for name in expired:
            del self.health_cache[name]
        expired_previous = [
            name
            for name, (_status, expires_at) in self.previous_states.items()
            if expires_at <= now_ts
        ]
        for name in expired_previous:
            del self.previous_states[name]
        self._prune_errors(now_ts)
        return now_ts

    def previous_status(self, service_name: str, now_ts: float) -> str | None:
        stored = self.previous_states.get(service_name)
        if stored is None:
            return None
        status, expires_at = stored
        if expires_at <= now_ts:
            del self.previous_states[service_name]
            return None
        return status

    def remember_status(self, service_name: str, status: str, now_ts: float) -> None:
        self.previous_states[service_name] = (
            status,
            now_ts + PROBE_TTL_SECONDS,
        )

    def store_probe(self, service_name: str, probe: dict, now_ts: float) -> None:
        self.health_cache[service_name] = CachedProbe(
            probe=dict(probe),
            expires_at=now_ts + PROBE_TTL_SECONDS,
        )

    def _prune_errors(self, now_ts: float) -> None:
        cutoff = now_ts - ERROR_WINDOW_SECONDS
        while self._error_events and self._error_events[0][0] < cutoff:
            self._error_events.popleft()

    def record_error(self, service_name: str, now_ts: float) -> None:
        self._error_events.append((now_ts, service_name))
        self._prune_errors(now_ts)

    def errors_by_service(self, now_ts: float) -> dict[str, int]:
        self._prune_errors(now_ts)
        counts = defaultdict(int)
        for _, service_name in self._error_events:
            counts[service_name] += 1
        return dict(counts)

    def snapshot(self, now_ts: float | None = None) -> StatusResponse:
        now_ts = self.sweep(now_ts)
        probes = {
            name: entry.probe for name, entry in self.health_cache.items()
        }
        if not probes:
            return _empty_status()
        return build_status_payload(
            probes, now_ts, self.errors_by_service(now_ts)
        )


state = MonitoringState()
