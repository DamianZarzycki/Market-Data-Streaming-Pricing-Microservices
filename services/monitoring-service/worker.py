import json
import logging
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from urllib import request

from shared.trading_shared.enums import ServiceStatus

ERROR_WINDOW_SECONDS = 300
DATA_WINDOW_LABEL = "last 5 min"

# Probe map only — FE-facing envelope is built in build_status_payload().
health_cache = {}
status_payload = {
    "services": {},
    "environment": {},
    "kpis": {},
    "alerts": [],
}

previous_states = {
    "market-data-service": None,
    "pricing-service": None,
    "trade-generation-service": None,
    "trade-action-service": None,
    "book-service": None,
    "blotter-service": None,
}

# Sliding window of (unix_ts, service_name) for DOWN transitions.
_error_events = deque()

PROBE_TARGETS = (
    ("market-data-service", "http://market-data-service:8001/health"),
    ("pricing-service", "http://pricing-service:8002/health"),
    ("trade-generation-service", "http://trade-generation-service:8007/health"),
    ("trade-action-service", "http://trade-action-service:8080/health"),
    # Compose service name is books-service; cache key stays book-service for FE.
    ("book-service", "http://books-service:8004/health"),
    ("blotter-service", "http://blotter-service:8006/health"),
)

SHORT_LABELS = {
    "market-data-service": "market data",
    "pricing-service": "pricing",
    "trade-generation-service": "trade gen",
    "trade-action-service": "trade action",
    "book-service": "books",
    "blotter-service": "blotter",
}


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


def _format_ago(iso_ts, now=None):
    parsed = _parse_iso(iso_ts)
    if parsed is None:
        return None
    now = now or _utc_now()
    delta = max(0, int((now - parsed).total_seconds()))
    if delta < 60:
        return f"{delta}s ago"
    if delta < 3600:
        return f"{delta // 60}m ago"
    if delta < 86400:
        return f"{delta // 3600}h ago"
    return f"{delta // 86400}d ago"


def _prune_errors(now_ts):
    cutoff = now_ts - ERROR_WINDOW_SECONDS
    while _error_events and _error_events[0][0] < cutoff:
        _error_events.popleft()


def _record_error(service_name, now_ts):
    _error_events.append((now_ts, service_name))
    _prune_errors(now_ts)


def _errors_by_service(now_ts):
    _prune_errors(now_ts)
    counts = defaultdict(int)
    for _, service_name in _error_events:
        counts[service_name] += 1
    return dict(counts)


def _errors_detail(by_service):
    if not by_service:
        return None
    parts = []
    for service_name, count in sorted(
        by_service.items(), key=lambda item: (-item[1], item[0])
    ):
        label = SHORT_LABELS.get(service_name, service_name)
        parts.append(f"{count} {label}")
    return " · ".join(parts)


def check_service_health(url, service_name):
    start_time = time.perf_counter()
    last_checked = _iso_now()
    logging.info(f"Checking health for {service_name}...")

    try:
        with request.urlopen(url, timeout=2) as response:
            raw_body = response.read()
            end_time = time.perf_counter()
            response_time_ms = int((end_time - start_time) * 1000)

            body = {}
            if raw_body:
                try:
                    parsed = json.loads(raw_body.decode("utf-8"))
                    if isinstance(parsed, dict):
                        body = parsed
                except (UnicodeDecodeError, json.JSONDecodeError):
                    logging.warning(f"Non-JSON health payload from {service_name}")

            status = body.get("status") or ServiceStatus.UP.value
            result = {
                "status": status,
                "last_checked": last_checked,
                "response_time_ms": response_time_ms,
            }

            # Surface useful fields from richer /health payloads.
            for key in (
                "last_event_time",
                "last_symbol",
                "last_asset_type",
                "last_pricing_time",
                "last_valuation_symbol",
                "last_valuation_asset_class",
                "generated_events",
                "received_events",
                "market_data_connection",
            ):
                if key in body and body[key] is not None:
                    result[key] = body[key]

            return result
    except Exception as e:
        end_time = time.perf_counter()
        response_time_ms = int((end_time - start_time) * 1000)
        raw_error = str(e.reason) if hasattr(e, "reason") else str(e)

        if "]" in raw_error:
            clean_error = raw_error.split("]")[-1].strip()
        else:
            clean_error = raw_error

        return {
            "status": ServiceStatus.DOWN.value,
            "error": clean_error,
            "last_checked": last_checked,
            "response_time_ms": response_time_ms,
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

    if service_name == "market-data-service":
        last_event_at = probe.get("last_event_time")
        symbol = probe.get("last_symbol")
        asset_type = probe.get("last_asset_type")
        if last_event_at:
            entry["last_event"] = "tick"
            entry["last_event_at"] = last_event_at
        if probe.get("status") == ServiceStatus.UP.value:
            entry["summary"] = "Streaming normally"
            entry["message"] = entry["summary"]
        if symbol or asset_type:
            detail_bits = [bit for bit in (symbol, asset_type) if bit]
            ago = _format_ago(last_event_at, now)
            entry["last_event_detail"] = " · ".join(
                bit for bit in (" ".join(detail_bits), ago) if bit
            )

    elif service_name == "pricing-service":
        last_event_at = probe.get("last_pricing_time")
        if last_event_at:
            entry["last_event"] = "valuation"
            entry["last_event_at"] = last_event_at
        if probe.get("status") == ServiceStatus.UP.value:
            entry["summary"] = "Pricing normally"
            entry["message"] = entry["summary"]

    if probe.get("status") == ServiceStatus.DOWN.value:
        entry["summary"] = probe.get("error") or "Unreachable"
        entry["message"] = entry["summary"]

    return entry


def build_status_payload(services_probes, now_ts):
    now = datetime.fromtimestamp(now_ts, tz=timezone.utc)
    by_service = _errors_by_service(now_ts)
    total_errors = sum(by_service.values())

    services = {}
    for service_name, probe in services_probes.items():
        services[service_name] = _enrich_service_entry(
            service_name,
            probe,
            by_service.get(service_name, 0),
            now,
        )

    up_count = sum(
        1
        for probe in services_probes.values()
        if str(probe.get("status", "")).upper()
        in (ServiceStatus.UP.value, "OK", "HEALTHY", "LIVE")
    )
    total = len(services_probes)
    down_count = total - up_count

    if down_count == 0:
        env_status = "HEALTHY"
        env_message = "All probed services look healthy"
    elif up_count == 0:
        env_status = "DOWN"
        env_message = "All probed services are down"
    else:
        env_status = "DEGRADED"
        env_message = (
            f"Attention needed · {down_count} service"
            f"{'' if down_count == 1 else 's'} down"
        )

    md = services_probes.get("market-data-service") or {}
    pricing = services_probes.get("pricing-service") or {}

    last_market_tick = md.get("last_event_time")
    last_market_symbol = md.get("last_symbol")
    last_market_asset = md.get("last_asset_type")
    last_market_ago = _format_ago(last_market_tick, now)
    last_market_detail = None
    if last_market_symbol or last_market_asset or last_market_ago:
        label = " ".join(
            bit for bit in (last_market_symbol, last_market_asset) if bit
        )
        last_market_detail = " · ".join(
            bit for bit in (label or None, last_market_ago) if bit
        )

    last_valuation = pricing.get("last_pricing_time")
    last_valuation_symbol = pricing.get("last_valuation_symbol")
    last_valuation_asset = pricing.get("last_valuation_asset_class")
    last_valuation_ago = _format_ago(last_valuation, now)
    last_valuation_detail = None
    if last_valuation or last_valuation_symbol:
        if last_valuation_symbol:
            label = " ".join(
                bit
                for bit in (last_valuation_symbol, last_valuation_asset)
                if bit
            )
            last_valuation_detail = " · ".join(
                bit for bit in (label, last_valuation_ago) if bit
            )
        else:
            last_valuation_detail = " · ".join(
                bit for bit in ("Portfolio mark", last_valuation_ago) if bit
            )

    errors_detail = _errors_detail(by_service)

    environment = {
        "status": env_status,
        "message": env_message,
        "services_up": up_count,
        "services_total": total,
        "data_window": DATA_WINDOW_LABEL,
        "errors_5m": total_errors,
        "errors_by_service": by_service,
        "last_market_tick": last_market_tick,
        "last_market_tick_symbol": last_market_symbol,
        "last_market_tick_detail": last_market_detail,
        "last_valuation": last_valuation,
        "last_valuation_detail": last_valuation_detail,
    }

    kpis = {
        "errors_5m": total_errors,
        "errors_detail": errors_detail,
        "last_market_tick": last_market_tick,
        "last_market_tick_detail": last_market_detail,
        "last_valuation": last_valuation,
        "last_valuation_detail": last_valuation_detail,
    }

    return {
        "services": services,
        "environment": environment,
        "kpis": kpis,
        "alerts": [],
    }


def monitoring_worker():
    global health_cache, status_payload

    while True:
        current_states = {}
        for service_name, url in PROBE_TARGETS:
            current_states[service_name] = check_service_health(url, service_name)

        now_ts = time.time()

        for service_name, status_data in current_states.items():
            current_status = status_data["status"]
            prev_status = previous_states[service_name]

            if (
                current_status == ServiceStatus.DOWN.value
                and prev_status != ServiceStatus.DOWN.value
            ):
                logging.warning(
                    f"Service Unavailability Detected: {service_name} is DOWN! "
                    f"Error: {status_data.get('error')}"
                )
                _record_error(service_name, now_ts)

            elif (
                current_status == ServiceStatus.UP.value
                and prev_status == ServiceStatus.DOWN.value
            ):
                logging.info(
                    f"Service Recovery Detected: {service_name} is back UP!"
                )

            previous_states[service_name] = current_status

        # Flat map kept for any consumers that still expect the old shape
        # via status_payload["services"].
        health_cache = {
            name: {
                "status": probe.get("status"),
                "last_checked": probe.get("last_checked"),
                "response_time_ms": probe.get("response_time_ms"),
                **(
                    {"error": probe["error"]}
                    if probe.get("error")
                    else {}
                ),
            }
            for name, probe in current_states.items()
        }
        status_payload = build_status_payload(current_states, now_ts)

        time.sleep(1)
