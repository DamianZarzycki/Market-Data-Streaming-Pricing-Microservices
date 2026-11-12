import asyncio
import json
import logging
import time

import httpx2

from shared.trading_shared.enums import ServiceStatus
from shared.trading_shared.env import env, env_float

PROBE_INTERVAL_SECONDS = env_float("MONITORING_PROBE_INTERVAL_SECONDS", 1.0, minimum=0.1)

from state import _iso_now, state

PROBE_TARGETS = (
    ("market-data-service", f"{env('MARKET_DATA_SERVICE_URL').rstrip('/')}/health"),
    ("pricing-service", f"{env('PRICING_SERVICE_URL').rstrip('/')}/health"),
    ("trade-generation-service", f"{env('TRADE_GENERATION_SERVICE_URL').rstrip('/')}/health"),
    ("trade-action-service", f"{env('TRADE_ACTION_SERVICE_URL').rstrip('/')}/health"),
    # Compose service name is books-service; cache key stays book-service for FE.
    ("book-service", f"{env('BOOKS_SERVICE_URL').rstrip('/')}/health"),
    ("blotter-service", f"{env('BLOTTER_SERVICE_URL').rstrip('/')}/health"),
)


def _clean_probe_error(exc: Exception) -> str:
    if isinstance(exc, httpx2.HTTPStatusError):
        reason = exc.response.reason_phrase or ""
        raw_error = f"HTTP Error {exc.response.status_code}: {reason}".rstrip()
    elif isinstance(exc, httpx2.TimeoutException):
        raw_error = str(exc).strip() or "timed out"
    else:
        raw_error = str(getattr(exc, "reason", "") or exc)
    if "]" in raw_error:
        raw_error = raw_error.split("]")[-1].strip()
    return raw_error or "request failed"


async def check_service_health(client: httpx2.AsyncClient, url: str, service_name: str):
    start_time = time.perf_counter()
    last_checked = _iso_now()
    logging.info(f"Checking health for {service_name}...")

    try:
        response = await client.get(url)
        response.raise_for_status()
        end_time = time.perf_counter()
        response_time_ms = int((end_time - start_time) * 1000)

        body = {}
        raw_body = response.content
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
        return {
            "status": ServiceStatus.DOWN.value,
            "error": _clean_probe_error(e),
            "last_checked": last_checked,
            "response_time_ms": response_time_ms,
        }


def apply_probe(service_name: str, probe: dict, now_ts: float) -> None:
    current_status = probe["status"]
    prev_status = state.previous_status(service_name, now_ts)

    if (
        current_status == ServiceStatus.DOWN.value
        and prev_status != ServiceStatus.DOWN.value
    ):
        logging.warning(
            f"Service Unavailability Detected: {service_name} is DOWN! "
            f"Error: {probe.get('error')}"
        )
        state.record_error(service_name, now_ts)
    elif (
        current_status == ServiceStatus.UP.value
        and prev_status == ServiceStatus.DOWN.value
    ):
        logging.info(f"Service Recovery Detected: {service_name} is back UP!")

    state.remember_status(service_name, current_status, now_ts)
    state.store_probe(service_name, probe, now_ts)


async def probe_once(client: httpx2.AsyncClient) -> None:
    state.sweep()
    results = await asyncio.gather(
        *(
            check_service_health(client, url, service_name)
            for service_name, url in PROBE_TARGETS
        )
    )
    now_ts = time.time()
    for (service_name, _url), probe in zip(PROBE_TARGETS, results, strict=True):
        apply_probe(service_name, probe, now_ts)


async def monitoring_worker(client: httpx2.AsyncClient):
    while True:
        await probe_once(client)
        await asyncio.sleep(PROBE_INTERVAL_SECONDS)
