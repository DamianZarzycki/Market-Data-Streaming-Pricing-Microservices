import asyncio
import json
import logging

import httpx2

from cache.valuation_cache_service import ValuationCacheService
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.enums import EventType
from shared.trading_shared.env import env, env_float, env_int

MAX_RECONNECT_DELAY = env_int("BLOTTER_MAX_RECONNECT_DELAY_SECONDS", 30)
BASE_RECONNECT_DELAY = env_int("BLOTTER_BASE_RECONNECT_DELAY_SECONDS", 1, minimum=0)
STREAM_URL = f"{env('PRICING_SERVICE_URL').rstrip('/')}/valuation-stream"
STREAM_TIMEOUT_SECONDS = env_float("BLOTTER_STREAM_TIMEOUT_SECONDS", 30.0, minimum=0.1)

cache_service = ValuationCacheService()
audit_logger = AuditLogger("blotter-service")
http_client: httpx2.AsyncClient | None = None


def open_http() -> None:
    """Build the stream client. The first request is the await, not this call."""
    global http_client
    http_client = httpx2.AsyncClient(
        timeout=httpx2.Timeout(STREAM_TIMEOUT_SECONDS, read=STREAM_TIMEOUT_SECONDS)
    )


# Async: awaits AsyncClient.aclose.
async def close_http() -> None:
    global http_client
    client = http_client
    http_client = None
    if client is not None:
        await client.aclose()


def parse_valuation_line(line: str):
    decoded = line.strip()
    if not decoded.startswith("data:"):
        return None
    data_str = decoded[len("data:"):].strip()
    try:
        valuation_data = json.loads(data_str)
    except json.JSONDecodeError as exc:
        logging.error("Failed to parse valuation data: %s", exc)
        return None
    if not isinstance(valuation_data, dict):
        return None
    if not valuation_data.get("trade_id"):
        return None
    return valuation_data


# Async generator: awaits AsyncClient.stream and aiter_lines on the valuation SSE body.
async def iter_valuations():
    if http_client is None:
        raise RuntimeError("HTTP client is not open")
    logging.info("Attempting to connect to the stream...")
    async with http_client.stream("GET", STREAM_URL) as response:
        response.raise_for_status()
        logging.info("Connected to the stream successfully.")
        await audit_logger.ainfo(
            EventType.STREAM_CONNECTED,
            "Connected to pricing-service valuation stream",
        )
        logging.info("Connected to valuation stream, starting to read events...")
        async for line in response.aiter_lines():
            valuation_data = parse_valuation_line(line)
            if valuation_data is None:
                continue
            yield valuation_data


# Async task: awaits the valuation SSE body and asyncio.sleep between reconnects.
async def valuation_worker():
    logging.info("Valuation worker started.")
    reconnect_delay = BASE_RECONNECT_DELAY
    while True:
        try:
            async for valuation_data in iter_valuations():
                reconnect_delay = BASE_RECONNECT_DELAY
                cache_service.update_cache(valuation_data["trade_id"], valuation_data)
                logging.info(
                    "Updated valuation cache for trade %s",
                    valuation_data["trade_id"],
                )
            reconnect_delay = BASE_RECONNECT_DELAY
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.error(
                "Connection lost: %s. Reconnecting in %ss...",
                exc,
                reconnect_delay,
            )
            await audit_logger.aerror(
                EventType.STREAM_DISCONNECTED,
                "Lost connection to valuation stream: %s" % exc,
            )
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, MAX_RECONNECT_DELAY)
