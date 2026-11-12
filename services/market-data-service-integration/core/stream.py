import asyncio
import json
import logging
import random
import threading
from datetime import datetime, timezone

from fastapi import status

from shared.trading_shared.env import env_bool, env_float
from shared.trading_shared.http_errors import ApiError
from core.polling import read_poll_symbols
from core.runtime import get_quote_service

BASE_PRICES = {
    "AAPL": 336.13,
    "MSFT": 450.20,
    "TSLA": 250.75,
    "GOOGL": 180.40,
    "AMZN": 200.10,
    "NVDA": 120.55,
    "SPY": 560.30,
}
DEFAULT_BASE_PRICE = 100.0
JITTER_PCT = 0.0015

subscribers: list[tuple[set[str] | None, asyncio.Queue]] = []
_lock = threading.Lock()


def mock_interval_seconds() -> float:
    return max(env_float("MARKET_DATA_MOCK_INTERVAL_SECONDS", 2.0), 0.5)


def _base_price(symbol: str) -> float:
    return BASE_PRICES.get(symbol.upper(), DEFAULT_BASE_PRICE)


def _drift(price: float) -> float:
    return round(price * (1 + random.uniform(-JITTER_PCT, JITTER_PCT)), 4)


def _wants_mock(raw: str) -> bool:
    return (raw or "").strip().lower() in ("1", "true", "yes")


def _env_flag(name: str) -> bool:
    return env_bool(name, False)


def subscribe(provider_keys: set[str] | None) -> asyncio.Queue:
    """Register one SSE client. The queue is in memory, so this stays synchronous."""
    queue: asyncio.Queue = asyncio.Queue()
    with _lock:
        subscribers.append((provider_keys, queue))
        subscriber_count = len(subscribers)
    logging.info("Client connected to market-data stream. Active subscribers: %s", subscriber_count)
    return queue


def unsubscribe(queue: asyncio.Queue) -> None:
    with _lock:
        subscribers[:] = [item for item in subscribers if item[1] is not queue]
        subscriber_count = len(subscribers)
    logging.info("Client disconnected from market-data stream. Active subscribers: %s", subscriber_count)


def publish_quote(payload: dict) -> None:
    """Fan one quote out. put_nowait does not wait, so this stays synchronous."""
    provider = payload.get("provider")
    if not isinstance(provider, str):
        return
    provider = provider.strip().lower()
    frame = f"event: quote\ndata: {json.dumps(payload)}\n\n"
    with _lock:
        targets = list(subscribers)
    for keys, queue in targets:
        if keys is not None and provider not in keys:
            continue
        queue.put_nowait(frame)


def open_quote_stream(provider: str | None, mock_query: str):
    """Return the async body for one SSE response. Building it does not await."""
    service = get_quote_service()
    provider_keys = None
    if provider is not None:
        provider_key = provider.strip().lower()
        if provider_key not in service.providers:
            raise ApiError(status.HTTP_404_NOT_FOUND, f"Unknown provider: {provider}")
        provider_keys = {provider_key}

    names = sorted(provider_keys) if provider_keys else list(service.providers.keys())
    symbols = read_poll_symbols()
    if _wants_mock(mock_query) or _env_flag("MARKET_DATA_MOCK_STREAM"):
        logging.info(
            "Client connected to MOCK market-data stream providers=%s symbols=%s",
            names,
            symbols,
        )
        return iter_mock_quote_events(names, symbols)

    logging.info(
        "Client connected to market-data stream providers=%s symbols=%s",
        names,
        symbols,
    )
    return iter_subscriber(subscribe(provider_keys))


# Async generator: awaits asyncio.sleep between ticks. Mock frames are not persisted.
async def iter_mock_quote_events(providers: list[str], symbols: list[str]):
    yield ": connected (mock)\n\n"
    interval = mock_interval_seconds()
    prices = {
        (provider, symbol): _base_price(symbol)
        for provider in providers
        for symbol in symbols
    }
    logging.info(
        "Streaming MOCK quotes providers=%s symbols=%s interval=%ss",
        providers,
        symbols,
        interval,
    )

    while True:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for symbol in symbols:
            for provider in providers:
                key = (provider, symbol)
                prices[key] = _drift(prices[key])
                payload = {
                    "provider": provider.upper(),
                    "asset_class": "EQUITY",
                    "symbol": symbol,
                    "bid": None,
                    "ask": None,
                    "last": prices[key],
                    "currency": "USD",
                    "provider_timestamp": now,
                    "received_at": now,
                    "raw_payload": {"mock": True},
                }
                yield f"event: quote\ndata: {json.dumps(payload)}\n\n"
        await asyncio.sleep(interval)


# Async generator: awaits asyncio.Queue.get, which suspends this request and holds no thread.
# None closes the generator so a test can finish the response.
async def iter_subscriber(queue: asyncio.Queue):
    try:
        yield ": connected\n\n"
        while True:
            frame = await queue.get()
            if frame is None:
                break
            yield frame
    finally:
        unsubscribe(queue)
