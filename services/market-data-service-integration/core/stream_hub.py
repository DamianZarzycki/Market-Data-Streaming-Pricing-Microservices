import asyncio
import json
import logging
import os
import threading

from fastapi import status

from api.errors import ApiError
from core import state
from core.mock_quote_stream import iter_mock_quote_events
from core.polling.config import read_poll_symbols

subscribers: list[tuple[set[str] | None, asyncio.Queue]] = []
_lock = threading.Lock()


def _wants_mock(raw: str) -> bool:
    return (raw or "").strip().lower() in ("1", "true", "yes")


def _env_flag(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in ("1", "true", "yes")


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
    provider = str(payload.get("provider") or "").strip().lower()
    frame = f"event: quote\ndata: {json.dumps(payload)}\n\n"
    with _lock:
        targets = list(subscribers)
    for keys, queue in targets:
        if keys is not None and provider not in keys:
            continue
        queue.put_nowait(frame)


def open_quote_stream(provider: str | None, mock_query: str):
    """Return the async body for one SSE response. Building it does not await."""
    service = state.get_quote_service()
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
