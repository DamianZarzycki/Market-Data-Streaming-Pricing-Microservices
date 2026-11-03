import asyncio
import json
import logging
import os
import random
from datetime import datetime, timezone

# Seed prices for the default MARKET_DATA_SYMBOLS universe; anything else
# falls back to DEFAULT_BASE_PRICE so an unrecognized symbol still ticks.
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
JITTER_PCT = 0.0015  # +/- ~0.15% drift per tick, per (provider, symbol)


def mock_interval_seconds() -> float:
    try:
        return max(float(os.getenv("MARKET_DATA_MOCK_INTERVAL_SECONDS", "2")), 0.5)
    except ValueError:
        return 2.0


def _base_price(symbol: str) -> float:
    return BASE_PRICES.get(symbol.upper(), DEFAULT_BASE_PRICE)


def _drift(price: float) -> float:
    return round(price * (1 + random.uniform(-JITTER_PCT, JITTER_PCT)), 4)


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
