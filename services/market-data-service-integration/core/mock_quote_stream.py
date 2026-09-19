"""Synthetic replacement for the real provider fetch loop behind
GET /market-data/stream. Emits the same "quote" SSE event shape as the real
providers (see providers/quota_providers/*.py normalize_data) with a small
random price drift each tick, so a client can develop against the stream
without real provider API keys or burning their rate limits.

Not wired to persistence — mock ticks are never written to
market_data_provider_quotes, so they can't be mistaken for real history.
"""

import json
import logging
import os
import random
import time
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
    "EURUSD": 1.0850,
    "GBPUSD": 1.2700,
    "USDJPY": 150.20,
}
DEFAULT_BASE_PRICE = 100.0
FX_SYMBOLS = {"EURUSD", "GBPUSD", "USDJPY"}
JITTER_PCT = 0.0015  # +/- ~0.15% drift per tick, per (provider, symbol)


def mock_interval_seconds() -> float:
    try:
        return max(float(os.getenv("MARKET_DATA_MOCK_INTERVAL_SECONDS", "2")), 0.5)
    except ValueError:
        return 2.0


def _asset_class_for(symbol: str) -> str:
    return "FX" if symbol.upper() in FX_SYMBOLS else "EQUITY"


def _base_price(symbol: str) -> float:
    return BASE_PRICES.get(symbol.upper(), DEFAULT_BASE_PRICE)


def _drift(price: float) -> float:
    return round(price * (1 + random.uniform(-JITTER_PCT, JITTER_PCT)), 4)


def iter_mock_quote_events(providers: list[str], symbols: list[str]):
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
                    "asset_class": _asset_class_for(symbol),
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
        time.sleep(interval)
