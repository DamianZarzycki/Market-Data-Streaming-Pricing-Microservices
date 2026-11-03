import asyncio
import logging

from core.fetch_audit import QUOTE_SERVICE, record_error
from core.http import provider_timeout_seconds


class MarketDataAggregator:
    def __init__(self, fetch_quote):
        self.fetch_quote = fetch_quote

    # Async: awaits asyncio.gather of the provider fetches.
    async def fetch(self, pairs: list[tuple[str, str]]) -> list[tuple[str, dict]]:
        results = await asyncio.gather(
            *[self.safe_fetch(key, symbol) for key, symbol in pairs],
            return_exceptions=True,
        )
        normalized: list[tuple[str, dict]] = []
        for pair, result in zip(pairs, results):
            if isinstance(result, Exception):
                key, symbol = pair
                logging.error("Aggregator failed for %s %s: %s", key, symbol, result)
                payload = {
                    "provider": key,
                    "symbol": symbol,
                    "error": str(result),
                    "status": "error",
                }
                record_error(QUOTE_SERVICE, payload)
                normalized.append(("error", payload))
            elif isinstance(result, tuple) and len(result) == 2:
                normalized.append(result)
        return normalized

    # Async: awaits fetch_quote, which awaits AsyncClient.get.
    async def safe_fetch(self, key: str, symbol: str) -> tuple[str, dict]:
        try:
            # The client times out at PROVIDER_FETCH_TIMEOUT_SECONDS. This wait
            # is a backstop so a stuck call still returns instead of hanging.
            return await asyncio.wait_for(
                self.fetch_quote(key, symbol),
                timeout=provider_timeout_seconds() + 1.0,
            )
        except asyncio.TimeoutError:
            payload = {
                "provider": key,
                "symbol": symbol,
                "error": "timeout",
                "status": "timeout",
            }
            record_error(QUOTE_SERVICE, payload)
            return "error", payload
        except Exception as exc:
            logging.error("Provider %s error for %s: %s", key, symbol, exc)
            payload = {
                "provider": key,
                "symbol": symbol,
                "error": str(exc),
                "status": "error",
            }
            record_error(QUOTE_SERVICE, payload)
            return "error", payload
