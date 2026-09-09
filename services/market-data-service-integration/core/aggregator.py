import asyncio
import logging
import os
from collections.abc import Callable

from core.fetch_audit import QUOTE_SERVICE, record_error


PROVIDER_FETCH_TIMEOUT_SECONDS = os.getenv("PROVIDER_FETCH_TIMEOUT_SECONDS", "10")


class MarketDataAggregator:
    def __init__(
        self,
        providers: dict,
        fetch_quote: Callable[[str, str], tuple[str, dict]],
    ):
        self.providers = providers
        self.fetch_quote = fetch_quote
        self._limits = {
            key: provider.semaphore_limit
            for key, provider in providers.items()
        }

    async def fetch(self, pairs: list[tuple[str, str]]) -> list[tuple[str, dict]]:
        semaphores = {
            key: asyncio.Semaphore(limit) for key, limit in self._limits.items()
        }
        results = await asyncio.gather(
            *[self.safe_fetch(key, symbol, semaphores) for key, symbol in pairs],
            return_exceptions=True,
        )
        normalized: list[tuple[str, dict]] = []
        for pair, result in zip(pairs, results):
            if isinstance(result, Exception):
                key, symbol = pair
                logging.error(
                    "Aggregator failed for %s %s: %s", key, symbol, result
                )
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

    async def safe_fetch(
        self,
        key: str,
        symbol: str,
        semaphores: dict[str, asyncio.Semaphore],
    ) -> tuple[str, dict]:
        semaphore = semaphores.get(key) or asyncio.Semaphore(1)
        try:
            async with semaphore:
                return await asyncio.wait_for(
                    asyncio.to_thread(self.fetch_quote, key, symbol),
                    timeout=float(PROVIDER_FETCH_TIMEOUT_SECONDS),
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
