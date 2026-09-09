import asyncio
import logging
import os

from providers.quota_providers._base import ProviderAPIError


PROVIDER_FETCH_TIMEOUT_SECONDS = os.getenv("PROVIDER_FETCH_TIMEOUT_SECONDS", "1.0")


class MarketDataAggregator:
    def __init__(self, providers: dict):
        self.providers = providers
        self.semaphores = {
            key: asyncio.Semaphore(provider.semaphore_limit)
            for key, provider in providers.items()
        }

    async def fetch(self, pairs: list[tuple[str, str]]) -> list[dict]:
        results = await asyncio.gather(
            *[self.safe_fetch(key, symbol) for key, symbol in pairs],
            return_exceptions=True,
        )
        normalized = []
        for pair, result in zip(pairs, results):
            if isinstance(result, Exception):
                key, symbol = pair
                logging.error(
                    "Aggregator failed for %s %s: %s", key, symbol, result
                )
                normalized.append(
                    {
                        "ok": False,
                        "provider": key,
                        "symbol": symbol,
                        "status": "error",
                        "reason": str(result),
                    }
                )
            else:
                normalized.append(result)
        return normalized

    async def safe_fetch(self, key: str, symbol: str) -> dict:
        provider = self.providers[key]
        try:
            async with self.semaphores[key]:
                quote = await asyncio.wait_for(
                    asyncio.to_thread(provider.fetch_data, symbol),
                    timeout=float(PROVIDER_FETCH_TIMEOUT_SECONDS),
                )
            return {
                "ok": True,
                "provider": key,
                "symbol": symbol,
                "quote": quote,
            }
        except asyncio.TimeoutError:
            return {
                "ok": False,
                "provider": key,
                "symbol": symbol,
                "status": "timeout",
                "reason": "timeout",
            }
        except ProviderAPIError as exc:
            return {
                "ok": False,
                "provider": key,
                "symbol": symbol,
                "status": exc.status,
                "reason": str(exc),
            }
        except Exception as exc:
            logging.error("Provider %s error for %s: %s", key, symbol, exc)
            return {
                "ok": False,
                "provider": key,
                "symbol": symbol,
                "status": "error",
                "reason": str(exc),
            }
