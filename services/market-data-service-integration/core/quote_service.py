import logging

import httpx2
from fastapi import status

from api.errors import ApiError
from core.aggregator import MarketDataAggregator
from core.fetch_audit import QUOTE_SERVICE, record_error, record_query, record_success
from core.polling.config import futures_quotes_for_equity
from core.request_tracker import RequestTracker
from providers.quota_providers.alpha_vantage_data_provider import AlphaVantageProvider
from providers.quota_providers.finnhub_data_provider import FinnhubProvider
from providers.quota_providers.twelve_data_provider import TwelveDataProvider
from shared.trading_shared.db import AsyncDBSessionManager
from shared.trading_shared.enums import AssetClass
from shared.trading_shared.serialization import serialize


def default_providers() -> dict:
    return {
        "finnhub": FinnhubProvider(),
        "alpha_vantage": AlphaVantageProvider(),
        "twelve_data": TwelveDataProvider(),
    }


class QuoteService:
    def __init__(self, providers: dict, tracker: RequestTracker, quote_queue):
        self.providers = providers
        self.tracker = tracker
        self._queue = quote_queue
        self.aggregator = MarketDataAggregator(self.fetch_quote)

    @classmethod
    def create(cls, tracker: RequestTracker, quote_queue) -> "QuoteService":
        return cls(default_providers(), tracker, quote_queue)

    def persist_quote(self, quote: dict, fallback_symbol: str | None = None) -> None:
        """Enqueue a quote. put_nowait does not wait, so this stays synchronous."""
        payload = dict(quote)
        if not payload.get("symbol") and fallback_symbol:
            payload["symbol"] = fallback_symbol
        self._queue.put_nowait(payload)
        logging.info(
            "Enqueued %s quote for %s last=%s",
            payload.get("provider"),
            payload.get("symbol"),
            payload.get("last"),
        )
        for mirrored in futures_quotes_for_equity(payload):
            self._queue.put_nowait(mirrored)
            logging.info(
                "Enqueued %s futures quote for %s last=%s underlying=%s",
                mirrored.get("provider"),
                mirrored.get("symbol"),
                mirrored.get("last"),
                (mirrored.get("raw_payload") or {}).get("underlying_symbol"),
            )

    # Async: awaits provider_quotes.get_latest_for_symbol_async.
    async def get_quotes(self, symbol: str, asset_class: str) -> dict:
        symbol = (symbol or "").strip().upper()
        if not symbol:
            raise ApiError(status.HTTP_400_BAD_REQUEST, "symbol is required")
        asset_class = (asset_class or "").strip().upper()
        if asset_class not in {item.value for item in AssetClass}:
            raise ApiError(status.HTTP_400_BAD_REQUEST, f"Invalid asset_class: {asset_class}")
        async with AsyncDBSessionManager() as db:
            rows = await db.provider_quotes.get_latest_for_symbol_async(symbol, asset_class)
            return {"quotes": [serialize(row) for row in rows]}

    # Async: awaits provider_quotes.get_all_latest_async.
    async def get_snapshot(self) -> dict:
        """Latest normalized quote per (provider, symbol). Backs GET /market-data/snapshot."""
        async with AsyncDBSessionManager() as db:
            rows = await db.provider_quotes.get_all_latest_async()
            return {"snapshot": [serialize(row) for row in rows]}

    # Async: awaits provider_quotes.get_latest_async.
    async def get_quote(self, provider: str, symbol: str) -> dict:
        provider_key = provider.strip().lower()
        if provider_key not in self.providers:
            raise ApiError(status.HTTP_404_NOT_FOUND, f"Unknown provider: {provider}")
        symbol = symbol.strip().upper()
        async with AsyncDBSessionManager() as db:
            row = await db.provider_quotes.get_latest_async(provider_key.upper(), symbol)
        if row is None:
            raise ApiError(status.HTTP_404_NOT_FOUND, "Quote not found")
        return serialize(row)

    # Async: awaits AsyncClient.get through the provider, then AsyncSession.commit in tracker.record.
    async def fetch_quote(self, provider: str, symbol: str, persist: bool = True) -> tuple[str, dict]:
        provider_key = provider.strip().lower()
        provider_upper = provider_key.upper()
        symbol = symbol.strip().upper()
        record_query(QUOTE_SERVICE)

        def fail(payload: dict) -> tuple[str, dict]:
            record_error(QUOTE_SERVICE, payload)
            return "error", payload

        engine = self.providers.get(provider_key)
        if engine is None:
            return fail({
                "provider": provider_upper,
                "symbol": symbol,
                "error": f"Unknown provider: {provider_key}",
            })

        async with engine.semaphore:
            ok, reason = self.tracker.can_request(provider_key)
            if not ok:
                return fail({
                    "provider": provider_upper,
                    "symbol": symbol,
                    "error": reason or "request_denied",
                    "status": "quota_exceeded",
                })

            try:
                raw = await engine.fetch_data(symbol)
                provider_error = engine.provider_error(raw)
                if provider_error is not None:
                    await self.tracker.record(provider_key, symbol)
                    return fail({
                        "provider": provider_upper,
                        "symbol": symbol,
                        "error": str(provider_error),
                        "status": provider_error.status,
                    })

                normalized = engine.normalize_data(raw, symbol)
                if persist:
                    self.persist_quote(normalized, fallback_symbol=symbol)
                await self.tracker.record(provider_key, symbol)
                record_success(QUOTE_SERVICE, normalized)
                return "quote", normalized
            except httpx2.TimeoutException:
                logging.error("Provider %s timed out for %s", provider_key, symbol)
                return fail({
                    "provider": provider_upper,
                    "symbol": symbol,
                    "error": "timeout",
                    "status": "timeout",
                })
            except httpx2.HTTPStatusError as exc:
                await self.tracker.record(provider_key, symbol)
                code = exc.response.status_code
                logging.error("Provider %s HTTP %s for %s", provider_key, code, symbol)
                return fail({
                    "provider": provider_upper,
                    "symbol": symbol,
                    "error": str(exc),
                    "status": "rate_limited" if code == 429 else "error",
                })
            except httpx2.RequestError as exc:
                logging.error("Provider %s network error for %s: %s", provider_key, symbol, exc)
                return fail({
                    "provider": provider_upper,
                    "symbol": symbol,
                    "error": str(exc),
                    "status": "network_error",
                })
            except Exception as exc:
                logging.exception("Provider %s unexpected error for %s", provider_key, symbol)
                return fail({
                    "provider": provider_upper,
                    "symbol": symbol,
                    "error": str(exc),
                    "status": "error",
                })
