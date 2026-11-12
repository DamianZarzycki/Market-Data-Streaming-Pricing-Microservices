import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone

import httpx2
from fastapi import status

from shared.trading_shared.http_errors import ApiError
from core.audit import QUOTE_SERVICE, record_error, record_query, record_success
from core.polling import futures_quotes_for_equity
from core.runtime import provider_timeout_seconds
from providers.quota_providers.alpha_vantage_data_provider import AlphaVantageProvider
from providers.quota_providers.finnhub_data_provider import FinnhubProvider
from providers.quota_providers.twelve_data_provider import TwelveDataProvider
from shared.trading_shared.db import AsyncDBSessionManager
from shared.trading_shared.enums import AssetClass
from shared.trading_shared.models import MarketDataProviderRequest
from shared.trading_shared.serialization import serialize


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


class RequestTracker:
    """Simple per-provider quota tracker with rolling window counters."""

    def __init__(self, limits: dict[str, dict[str, int | None]] | None = None):
        self.limits = limits or {}
        self._lock = threading.Lock()
        self._counts: dict[str, dict[str, int]] = {}
        self._resets: dict[str, dict[str, datetime]] = {}
        for provider in self.limits:
            self._init_provider(provider)

    def _init_provider(self, provider: str) -> None:
        self._counts[provider] = {"minute": 0, "hourly": 0, "daily": 0, "monthly": 0}
        now = _utc_now()
        self._resets[provider] = {
            "minute": now.replace(second=0, microsecond=0) + timedelta(minutes=1),
            "hourly": now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1),
            "daily": (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0),
            "monthly": (now.replace(day=28) + timedelta(days=4)).replace(day=1, hour=0, minute=0, second=0, microsecond=0),
        }

    # Async: awaits provider_requests.list_since_async.
    @classmethod
    async def from_db(cls, limits: dict[str, dict[str, int | None]]) -> "RequestTracker":
        tracker = cls(limits)
        now = _utc_now()
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        async with AsyncDBSessionManager() as db:
            rows = await db.provider_requests.list_since_async(start_of_month)
        for row in rows:
            if row.provider not in tracker._counts:
                continue
            if row.requested_at >= now - timedelta(minutes=1):
                tracker._counts[row.provider]["minute"] += 1
            if row.requested_at >= now - timedelta(hours=1):
                tracker._counts[row.provider]["hourly"] += 1
            if row.requested_at >= now.replace(hour=0, minute=0, second=0, microsecond=0):
                tracker._counts[row.provider]["daily"] += 1
            tracker._counts[row.provider]["monthly"] += 1
        logging.info("Hydrated request tracker from DB: %d rows", len(rows))
        return tracker

    def can_request(self, provider: str) -> tuple[bool, str | None]:
        """Check quota. The counters live in memory, so this stays synchronous."""
        with self._lock:
            return self._can_request_locked(provider)

    def quota_snapshot(self, provider: str) -> dict | None:
        """Current used/limit counters plus whether another request is allowed."""
        with self._lock:
            if provider not in self.limits:
                return None
            allowed, reason = self._can_request_locked(provider)
            caps = self.limits[provider]
            counts = self._counts[provider]
            return {
                "allowed": allowed,
                "block_reason": reason,
                "usage": {
                    window: {"used": counts[window], "limit": caps.get(window)}
                    for window in ("minute", "hourly", "daily", "monthly")
                },
            }

    def _can_request_locked(self, provider: str) -> tuple[bool, str | None]:
        if provider not in self.limits:
            return False, "unknown_provider"
        self._expire_windows(provider)
        caps = self.limits[provider]
        counts = self._counts[provider]
        for window in ("minute", "hourly", "daily", "monthly"):
            cap = caps.get(window)
            if cap is not None and counts[window] >= cap:
                return False, f"{window}_limit"
        return True, None

    # Async: awaits AsyncSession.commit for the provider_requests insert.
    # The in-memory counters are updated first, and the lock is not held across the insert.
    async def record(self, provider: str, symbol: str | None = None) -> None:
        now = _utc_now()
        with self._lock:
            if provider not in self._counts:
                return
            self._expire_windows(provider)
            for window in self._counts[provider]:
                self._counts[provider][window] += 1
        try:
            async with AsyncDBSessionManager() as db:
                db.provider_requests.add(
                    MarketDataProviderRequest(provider=provider, symbol=symbol, requested_at=now)
                )
                await db.commit()
        except Exception as exc:
            logging.error("Failed to persist provider request %s: %s", provider, exc)

    def _expire_windows(self, provider: str) -> None:
        now = _utc_now()
        resets = self._resets[provider]
        counts = self._counts[provider]
        if now >= resets["minute"]:
            counts["minute"] = 0
            resets["minute"] = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
        if now >= resets["hourly"]:
            counts["hourly"] = 0
            resets["hourly"] = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        if now >= resets["daily"]:
            counts["daily"] = 0
            resets["daily"] = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        if now >= resets["monthly"]:
            counts["monthly"] = 0
            resets["monthly"] = (now.replace(day=28) + timedelta(days=4)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)


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
