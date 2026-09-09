import logging
import threading
from datetime import datetime, timedelta, timezone

from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.models import MarketDataProviderRequest


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

    @classmethod
    def from_db(cls, limits: dict[str, dict[str, int | None]]) -> "RequestTracker":
        tracker = cls(limits)
        now = _utc_now()
        start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        with DBSessionManager() as db:
            rows = db.provider_requests.list_since(start_of_month)
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
        """Check quota. Returns (allowed, block_reason)."""
        with self._lock:
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

    def record(self, provider: str, symbol: str | None = None) -> None:
        """Count the request and persist it to DB."""
        now = _utc_now()
        with self._lock:
            if provider not in self._counts:
                return
            self._expire_windows(provider)
            for window in self._counts[provider]:
                self._counts[provider][window] += 1
        try:
            with DBSessionManager() as db:
                db.provider_requests.add(
                    MarketDataProviderRequest(provider=provider, symbol=symbol, requested_at=now)
                )
                db.commit()
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
