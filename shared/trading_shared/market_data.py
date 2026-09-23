"""Shared market-data quote freshness helpers.

Used by trade-action-service (gating trade open/close on a fresh provider
quote) and pricing-service (flagging a mark as stale on the valuation stream).
Keeping the age windows in one place means both services agree on what
"stale" means for a given asset class.
"""

import os
from datetime import datetime, timezone

from shared.trading_shared.enums import AssetClass

# How old a provider quote is allowed to be before it's considered stale, for
# asset classes with no more specific entry below. Continuously-quoted
# instruments (listed equities, FX) need a tight window; something like a
# thinly-traded sovereign/corporate bond may only tick a few times a day.
DEFAULT_MAX_QUOTE_AGE_SECONDS = int(os.getenv("TRADE_ACTION_MAX_QUOTE_AGE_SECONDS", "120"))
ASSET_CLASS_MAX_QUOTE_AGE_SECONDS = {
    AssetClass.EQUITY.value: int(os.getenv("TRADE_ACTION_MAX_QUOTE_AGE_EQUITY_SECONDS", "120")),
    AssetClass.FX.value: int(os.getenv("TRADE_ACTION_MAX_QUOTE_AGE_FX_SECONDS", "60")),
    AssetClass.FUTURES.value: int(os.getenv("TRADE_ACTION_MAX_QUOTE_AGE_FUTURES_SECONDS", "120")),
    AssetClass.OPTION.value: int(os.getenv("TRADE_ACTION_MAX_QUOTE_AGE_OPTION_SECONDS", "120")),
    AssetClass.COMMODITY.value: int(os.getenv("TRADE_ACTION_MAX_QUOTE_AGE_COMMODITY_SECONDS", "300")),
    AssetClass.BOND.value: int(os.getenv("TRADE_ACTION_MAX_QUOTE_AGE_BOND_SECONDS", "86400")),
    AssetClass.IRS.value: int(os.getenv("TRADE_ACTION_MAX_QUOTE_AGE_IRS_SECONDS", "86400")),
    AssetClass.BENCHMARK.value: int(os.getenv("TRADE_ACTION_MAX_QUOTE_AGE_BENCHMARK_SECONDS", "86400")),
}


def max_quote_age_seconds(asset_class):
    """Allowed quote age for a given asset class, e.g. a Nigerian sovereign
    bond that only ticks a couple of times a day gets a much longer window
    than a continuously-quoted name like AAPL."""
    return ASSET_CLASS_MAX_QUOTE_AGE_SECONDS.get(asset_class, DEFAULT_MAX_QUOTE_AGE_SECONDS)


def _coerce_utc(quote_time):
    """Coerce a datetime or ISO-8601 string to timezone-aware UTC, or None."""
    if quote_time is None:
        return None
    if isinstance(quote_time, str):
        try:
            quote_time = datetime.fromisoformat(quote_time.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    if quote_time.tzinfo is None:
        return quote_time.replace(tzinfo=timezone.utc)
    return quote_time.astimezone(timezone.utc)


def quote_age_seconds(quote_time, now=None):
    """Age in seconds of a quote timestamp relative to `now` (defaults to
    current UTC). Returns None if the timestamp is missing/unparseable."""
    coerced = _coerce_utc(quote_time)
    if coerced is None:
        return None
    now = now or datetime.now(timezone.utc)
    return (now - coerced).total_seconds()


def is_quote_stale(asset_class, quote_time, now=None):
    """Return (is_stale, age_seconds). A missing/unparseable timestamp is
    treated as stale with age None."""
    age = quote_age_seconds(quote_time, now=now)
    if age is None:
        return True, None
    return age > max_quote_age_seconds(asset_class), age
