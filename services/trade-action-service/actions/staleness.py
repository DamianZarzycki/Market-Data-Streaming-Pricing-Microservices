from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from shared.trading_shared.db import AsyncDBSessionManager
from shared.trading_shared.env import env_int
from shared.trading_shared.market_data import max_quote_age_seconds

MAX_CLOCK_SKEW_SECONDS = env_int("TRADE_ACTION_MAX_CLOCK_SKEW_SECONDS", 5, minimum=0)


class QuoteStaleness(Enum):
    """Why a provider quote was (or wasn't) accepted as fresh."""

    FRESH = "fresh"
    MISSING_INPUT = "missing_input"
    NO_QUOTE = "no_quote"
    MISSING_TIMESTAMP = "missing_timestamp"
    CLOCK_SKEW = "clock_skew"
    STALE = "stale"


@dataclass(frozen=True)
class StalenessResult:
    provider: str | None = None
    symbol: str | None = None
    provider_timestamp_utc: datetime | None = None
    received_at_utc: datetime | None = None
    reason: QuoteStaleness = QuoteStaleness.MISSING_INPUT
    detail: str = ""
    last_fetched_at: datetime | None = None
    age_seconds: float | None = None
    max_age_seconds: int | None = None
    asset_class: str | None = None

    @property
    def stale(self) -> bool:
        return self.reason != QuoteStaleness.FRESH


def _to_utc(dt):
    """Coerce a timestamp to timezone-aware UTC.

    A naive timestamp is already UTC, which is how quotes are stored.
    None stays None so a null provider_timestamp is a rejection, not a 500.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


async def check_provider_data_staleness(provider_name, symbol) -> StalenessResult:
    """Whether the latest quote on file for this provider and symbol is usable.

    The freshness window comes from the quote's own asset class. Age is
    measured from `received_at`, not `provider_timestamp`.
    """
    if not provider_name or not symbol:
        return StalenessResult(
            reason=QuoteStaleness.MISSING_INPUT,
            detail="No provider or symbol specified",
        )

    async with AsyncDBSessionManager() as db:
        quote = await db.provider_quotes.get_latest_freshness_async(provider_name, symbol)
        if quote is None:
            return StalenessResult(
                reason=QuoteStaleness.NO_QUOTE,
                detail=f"No quote on file from provider '{provider_name}' for symbol '{symbol}'",
            )
        # Copy the three columns out before the session closes.
        asset_class = quote.asset_class
        provider_timestamp = quote.provider_timestamp
        received_at = quote.received_at

    provider_timestamp_utc = _to_utc(provider_timestamp)
    if not provider_timestamp_utc:
        return StalenessResult(
            reason=QuoteStaleness.MISSING_TIMESTAMP,
            detail=f"Quote from provider '{provider_name}' for symbol '{symbol}' has no timestamp",
        )

    received_at_utc = _to_utc(received_at)
    if not received_at_utc:
        return StalenessResult(
            reason=QuoteStaleness.MISSING_TIMESTAMP,
            detail=(
                f"Quote from provider '{provider_name}' for symbol '{symbol}' "
                "has no received_at timestamp"
            ),
        )

    age_seconds = (datetime.now(timezone.utc) - received_at_utc).total_seconds()
    allowed_age = max_quote_age_seconds(asset_class)

    if age_seconds < -MAX_CLOCK_SKEW_SECONDS:
        return StalenessResult(
            reason=QuoteStaleness.CLOCK_SKEW,
            detail=(
                f"Quote from provider '{provider_name}' for symbol '{symbol}' is timestamped "
                f"{-age_seconds:.0f}s in the future, rejecting rather than trusting it"
            ),
            provider=provider_name,
            symbol=symbol,
            provider_timestamp_utc=provider_timestamp_utc,
            last_fetched_at=received_at_utc,
            age_seconds=age_seconds,
            max_age_seconds=allowed_age,
            asset_class=asset_class,
        )

    if age_seconds > allowed_age:
        return StalenessResult(
            reason=QuoteStaleness.STALE,
            detail=(
                f"Quote from provider '{provider_name}' for symbol '{symbol}' is {age_seconds:.0f}s "
                f"old, exceeding the {allowed_age}s limit for asset class '{asset_class}'"
            ),
            provider=provider_name,
            symbol=symbol,
            provider_timestamp_utc=provider_timestamp_utc,
            last_fetched_at=received_at_utc,
            age_seconds=age_seconds,
            max_age_seconds=allowed_age,
            asset_class=asset_class,
        )

    return StalenessResult(
        reason=QuoteStaleness.FRESH,
        provider=provider_name,
        symbol=symbol,
        provider_timestamp_utc=provider_timestamp_utc,
        last_fetched_at=received_at_utc,
        age_seconds=age_seconds,
        max_age_seconds=allowed_age,
        asset_class=asset_class,
    )
