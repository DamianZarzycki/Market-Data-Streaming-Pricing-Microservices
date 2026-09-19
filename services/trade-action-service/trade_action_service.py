import logging
import os
import queue as queue_module
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import ActionType, AssetClass, EventType, ServiceStatus
import metrics
import worker

VALID_ACTION_TYPES = {ActionType.OPEN_TRADE.value, ActionType.CLOSE_TRADE.value}
# Fields that must be present on an OPEN_TRADE request so we know which quote
# to check for freshness and what reference price/source to persist on the trade.
REQUIRED_OPEN_TRADE_FIELDS = (
    "market_data_provider",
    "reference_price",
    "reference_price_timestamp",
    "source",
)
# How old a provider quote is allowed to be before it's considered stale, for
# asset classes with no more specific entry below. Continuously-quoted
# instruments (listed equities, FX) need a tight window; something like a
# thinly-traded sovereign/corporate bond may only tick a few times a day, so
# holding it to the same bar would reject every trade as "stale".
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
# A quote timestamped further in the future than this (server clock skew
# between us and the provider) is treated as bad data rather than "fresh".
MAX_CLOCK_SKEW_SECONDS = int(os.getenv("TRADE_ACTION_MAX_CLOCK_SKEW_SECONDS", "5"))
audit_logger = AuditLogger("trade-action-service")


def get_health():
    return {
        "service": "trade-action-service",
        "status": ServiceStatus.UP.value,
    }


def get_status():
    return metrics.build_status(worker.trade_queue)

def _quote_freshness_error(item):
    """For OPEN_TRADE requests, ensure the selected provider has a current
    quote before we let the trade proceed. Returns an error string, or None
    if the check passes (or doesn't apply)."""
    if item.get("action_type") != ActionType.OPEN_TRADE.value:
        return None

    provider = item.get("market_data_provider")
    symbol = item.get("symbol")
    result = check_provider_data_staleness(provider, symbol)
    return result.detail if result.stale else None

def validate_trade_action(item):
    action_type = item.get("action_type")
    if not action_type or action_type not in VALID_ACTION_TYPES:
        return f"Missing or invalid action_type ('{action_type}')"
    if not item.get("client_request_id"):
        return "Missing client_request_id"

    if action_type == ActionType.OPEN_TRADE.value:
        for field in REQUIRED_OPEN_TRADE_FIELDS:
            if item.get(field) in (None, ""):
                return f"Missing {field}"

    if action_type != ActionType.OPEN_TRADE.value:
        return None

    provider = item.get("market_data_provider")
    symbol = item.get("symbol")

    result = check_provider_data_staleness(provider, symbol)
    return result.detail if result.stale else None


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
    reason: QuoteStaleness
    detail: str
    # When set, these answer "when was data for this provider/symbol last
    # fetched, and how does that compare to what its asset class allows" -
    # derived from the persisted quote row, so it reads the same regardless
    # of how many times this service (or the fetcher) has been restarted
    # since that fetch happened.
    last_fetched_at: datetime = None
    age_seconds: float = None
    max_age_seconds: int = None
    asset_class: str = None

    @property
    def stale(self):
        return self.reason != QuoteStaleness.FRESH


def _to_utc(dt):
    """Coerce a timestamp to timezone-aware UTC. A naive timestamp is assumed
    to already be UTC (the convention ticks/quotes are written under), since
    there's no reliable way to recover a missing timezone after the fact."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def max_quote_age_seconds(asset_class):
    """Allowed quote age for a given asset class, e.g. a Nigerian sovereign
    bond that only ticks a couple of times a day gets a much longer window
    than a continuously-quoted name like AAPL."""
    return ASSET_CLASS_MAX_QUOTE_AGE_SECONDS.get(asset_class, DEFAULT_MAX_QUOTE_AGE_SECONDS)


def check_provider_data_staleness(provider_name, symbol):
    """Check whether the latest quote on file for this provider/symbol is
    usable, applying a freshness window sized to the quote's own asset class
    rather than one fixed cutoff for every instrument."""
    if not provider_name or not symbol:
        return StalenessResult(QuoteStaleness.MISSING_INPUT, "No provider or symbol specified")

    with DBSessionManager() as db:
        quote = db.provider_quotes.get_latest(provider_name, symbol)

    if not quote:
        return StalenessResult(
            QuoteStaleness.NO_QUOTE,
            f"No quote on file from provider '{provider_name}' for symbol '{symbol}'",
        )

    quote_time = quote.provider_timestamp or quote.received_at
    if not quote_time:
        return StalenessResult(
            QuoteStaleness.MISSING_TIMESTAMP,
            f"Quote from provider '{provider_name}' for symbol '{symbol}' has no timestamp",
        )

    quote_time = _to_utc(quote_time)
    age_seconds = (datetime.now(timezone.utc) - quote_time).total_seconds()
    allowed_age = max_quote_age_seconds(quote.asset_class)

    # Provider or server clock skew can make a tick look like it's from the
    # future; a stale check should reject that rather than call it fresh.
    if age_seconds < -MAX_CLOCK_SKEW_SECONDS:
        return StalenessResult(
            QuoteStaleness.CLOCK_SKEW,
            f"Quote from provider '{provider_name}' for symbol '{symbol}' is timestamped "
            f"{-age_seconds:.0f}s in the future, rejecting rather than trusting it",
            last_fetched_at=quote_time,
            age_seconds=age_seconds,
            max_age_seconds=allowed_age,
            asset_class=quote.asset_class,
        )

    if age_seconds > allowed_age:
        return StalenessResult(
            QuoteStaleness.STALE,
            f"Quote from provider '{provider_name}' for symbol '{symbol}' is {age_seconds:.0f}s "
            f"old, exceeding the {allowed_age}s limit for asset class '{quote.asset_class}'",
            last_fetched_at=quote_time,
            age_seconds=age_seconds,
            max_age_seconds=allowed_age,
            asset_class=quote.asset_class,
        )

    return StalenessResult(
        QuoteStaleness.FRESH,
        "",
        last_fetched_at=quote_time,
        age_seconds=age_seconds,
        max_age_seconds=allowed_age,
        asset_class=quote.asset_class,
    )





def submit_trade_action(data):
    error = validate_trade_action(data) or _quote_freshness_error(data)
    if error:
        logging.warning(f"Rejected request: {error}")
        audit_logger.warning(
            EventType.DB_REJECT,
            f"Trade action rejected: {error}",
            payload={
                "action_type": data.get("action_type"),
                "client_request_id": data.get("client_request_id"),
            },
        )
        metrics.record_rejected(
            data.get("action_type"),
            data.get("client_request_id"),
            data.get("symbol"),
            error,
        )
        return {"error": error}, 400

    client_request_id = data["client_request_id"]
    payload = dict(data)
    payload["_enqueued_at"] = time.time()
    try:
        worker.trade_queue.put(payload, block=False)
        logging.info(f"Enqueued request {client_request_id}.")
    except queue_module.Full:
        logging.error(f"Queue full. Rejected request: {client_request_id}")
        audit_logger.error(
            EventType.DB_ERROR,
            f"Trade action queue full. Rejected: {client_request_id}",
            correlation_id=client_request_id,
        )
        metrics.record_overload(
            data.get("action_type"),
            client_request_id,
            data.get("symbol"),
        )
        return {"error": "System overloaded, please try again later"}, 503

    metrics.record_accepted(
        data.get("action_type"),
        client_request_id,
        data.get("symbol"),
    )
    return {
        "message": "Trade action accepted for processing",
        "client_request_id": client_request_id,
    }, 202


def submit_trade_action_batch(items):
    accepted_count = 0
    errors = []

    for index, item in enumerate(items):
        error = validate_trade_action(item) or _quote_freshness_error(item)
        if error:
            errors.append(f"Item {index}: {error}")
            metrics.record_rejected(
                item.get("action_type"),
                item.get("client_request_id"),
                item.get("symbol"),
                error,
            )
            continue

        payload = dict(item)
        payload["_enqueued_at"] = time.time()
        try:
            worker.trade_queue.put(payload, block=False)
            accepted_count += 1
            metrics.record_accepted(
                item.get("action_type"),
                item.get("client_request_id"),
                item.get("symbol"),
            )
        except queue_module.Full:
            logging.error("Queue full during batch processing.")
            audit_logger.error(
                EventType.DB_ERROR,
                "Trade action queue full during batch processing",
                correlation_id=item.get("client_request_id"),
            )
            metrics.record_overload(
                item.get("action_type"),
                item.get("client_request_id"),
                item.get("symbol"),
            )
            return {
                "error": "System overloaded, queue is full",
                "accepted_so_far": accepted_count,
            }, 503

    logging.info(f"Batch processed. Accepted: {accepted_count}, Errors: {len(errors)}")
    return {
        "message": "Batch processed",
        "accepted_count": accepted_count,
        "errors": errors,
    }, 202
