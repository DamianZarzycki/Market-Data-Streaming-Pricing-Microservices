import logging
import os
import queue as queue_module
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import ActionType, EventType, ServiceStatus
from shared.trading_shared.market_data import max_quote_age_seconds
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
# Per-asset-class quote-age windows live in shared.trading_shared.market_data
# so pricing-service uses the exact same "stale" definition when it flags a
# mark on the valuation stream.
# A quote timestamped further in the future than this (server clock skew
# between us and the provider) is treated as bad data rather than "fresh".
MAX_CLOCK_SKEW_SECONDS = int(os.getenv("TRADE_ACTION_MAX_CLOCK_SKEW_SECONDS", "5"))
audit_logger = AuditLogger("trade-action-service")
_provider_mode_lock = threading.Lock()


def _env_use_provider_data():
    raw = os.getenv("USE_PROVIDER_DATA")
    if raw is None or not raw.strip():
        return False
    normalized = raw.strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    logging.warning(
        "Invalid USE_PROVIDER_DATA=%r, falling back to false",
        raw,
    )
    return False


_use_provider_data = _env_use_provider_data()


def use_provider_data():
    with _provider_mode_lock:
        return _use_provider_data


def set_use_provider_data(enabled):
    global _use_provider_data
    if not isinstance(enabled, bool):
        raise ValueError("use_provider_data must be a boolean")
    with _provider_mode_lock:
        _use_provider_data = enabled
        return _use_provider_data


def get_health():
    return {
        "service": "trade-action-service",
        "status": ServiceStatus.UP.value,
    }


def get_status():
    return metrics.build_status(worker.trade_queue)

def validate_trade_action(item):
    action_type = item.get("action_type")
    if not action_type or action_type not in VALID_ACTION_TYPES:
        return f"Missing or invalid action_type ('{action_type}')"
    if not item.get("client_request_id"):
        return "Missing client_request_id"

    provider = item.get("market_data_provider")
    symbol = item.get("symbol")
    require_provider = use_provider_data()

    # USE_PROVIDER_DATA on: an OPEN must name the quote it was booked
    # against. Off: the simulator generator omits those fields and is
    # accepted. A caller that still sends a provider (the trading ticket)
    # keeps the freshness check either way.
    if require_provider and action_type == ActionType.OPEN_TRADE.value:
        for field in REQUIRED_OPEN_TRADE_FIELDS:
            if item.get(field) in (None, ""):
                return f"Missing {field}"

    if provider or (require_provider and action_type == ActionType.OPEN_TRADE.value):
        result = check_provider_data_staleness(provider, symbol)
        if result.reason == QuoteStaleness.STALE:
            return None
        return result.detail if result.stale else None

    return None


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
    # When set, these answer "when was data for this provider/symbol last
    # fetched, and how does that compare to what its asset class allows" -
    # derived from the persisted quote row, so it reads the same regardless
    # of how many times this service (or the fetcher) has been restarted
    # since that fetch happened.
    last_fetched_at: datetime | None = None
    age_seconds: float | None = None
    max_age_seconds: int | None = None
    asset_class: str | None = None

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


def check_provider_data_staleness(provider_name, symbol):
    """Check whether the latest quote on file for this provider/symbol is
    usable, applying a freshness window sized to the quote's own asset class
    rather than one fixed cutoff for every instrument."""
    if not provider_name or not symbol:
        return StalenessResult(
            reason=QuoteStaleness.MISSING_INPUT,
            detail="No provider or symbol specified",
        )

    with DBSessionManager() as db:
        quote = db.provider_quotes.get_latest(provider_name, symbol)

    if not quote:
        return StalenessResult(
            reason=QuoteStaleness.NO_QUOTE,
            detail=f"No quote on file from provider '{provider_name}' for symbol '{symbol}'",
        )

    provider_timestamp_utc = _to_utc(quote.provider_timestamp)
    if not provider_timestamp_utc:
        return StalenessResult(
            reason=QuoteStaleness.MISSING_TIMESTAMP,
            detail=f"Quote from provider '{provider_name}' for symbol '{symbol}' has no timestamp",
        )

    received_at_utc = _to_utc(quote.received_at)
    if not received_at_utc:
        return StalenessResult(
            reason=QuoteStaleness.MISSING_TIMESTAMP,
            detail=(
                f"Quote from provider '{provider_name}' for symbol '{symbol}' "
                "has no received_at timestamp"
            ),
        )

    age_seconds = (datetime.now(timezone.utc) - received_at_utc).total_seconds()
    allowed_age = max_quote_age_seconds(quote.asset_class)

    # Provider or server clock skew can make a tick look like it's from the
    # future; a stale check should reject that rather than call it fresh.
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
            asset_class=quote.asset_class,
        )

    if age_seconds > allowed_age:
        return StalenessResult(
            reason=QuoteStaleness.STALE,
            detail=(
                f"Quote from provider '{provider_name}' for symbol '{symbol}' is {age_seconds:.0f}s "
                f"old, exceeding the {allowed_age}s limit for asset class '{quote.asset_class}'"
            ),
            provider=provider_name,
            symbol=symbol,
            provider_timestamp_utc=provider_timestamp_utc,
            last_fetched_at=received_at_utc,
            age_seconds=age_seconds,
            max_age_seconds=allowed_age,
            asset_class=quote.asset_class,
        )

    return StalenessResult(
        reason=QuoteStaleness.FRESH,
        provider=provider_name,
        symbol=symbol,
        provider_timestamp_utc=provider_timestamp_utc,
        last_fetched_at=received_at_utc,
        age_seconds=age_seconds,
        max_age_seconds=allowed_age,
        asset_class=quote.asset_class,
    )





def submit_trade_action(data):
    error = validate_trade_action(data)
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
        error = validate_trade_action(item)
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
