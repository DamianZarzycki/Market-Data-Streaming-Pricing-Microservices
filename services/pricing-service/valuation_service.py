import logging
import uuid
from datetime import datetime, timezone

from shared.trading_shared.models import Valuation


def _parse_market_data_timestamp(value):
    if not value:
        return None
    # Already a datetime (e.g. passed straight from the DB): just ensure UTC.
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    # Otherwise it's a serialized ISO-8601 string. `serialize()` emits
    # `...+00:00` (isoformat), the trading-ticket UI emits `...Z`, and some
    # providers add microseconds — datetime.fromisoformat handles all of these
    # on Python 3.11+, we only normalise the trailing `Z` it doesn't accept.
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        logging.warning(f"Could not parse market_data_timestamp: {value}")
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _parse_valuation_time(value):
    """Use the mark time from the payload. Fall back to now only when it is missing."""
    parsed = _parse_market_data_timestamp(value)
    if parsed is None:
        return datetime.now(timezone.utc)
    return parsed


def _as_uuid(value):
    if isinstance(value, uuid.UUID):
        return value
    return uuid.UUID(str(value))


def to_valuation(valuation_data):
    valuation_time = _parse_valuation_time(valuation_data.get("valuation_time"))
    return Valuation(
        valuation_id=uuid.uuid4(),
        trade_id=_as_uuid(valuation_data["trade_id"]),
        book_id=_as_uuid(valuation_data["book_id"]),
        asset_class=valuation_data["asset_class"],
        valuation_time=valuation_time,
        fair_value=valuation_data["fair_value"],
        market_value=valuation_data.get("market_value"),
        market_data_reference=(
            f"{valuation_data['asset_class']}:{valuation_data['symbol']}@{valuation_time.isoformat()}"
        ),
        unrealized_pnl=valuation_data["unrealized_pnl"],
        realized_pnl=valuation_data["realized_pnl"],
        total_pnl=valuation_data["total_pnl"],
        currency=valuation_data["currency"],
        market_data_provider=valuation_data.get("market_data_provider"),
        market_data_timestamp=_parse_market_data_timestamp(
            valuation_data.get("market_data_timestamp")
        ),
        valuation_payload=valuation_data,
    )
