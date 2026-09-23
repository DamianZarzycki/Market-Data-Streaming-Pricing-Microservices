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


def to_valuation(valuation_data):
    valuation_time = datetime.now(timezone.utc)
    return Valuation(
        valuation_id=uuid.uuid4(),
        trade_id=valuation_data["trade_id"],
        book_id=valuation_data["book_id"],
        asset_class=valuation_data["asset_class"],
        valuation_time=valuation_time,
        fair_value=valuation_data["fair_value"],
        market_value=valuation_data.get("market_value"),
        market_data_reference=(
            f"{valuation_data['asset_class']}:{valuation_data['symbol']}@{valuation_time}"
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
