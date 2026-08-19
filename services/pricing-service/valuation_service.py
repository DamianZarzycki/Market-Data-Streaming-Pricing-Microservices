import uuid
from datetime import datetime, timezone

from shared.trading_shared.models import Valuation


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
        valuation_payload=valuation_data,
    )
