import logging
import uuid
from datetime import datetime, timezone

from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.models import Valuation


def save_valuation(trade, valuation_data):
    with DBSessionManager() as db:
        try:
            valuation_time = datetime.now(timezone.utc)

            logging.info(f"market_data_reference={valuation_data['asset_class']}:{valuation_data['symbol']}@{valuation_time}")
            valuation = Valuation(
                valuation_id=uuid.uuid4(),
                trade_id=trade.trade_id,
                book_id=trade.book_id,
                asset_class=valuation_data["asset_class"],
                valuation_time=valuation_time,
                fair_value=valuation_data["fair_value"],
                market_value=valuation_data.get("market_value"),
                market_data_reference=f"{valuation_data['asset_class']}:{valuation_data['symbol']}@{valuation_time}",
                unrealized_pnl=valuation_data["unrealized_pnl"],
                realized_pnl=valuation_data["realized_pnl"],
                total_pnl=valuation_data["total_pnl"],
                currency=valuation_data["currency"],
                valuation_payload=valuation_data,
            )
            db.valuations.add(valuation)
            db.commit()
        except Exception as e:
            logging.error(f"Error saving valuation for trade {trade.trade_id}: {e}")
            db.rollback()