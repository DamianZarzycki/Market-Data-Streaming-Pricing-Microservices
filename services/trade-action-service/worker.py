from datetime import datetime, timezone
import queue
import uuid
import logging
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import ActionType
from shared.trading_shared.models import Instrument, Trade, Valuation

trade_queue = queue.Queue()


def trade_action_worker():
    while True:
        try:
            payload = trade_queue.get()
            logging.info(
                f"payload:::: {payload}",
            )
            trade_action_handler(payload)
        except Exception as e:
            logging.error(f"Error: {e}")


def trade_action_handler(data):
    with DBSessionManager() as db:
        try:
            logging.info(f"Received data from queue: {data}")
            action_type = data.get("action_type")
            client_request_id = data.get("client_request_id")

            if action_type == ActionType.OPEN_TRADE.value:
                # IDEMPOTENCY check
                if client_request_id:
                    existing = db.trades.get_trades(client_request_id, None, None, True)
                    if existing:
                        logging.warning(
                            f"Duplicate OPEN_TRADE ignored: client_request_id {client_request_id} "
                            f"already maps to trade {existing.trade_id}"
                        )
                        return

                book_id = data.get("book_id")
                asset_class = data.get("asset_class")
                symbol = data.get("symbol")

                instrument = db.query(Instrument).filter_by(symbol=symbol).first()
                if not instrument:
                    logging.info(f"Instrument not found: {symbol}. Creating new instrument.")
                    instrument = Instrument(
                        symbol=symbol, asset_class=asset_class, multiplier=1
                    )
                    db.add(instrument)
                    db.flush()

                new_trade = Trade(
                    client_request_id=client_request_id,
                    book_id=book_id,
                    asset_class=asset_class,
                    symbol=symbol,
                    instrument_id=instrument.instrument_id,
                    side=data.get("side"),
                    quantity=data.get("quantity"),
                    trade_price=data.get("trade_price"),
                    trade_currency=data.get("currency"),
                    trade_date=datetime.now(timezone.utc),
                    status="ACTIVE",
                    source="GENERATED",
                )
                db.add(new_trade)
                logging.info(
                    f"Successfully opened trade. (Client ID: {client_request_id})"
                )

            elif action_type == ActionType.CLOSE_TRADE.value:
                trade_id_to_close = data.get("trade_id")
                logging.info(f"Attempting to close trade with ID: {trade_id_to_close}")

                # IDEMPOTENCY check
                if client_request_id:
                    duplicate = (
                        db.query(Trade)
                        .filter(
                            Trade.trade_id == trade_id_to_close,
                            Trade.metadata_payload["close_client_request_id"].as_string() == client_request_id,
                        )
                        .first()
                    )
                    if duplicate:
                        logging.warning(
                            f"Duplicate CLOSE_TRADE ignored: client_request_id {client_request_id} "
                            f"already closed trade {trade_id_to_close}"
                        )
                        return

                trade_to_close = (
                    db.query(Trade)
                    .filter_by(trade_id=trade_id_to_close)
                    # Preventing race conditions by locking
                    # the selected trade row until the transaction is complete
                    .with_for_update()
                    .first()
                )

                if not trade_to_close:
                    logging.error(
                        f"Trade not found in database with ID: {trade_id_to_close}"
                    )
                    return

                if trade_to_close.status != "ACTIVE":
                    logging.warning(
                        f"Trade {trade_id_to_close} is already closed or inactive."
                    )
                    return

                trade_to_close.status = "CLOSED"
                trade_to_close.close_price = data.get("close_price")
                trade_to_close.close_reason = data.get("close_reason")
                trade_to_close.closed_at = datetime.now(timezone.utc)


                close_price = float(data.get("close_price"))
                trade_price = float(trade_to_close.trade_price)
                quantity = float(trade_to_close.quantity)
                
                if trade_to_close.side == "BUY":
                    realized_pnl = round((close_price - trade_price) * quantity, 4)
                else:
                    realized_pnl = round((trade_price - close_price) * quantity, 4)

                closing_valuation = Valuation(
                    valuation_id=uuid.uuid4(),
                    trade_id=trade_to_close.trade_id,
                    book_id=trade_to_close.book_id,
                    asset_class=trade_to_close.asset_class,
                    valuation_time=datetime.now(timezone.utc),
                    fair_value=close_price * quantity,
                    market_value=close_price * quantity,
                    unrealized_pnl=0,
                    realized_pnl=realized_pnl,
                    total_pnl=realized_pnl,
                    currency=trade_to_close.trade_currency,
                    market_data_reference=f"CLOSE:{trade_to_close.symbol}@{datetime.now(timezone.utc).isoformat()}",
                    valuation_payload={
                        "close_price": close_price,
                        "trade_price": trade_price,
                        "quantity": quantity,
                        "close_reason": data.get("close_reason"),
                    },
                )
                db.valuations.add(closing_valuation)

                logging.info(f"Successfully closed trade: {trade_id_to_close}")

            db.commit()

        except Exception as e:
            db.rollback()
            logging.error(f"WENT WRONG: {e}")
        finally:
            db.close()
