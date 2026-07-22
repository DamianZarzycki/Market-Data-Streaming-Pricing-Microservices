from datetime import datetime, timezone
import queue
import uuid
import logging
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import ActionType, AssetClass, EntityType, EventType, OptionRightType, Severity, TradeSide
from shared.trading_shared.models import Instrument, Trade, Valuation

trade_queue = queue.Queue()
audit_logger = AuditLogger("trade-action-service")


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
            audit_logger.info(
                EventType.DB_CREATE,
                f"Processing trade action: {action_type} for client_request_id: {client_request_id}",
                entity_type=EntityType.TRADE.value,
                correlation_id=client_request_id,
                payload={"action_type": action_type, "client_request_id": client_request_id},
            )
            metadata_payload = None
            if action_type == ActionType.OPEN_TRADE.value:
                # IDEMPOTENCY check
                if client_request_id:
                    existing = db.trades.get_trades(
                        client_request_id=client_request_id, first_only=True
                    )
                    if existing:
                        logging.warning(
                            f"Duplicate OPEN_TRADE ignored: client_request_id {client_request_id} "
                            f"already maps to trade {existing.trade_id}"
                        )
                        audit_logger.warning(
                            EventType.DB_REJECT,
                            f"Duplicate OPEN_TRADE ignored for client_request_id {client_request_id}",
                            entity_type=EntityType.TRADE.value,
                            correlation_id=client_request_id,
                        )
                        return

                book_id = data.get("book_id")
                asset_class = data.get("asset_class")
                symbol = data.get("symbol")
                logging.info(f"SYMBOL: {symbol}.")
                instrument = db.instruments.get_by_symbol(symbol)
                logging.info(f"INSTRUMENT: {instrument}.")
                logging.info(f"ASSET_CLASS: {asset_class}.")
                if not instrument:
                    logging.info(f"Instrument not found: {symbol}. Creating new instrument.")
                    instrument = Instrument(symbol=symbol, asset_class=asset_class, multiplier=1)
                    db.instruments.add(instrument)
                    db.flush()

                if asset_class == AssetClass.OPTION.value:
                    logging.info(f"OPTION: {data}.")

                    option_right_type = data.get("option_right_type")
                    option_type = data.get("option_type")
                    option_details = {
                        "spot": data.get("spot"),
                        "strike": data.get("strike"),
                        "volatility": data.get("volatility"),
                        "maturity_years": data.get("maturity_years"),
                        "option_right_type": option_right_type,
                        "option_type": option_type,
                        "option_price": data.get("option_price"),
                    }

                    metadata_payload = option_details
                    trade_price = data.get("option_price")
                elif asset_class == AssetClass.IRS.value:
                    logging.info(f"IRS: {data}.")

                    # IRS jest wyceniany krzywą stóp — parametry kontraktu trzymamy
                    # w metadata_payload, a nie jako spot/price danych rynkowych.
                    metadata_payload = {
                        "notional": data.get("notional"),
                        "fixed_rate": data.get("fixed_rate"),
                        "maturity_years": data.get("maturity_years"),
                        "payments_per_year": data.get("payments_per_year", 1),
                        "direction": data.get("direction"),
                    }
                    # IRS wchodzi na rynek z PV ~ 0.
                    trade_price = data.get("trade_price")
                else:
                    trade_price = data.get("trade_price")

                new_trade = Trade(
                    client_request_id=client_request_id,
                    book_id=book_id,
                    asset_class=asset_class,
                    symbol=symbol,
                    instrument_id=instrument.instrument_id,
                    side=data.get("side"),
                    quantity=data.get("quantity"),
                    trade_price=trade_price,
                    trade_currency=data.get("currency"),
                    trade_date=datetime.now(timezone.utc),
                    status="ACTIVE",
                    source="GENERATED",
                    metadata_payload=metadata_payload if metadata_payload else None,
                )
                logging.info(f"Creating new trade: {new_trade}")
                db.trades.add(new_trade)
                logging.info(f"Successfully opened trade. (Client ID: {client_request_id})")

            elif action_type == ActionType.CLOSE_TRADE.value:
                trade_id_to_close = data.get("trade_id")
                logging.info(f"Attempting to close trade with ID: {trade_id_to_close}")

                # IDEMPOTENCY check
                if client_request_id:
                    trade = db.trades.get_by_id(trade_id_to_close)
                    if trade and trade.metadata_payload["close_client_request_id"].as_string() == client_request_id:
                        logging.warning(
                            f"Duplicate CLOSE_TRADE ignored: client_request_id {client_request_id} "
                            f"already closed trade {trade_id_to_close}"
                        )
                        audit_logger.warning(
                            EventType.DB_REJECT,
                            f"Duplicate CLOSE_TRADE ignored for client_request_id {client_request_id}",

                audit_logger.info(
                        EventType.DB_CLOSE,
                        f"Closing trade: {trade_id_to_close}",
                        entity_type=EntityType.TRADE.value,
                        entity_id=str(trade_id_to_close),
                        correlation_id=client_request_id,
                )
                trade_to_close = (
                    db.trades.get_trades(trade_id=trade_id_to_close, first_only=True)
                    .filter_by(trade_id=trade_id_to_close)
                    # Preventing race conditions by locking
                    # the selected trade row until the transaction is complete
                    .with_for_update()
                    .first()
                )

                if not trade_to_close:
                    logging.error(f"Trade not found in database with ID: {trade_id_to_close}")
                    audit_logger.warning(
                        EventType.DB_ERROR,
                        f"CLOSE_TRADE failed: trade {trade_id_to_close} not found",
                        entity_type=EntityType.TRADE.value,
                        entity_id=str(trade_id_to_close),
                        correlation_id=client_request_id,
                    )
                    return

                if trade_to_close.status != "ACTIVE":
                    logging.warning(f"Trade {trade_id_to_close} is already closed or inactive.")
                    audit_logger.warning(
                        EventType.DB_REJECT,
                        f"CLOSE_TRADE rejected: trade {trade_id_to_close} is not ACTIVE",
                        entity_type=EntityType.TRADE.value,
                        entity_id=str(trade_id_to_close),
                        correlation_id=client_request_id,
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
                logging.info(f"Creating closing valuation: {closing_valuation}")
                db.valuations.add(closing_valuation)
                # TODO Cache clearing clear cache of trade for trade close
                logging.info(f"Successfully closed trade: {trade_id_to_close}")

            db.commit()

            if action_type == ActionType.OPEN_TRADE.value:
                audit_logger.info(
                    EventType.DB_CREATE,
                    f"Trade opened: {new_trade.trade_id}",
                    entity_type=EntityType.TRADE.value,
                    entity_id=str(new_trade.trade_id),
                    correlation_id=client_request_id,
                    payload={"book_id": book_id, "symbol": symbol, "asset_class": asset_class},
                )
            elif action_type == ActionType.CLOSE_TRADE.value:
                audit_logger.info(
                    EventType.DB_CLOSE,
                    f"Trade closed: {trade_id_to_close}",
                    entity_type=EntityType.TRADE.value,
                    entity_id=str(trade_id_to_close),
                    correlation_id=client_request_id,
                    payload={"close_price": data.get("close_price"), "realized_pnl": realized_pnl},
                )

        except Exception as e:
            db.rollback()
            logging.error(f"WENT WRONG: {e}")
            audit_logger.error(
                EventType.DB_ERROR,
                f"Trade action failed: {e}",
                correlation_id=data.get("client_request_id"),
                payload={"action_type": data.get("action_type"), "error": str(e)},
            )

        finally:
            db.close()
