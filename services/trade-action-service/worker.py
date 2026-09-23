from datetime import datetime, timezone
import queue
import uuid
import logging
import metrics
from sqlalchemy.dialects.postgresql import insert as pg_insert
from shared.trading_shared.audit import AuditLogger
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import (
    ActionType,
    AssetClass,
    EntityType,
    EventType,
    OptionRightType,
    TradeSide,
    TradeSource,
)
from shared.trading_shared.models import Instrument, Trade, Valuation

trade_queue = queue.Queue(maxsize=metrics.QUEUE_MAXSIZE)
audit_logger = AuditLogger("trade-action-service")


def _normalize_trade_source(source_value):
    """Validate the caller-supplied `source` against the known values
    (TradeSource.TRADING_TICKET / TradeSource.GENERATED). Falls back to
    GENERATED to preserve the pre-existing default behavior."""
    normalized = (source_value or "").strip().upper()
    if normalized in (TradeSource.TRADING_TICKET.value, TradeSource.GENERATED.value):
        return normalized
    return TradeSource.GENERATED.value


def _as_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _reject_open_trade(db, metrics_note, action_type, client_request_id, symbol, enqueued_at):
    logging.error(
        f"OPEN_TRADE rejected: {metrics_note} for client_request_id {client_request_id}"
    )
    db.commit()
    metrics.record_error(
        action_type,
        client_request_id,
        symbol,
        metrics_note,
        enqueued_at,
    )


def _parse_reference_timestamp(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        logging.warning(f"Could not parse reference_price_timestamp: {value}")
        return None


def trade_action_worker():
    metrics.set_worker_running(True)
    while True:
        try:
            payload = trade_queue.get()
            logging.info(f"payload:::: {payload}")
            trade_action_handler(payload)
        except Exception as e:
            logging.error(f"Error: {e}")


def trade_action_handler(data):
    enqueued_at = data.get("_enqueued_at")
    action_type = data.get("action_type")
    client_request_id = data.get("client_request_id")
    symbol = data.get("symbol")

    with DBSessionManager() as db:
        try:
            logging.info(f"Received data from queue: {data}")
            new_trade_id = None
            trade_id_to_close = None
            realized_pnl = None
            book_id = None
            asset_class = None
            audit_logger.info(
                EventType.DB_CREATE,
                f"Processing trade action: {action_type} for client_request_id: {client_request_id}",
                entity_type=EntityType.TRADE.value,
                correlation_id=client_request_id,
                payload={"action_type": action_type, "client_request_id": client_request_id},
                db=db,
            )
            metadata_payload = None
            if action_type == ActionType.OPEN_TRADE.value:
                # Fast-path idempotency read (non-locking). This is only an
                # optimisation to avoid doing work for an obvious duplicate; the
                # authoritative guarantee is the INSERT ... ON CONFLICT below,
                # which is atomic even under concurrent consumers (RACE-2).
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
                            db=db,
                        )
                        db.commit()
                        metrics.record_duplicate(
                            action_type, client_request_id, symbol, enqueued_at
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
                    option_type = data.get("option_type") or "EUROPEAN"
                    strike = _as_float(data.get("strike"))
                    volatility = _as_float(data.get("volatility"))
                    maturity_years = _as_float(data.get("maturity_years"))
                    option_price = _as_float(data.get("option_price"))
                    missing_option = []
                    if strike is None or strike <= 0:
                        missing_option.append("strike")
                    if volatility is None or volatility <= 0:
                        missing_option.append("volatility")
                    if maturity_years is None or maturity_years <= 0:
                        missing_option.append("maturity_years")
                    if option_price is None or option_price < 0:
                        missing_option.append("option_price")
                    if option_right_type not in (
                        OptionRightType.CALL.value,
                        OptionRightType.PUT.value,
                    ):
                        missing_option.append("option_right_type")
                    if option_type != "EUROPEAN":
                        missing_option.append("option_type")
                    if missing_option:
                        _reject_open_trade(
                            db,
                            f"missing OPTION params {missing_option}",
                            action_type,
                            client_request_id,
                            symbol,
                            enqueued_at,
                        )
                        return

                    metadata_payload = {
                        "spot": data.get("spot"),
                        "strike": strike,
                        "volatility": volatility,
                        "maturity_years": maturity_years,
                        "option_right_type": option_right_type,
                        "option_type": option_type,
                        "option_price": option_price,
                    }
                    trade_price = option_price
                    process_note = "option opened"
                elif asset_class == AssetClass.IRS.value:
                    logging.info(f"IRS: {data}.")

                    # IRS jest wyceniany krzywą stóp — parametry kontraktu trzymamy
                    # w metadata_payload, a nie jako spot/price danych rynkowych.
                    required_irs = (
                        "notional",
                        "fixed_rate",
                        "maturity_years",
                        "direction",
                    )
                    missing_irs = [key for key in required_irs if data.get(key) is None]
                    if missing_irs:
                        logging.error(
                            f"OPEN_TRADE IRS rejected: missing params {missing_irs} "
                            f"for client_request_id {client_request_id}"
                        )
                        db.commit()
                        metrics.record_error(
                            action_type,
                            client_request_id,
                            symbol,
                            f"missing IRS params {missing_irs}",
                            enqueued_at,
                        )
                        return

                    metadata_payload = {
                        "notional": data.get("notional"),
                        "fixed_rate": data.get("fixed_rate"),
                        "maturity_years": data.get("maturity_years"),
                        "payments_per_year": data.get("payments_per_year", 1),
                        "direction": data.get("direction"),
                    }
                    # IRS wchodzi na rynek z PV ~ 0.
                    trade_price = data.get("trade_price")
                    if trade_price is None:
                        trade_price = 0
                    process_note = "IRS metadata"
                elif asset_class == AssetClass.FUTURES.value:
                    logging.info(f"FUTURES: {data}.")
                    multiplier = _as_float(data.get("contract_multiplier"))
                    trade_price = _as_float(data.get("trade_price"))
                    if multiplier is None or multiplier <= 0 or trade_price is None:
                        _reject_open_trade(
                            db,
                            "missing FUTURES params contract_multiplier/trade_price",
                            action_type,
                            client_request_id,
                            symbol,
                            enqueued_at,
                        )
                        return
                    metadata_payload = {
                        "contract_multiplier": multiplier,
                        "underlying_symbol": data.get("underlying_symbol"),
                    }
                    if instrument.asset_class == AssetClass.FUTURES.value:
                        instrument.multiplier = multiplier
                    process_note = "futures metadata"
                else:
                    trade_price = data.get("trade_price")
                    process_note = "trade created"

                now = datetime.now(timezone.utc)
                trades_table = Trade.__table__

                trade_values = {
                    "trade_id": uuid.uuid4(),
                    "client_request_id": client_request_id,
                    "book_id": book_id,
                    "asset_class": asset_class,
                    "symbol": symbol,
                    "instrument_id": instrument.instrument_id,
                    "side": data.get("side"),
                    "quantity": data.get("quantity"),
                    "trade_price": trade_price,
                    "trade_currency": data.get("currency"),
                    "trade_date": now,
                    "status": "ACTIVE",
                    "source": _normalize_trade_source(data.get("source")),
                    "market_data_provider": data.get("market_data_provider"),
                    "reference_price": data.get("reference_price"),
                    "reference_price_timestamp": _parse_reference_timestamp(
                        data.get("reference_price_timestamp")
                    ),
                    "metadata": metadata_payload if metadata_payload else None,
                    "opened_at": now,
                    "created_at": now,
                    "updated_at": now,
                }

                insert_stmt = (
                    pg_insert(trades_table)
                    .values(trade_values)
                    .on_conflict_do_nothing(index_elements=["client_request_id"])
                    .returning(trades_table.c.trade_id)
                )
                inserted = db.session.execute(insert_stmt).first()
                if inserted is None:
                    logging.warning(
                        f"Duplicate OPEN_TRADE ignored (unique conflict): "
                        f"client_request_id {client_request_id}"
                    )
                    audit_logger.warning(
                        EventType.DB_REJECT,
                        f"Duplicate OPEN_TRADE ignored for client_request_id {client_request_id}",
                        entity_type=EntityType.TRADE.value,
                        correlation_id=client_request_id,
                        db=db,
                    )
                    db.commit()
                    metrics.record_duplicate(action_type, client_request_id, symbol, enqueued_at)
                    return

                new_trade_id = inserted[0]
                logging.info(f"Successfully opened trade. (Client ID: {client_request_id})")

            elif action_type == ActionType.CLOSE_TRADE.value:
                trade_id_to_close = data.get("trade_id") or None
                logging.info(f"Attempting to close trade with ID: {trade_id_to_close}")

                # FOR NO KEY UPDATE waits if another transaction holds the row,
                # then re-checks status / close_client_request_id under the lock.
                trade_to_close = (
                    db.session.query(Trade)
                    .filter_by(trade_id=trade_id_to_close)
                    .with_for_update(key_share=True)
                    .first()
                )

                if not trade_to_close:
                    logging.error(f"Trade not found in database with ID: {trade_id_to_close}")
                    db.commit()
                    audit_logger.warning(
                        EventType.DB_ERROR,
                        f"CLOSE_TRADE failed: trade {trade_id_to_close} not found",
                        entity_type=EntityType.TRADE.value,
                        entity_id=str(trade_id_to_close),
                        correlation_id=client_request_id,
                        db=db,
                    )
                    db.commit()
                    metrics.record_error(
                        action_type,
                        client_request_id,
                        symbol,
                        "trade not found",
                        enqueued_at,
                    )
                    return

                close_req = None
                if client_request_id and trade_to_close.metadata_payload:
                    close_req = trade_to_close.metadata_payload.get("close_client_request_id")
                    if hasattr(close_req, "as_string"):
                        close_req = close_req.as_string()
                if client_request_id and close_req == client_request_id:
                    logging.warning(
                        f"Duplicate CLOSE_TRADE ignored: client_request_id {client_request_id} "
                        f"already closed trade {trade_id_to_close}"
                    )
                    db.commit()
                    audit_logger.warning(
                        EventType.DB_REJECT,
                        f"Duplicate CLOSE_TRADE ignored for client_request_id {client_request_id}",
                        entity_type=EntityType.TRADE.value,
                        correlation_id=client_request_id,
                        db=db,
                    )
                    db.commit()
                    metrics.record_duplicate(action_type, client_request_id, symbol, enqueued_at)
                    return

                if trade_to_close.status != "ACTIVE":
                    logging.warning(f"Trade {trade_id_to_close} is already closed or inactive.")
                    db.commit()
                    audit_logger.warning(
                        EventType.DB_REJECT,
                        f"CLOSE_TRADE rejected: trade {trade_id_to_close} is not ACTIVE",
                        entity_type=EntityType.TRADE.value,
                        entity_id=str(trade_id_to_close),
                        correlation_id=client_request_id,
                        db=db,
                    )
                    db.commit()
                    metrics.record_error(
                        action_type,
                        client_request_id,
                        trade_to_close.symbol or symbol,
                        "trade not ACTIVE",
                        enqueued_at,
                    )
                    return

                symbol = trade_to_close.symbol or symbol

                # A trade opened against a specific provider must close at
                # THAT provider's current price, not whatever the caller
                # supplies — otherwise realized PnL could be priced off a
                # different source than the position was opened with.
                close_price = None
                close_price_error = None
                market_data_timestamp = None
                is_long = (
                    trade_to_close.side == TradeSide.BUY.value
                    or trade_to_close.side == "BUY"
                )

                if trade_to_close.market_data_provider:
                    latest_quote = db.provider_quotes.get_latest(
                        trade_to_close.market_data_provider, trade_to_close.symbol
                    )
                    if latest_quote is None:
                        close_price_error = (
                            f"no current quote from {trade_to_close.market_data_provider} "
                            f"for {trade_to_close.symbol} to price the close"
                        )
                    else:
                        # Closing a long SELLS into the bid; closing a short
                        # BUYS BACK at the ask — using the midpoint instead
                        # would give away the spread. Only fall back to the
                        # last traded price if the book side we need isn't
                        # quoted (e.g. free-tier providers with bid=ask=None).
                        if is_long:
                            quote_price = (
                                latest_quote.bid
                                if latest_quote.bid is not None
                                else latest_quote.last
                            )
                        else:
                            quote_price = (
                                latest_quote.ask
                                if latest_quote.ask is not None
                                else latest_quote.last
                            )
                        if quote_price is None:
                            close_price_error = (
                                f"quote from {trade_to_close.market_data_provider} for "
                                f"{trade_to_close.symbol} has neither the needed side "
                                f"nor a last price"
                            )
                        else:
                            close_price = float(quote_price)
                            market_data_timestamp = (
                                latest_quote.provider_timestamp or latest_quote.received_at
                            )
                else:
                    raw_close_price = data.get("close_price")
                    try:
                        parsed_close_price = float(raw_close_price)
                    except (TypeError, ValueError):
                        parsed_close_price = None
                    if parsed_close_price is None or parsed_close_price <= 0:
                        close_price_error = f"Invalid close_price: {raw_close_price!r}"
                    else:
                        close_price = parsed_close_price

                if close_price_error:
                    logging.error(f"CLOSE_TRADE failed: {close_price_error}")
                    db.rollback()
                    audit_logger.warning(
                        EventType.DB_REJECT,
                        f"CLOSE_TRADE rejected: {close_price_error}",
                        entity_type=EntityType.TRADE.value,
                        entity_id=str(trade_id_to_close),
                        correlation_id=client_request_id,
                        db=db,
                    )
                    db.commit()
                    metrics.record_error(
                        action_type,
                        client_request_id,
                        symbol,
                        close_price_error,
                        enqueued_at,
                    )
                    return

                trade_to_close.status = "CLOSED"
                trade_to_close.close_price = close_price
                trade_to_close.close_reason = data.get("close_reason")
                trade_to_close.closed_at = datetime.now(timezone.utc)
                if client_request_id:
                    close_metadata = dict(trade_to_close.metadata_payload or {})
                    close_metadata["close_client_request_id"] = client_request_id
                    trade_to_close.metadata_payload = close_metadata

                trade_price = float(trade_to_close.trade_price)
                quantity = float(trade_to_close.quantity)

                if is_long:
                    realized_pnl = round((close_price - trade_price) * quantity, 4) or None
                else:
                    realized_pnl = round((trade_price - close_price) * quantity, 4) or None

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
                    market_data_provider=trade_to_close.market_data_provider,
                    market_data_timestamp=market_data_timestamp,
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
                process_note = "realized PnL written"

            else:
                metrics.record_error(
                    action_type,
                    client_request_id,
                    symbol,
                    f"unsupported action_type: {action_type}",
                    enqueued_at,
                )
                return

            # Row locks are released here. Audit joins this same session
            # (LOCK-3) so we never check out a second pooled connection.
            db.commit()

            if action_type == ActionType.OPEN_TRADE.value:
                audit_logger.info(
                    EventType.DB_CREATE,
                    f"Trade opened: {new_trade_id}",
                    entity_type=EntityType.TRADE.value,
                    entity_id=str(new_trade_id),
                    correlation_id=client_request_id,
                    payload={
                        "book_id": book_id,
                        "symbol": symbol,
                        "asset_class": asset_class,
                        "source": trade_values.get("source"),
                        "market_data_provider": trade_values.get("market_data_provider"),
                        "reference_price": trade_values.get("reference_price"),
                        "reference_price_timestamp": (
                            trade_values["reference_price_timestamp"].isoformat()
                            if trade_values.get("reference_price_timestamp")
                            else None
                        ),
                    },
                    db=db,
                )
            elif action_type == ActionType.CLOSE_TRADE.value:
                audit_logger.info(
                    EventType.DB_CLOSE,
                    f"Trade closed: {trade_id_to_close}",
                    entity_type=EntityType.TRADE.value,
                    entity_id=str(trade_id_to_close),
                    correlation_id=client_request_id,
                    payload={
                        "close_price": data.get("close_price"),
                        "realized_pnl": realized_pnl,
                        "source": trade_to_close.source if trade_to_close else None,
                        "market_data_provider": (
                            trade_to_close.market_data_provider if trade_to_close else None
                        ),
                        "reference_price": (
                            float(trade_to_close.reference_price)
                            if trade_to_close and trade_to_close.reference_price is not None
                            else None
                        ),
                    },
                    db=db,
                )
            db.commit()

            metrics.record_processed(
                action_type, client_request_id, symbol, process_note, enqueued_at
            )

        except Exception as e:
            db.rollback()
            logging.error(f"WENT WRONG: {e}")
            try:
                audit_logger.error(
                    EventType.DB_ERROR,
                    f"Trade action failed: {e}",
                    correlation_id=data.get("client_request_id"),
                    payload={"action_type": data.get("action_type"), "error": str(e)},
                    db=db,
                )
                db.commit()
            except Exception:
                logging.exception("Failed to persist error audit")
            metrics.record_error(
                action_type,
                client_request_id,
                symbol,
                str(e)[:120],
                enqueued_at,
            )

        finally:
            db.close()
