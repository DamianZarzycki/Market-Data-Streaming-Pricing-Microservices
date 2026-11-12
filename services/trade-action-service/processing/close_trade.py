import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import metrics
from processing.pnl import closing_market_value, futures_multiplier, realized_pnl
from shared.trading_shared.audit import append_audit
from shared.trading_shared.enums import EntityType, EventType, Severity, TradeSide, TradeStatus
from shared.trading_shared.models import Valuation

_SERVICE_NAME = "trade-action-service"


def _stage(db, event_type, severity, message, **kwargs) -> None:
    append_audit(
        db.session,
        event_type,
        message,
        service_name=_SERVICE_NAME,
        severity=severity,
        **kwargs,
    )


@dataclass(frozen=True)
class ClosedTrade:
    trade: object
    trade_id: object
    symbol: object
    realized_pnl: float
    process_note: str


def _is_long(side) -> bool:
    return side == TradeSide.BUY.value or side == "BUY"


def _is_duplicate_close(trade, client_request_id) -> bool:
    if not client_request_id or not trade.metadata_payload:
        return False
    close_req = trade.metadata_payload.get("close_client_request_id")
    if hasattr(close_req, "as_string"):
        close_req = close_req.as_string()
    return close_req == client_request_id


async def _commit_then_audit(db, stage_audit) -> None:
    await db.commit()
    stage_audit()
    await db.commit()


async def _missing_trade(db, trade_id, action_type, client_request_id, symbol, enqueued_at) -> None:
    logging.error("Trade not found in database with ID: %s", trade_id)

    def stage_audit():
        _stage(
            db,
            EventType.DB_ERROR,
            Severity.WARNING,
            f"CLOSE_TRADE failed: trade {trade_id} not found",
            entity_type=EntityType.TRADE.value,
            entity_id=str(trade_id),
            correlation_id=client_request_id,
        )

    await _commit_then_audit(db, stage_audit)
    metrics.record_error(action_type, client_request_id, symbol, "trade not found", enqueued_at)


async def _duplicate_trade(db, trade_id, action_type, client_request_id, symbol, enqueued_at) -> None:
    logging.warning(
        "Duplicate CLOSE_TRADE ignored: client_request_id %s already closed trade %s",
        client_request_id,
        trade_id,
    )

    def stage_audit():
        _stage(
            db,
            EventType.DB_REJECT,
            Severity.WARNING,
            f"Duplicate CLOSE_TRADE ignored for client_request_id {client_request_id}",
            entity_type=EntityType.TRADE.value,
            correlation_id=client_request_id,
        )

    await _commit_then_audit(db, stage_audit)
    metrics.record_duplicate(action_type, client_request_id, symbol, enqueued_at)


async def _inactive_trade(db, trade, action_type, client_request_id, symbol, enqueued_at) -> None:
    logging.warning("Trade %s is already closed or inactive.", trade.trade_id)

    def stage_audit():
        _stage(
            db,
            EventType.DB_REJECT,
            Severity.WARNING,
            f"CLOSE_TRADE rejected: trade {trade.trade_id} is not ACTIVE",
            entity_type=EntityType.TRADE.value,
            entity_id=str(trade.trade_id),
            correlation_id=client_request_id,
        )

    await _commit_then_audit(db, stage_audit)
    metrics.record_error(
        action_type,
        client_request_id,
        trade.symbol or symbol,
        "trade not ACTIVE",
        enqueued_at,
    )


def _provider_close_price(quote, trade, is_long: bool):
    # Closing a long sells into the bid. Closing a short buys back at the ask.
    # Last is only a fallback when that side of the book is missing.
    if is_long:
        quote_price = quote.bid if quote.bid is not None else quote.last
    else:
        quote_price = quote.ask if quote.ask is not None else quote.last
    if quote_price is None:
        return None, None, (
            f"quote from {trade.market_data_provider} for {trade.symbol} has neither "
            "the needed side nor a last price"
        )
    return float(quote_price), quote.provider_timestamp or quote.received_at, None


async def _resolve_close_price(db, trade, data, is_long: bool):
    if trade.market_data_provider:
        latest_quote = await db.provider_quotes.get_latest_close_price_async(
            trade.market_data_provider, trade.symbol
        )
        if latest_quote is None:
            return None, None, (
                f"no current quote from {trade.market_data_provider} "
                f"for {trade.symbol} to price the close"
            )
        return _provider_close_price(latest_quote, trade, is_long)

    raw_close_price = data.get("close_price")
    try:
        parsed_close_price = float(raw_close_price)
    except (TypeError, ValueError):
        parsed_close_price = None
    if parsed_close_price is None or parsed_close_price <= 0:
        return None, None, f"Invalid close_price: {raw_close_price!r}"
    return parsed_close_price, None, None


async def _reject_close_price(db, trade_id, action_type, client_request_id, symbol, enqueued_at, price_error) -> None:
    logging.error("CLOSE_TRADE failed: %s", price_error)
    await db.rollback()
    _stage(
        db,
        EventType.DB_REJECT,
        Severity.WARNING,
        f"CLOSE_TRADE rejected: {price_error}",
        entity_type=EntityType.TRADE.value,
        entity_id=str(trade_id),
        correlation_id=client_request_id,
    )
    await db.commit()
    metrics.record_error(action_type, client_request_id, symbol, price_error, enqueued_at)


def _mark_closed(db, trade, data, close_price, market_data_timestamp, client_request_id, is_long: bool) -> float:
    trade.status = TradeStatus.CLOSED.value
    trade.close_price = close_price
    trade.close_reason = data.get("close_reason")
    closed_at = datetime.now(timezone.utc)
    trade.closed_at = closed_at
    if client_request_id:
        close_metadata = dict(trade.metadata_payload or {})
        close_metadata["close_client_request_id"] = client_request_id
        trade.metadata_payload = close_metadata

    trade_price = float(trade.trade_price)
    quantity = float(trade.quantity)
    multiplier = futures_multiplier(trade.asset_class, trade.metadata_payload)
    pnl = realized_pnl(is_long, trade_price, close_price, quantity, multiplier)
    value = closing_market_value(close_price, quantity, multiplier)
    closing_valuation = Valuation(
        valuation_id=uuid.uuid4(),
        trade_id=trade.trade_id,
        book_id=trade.book_id,
        asset_class=trade.asset_class,
        valuation_time=closed_at,
        fair_value=value,
        market_value=value,
        unrealized_pnl=0,
        realized_pnl=pnl,
        total_pnl=pnl,
        currency=trade.trade_currency,
        market_data_provider=trade.market_data_provider,
        market_data_timestamp=market_data_timestamp,
        market_data_reference=f"CLOSE:{trade.symbol}@{closed_at.isoformat()}",
        valuation_payload={
            "close_price": close_price,
            "trade_price": trade_price,
            "quantity": quantity,
            "close_reason": data.get("close_reason"),
        },
    )
    logging.info("Closing valuation for trade %s", trade.trade_id)
    db.valuations.add(closing_valuation)
    # TODO Cache clearing clear cache of trade for trade close
    logging.info("Closed trade %s", trade.trade_id)
    return pnl


async def close_trade(db, data, *, action_type, client_request_id, symbol, enqueued_at) -> ClosedTrade | None:
    trade_id = data.get("trade_id") or None
    logging.info("Closing trade %s", trade_id)
    trade = await db.trades.lock_trade(trade_id)
    if trade is None:
        await _missing_trade(db, trade_id, action_type, client_request_id, symbol, enqueued_at)
        return None
    if _is_duplicate_close(trade, client_request_id):
        await _duplicate_trade(db, trade_id, action_type, client_request_id, symbol, enqueued_at)
        return None
    if trade.status != TradeStatus.ACTIVE.value:
        await _inactive_trade(db, trade, action_type, client_request_id, symbol, enqueued_at)
        return None

    symbol = trade.symbol or symbol
    is_long = _is_long(trade.side)
    close_price, market_data_timestamp, price_error = await _resolve_close_price(db, trade, data, is_long)
    if price_error:
        await _reject_close_price(
            db, trade_id, action_type, client_request_id, symbol, enqueued_at, price_error
        )
        return None

    pnl = _mark_closed(
        db, trade, data, close_price, market_data_timestamp, client_request_id, is_long
    )
    return ClosedTrade(
        trade=trade,
        trade_id=trade_id,
        symbol=symbol,
        realized_pnl=pnl,
        process_note="realized PnL written",
    )
