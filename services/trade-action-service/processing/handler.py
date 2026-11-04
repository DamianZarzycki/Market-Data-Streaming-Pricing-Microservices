import logging

import metrics
from shared.trading_shared.db import AsyncDBSessionManager
from processing.close_trade import ClosedTrade, close_trade
from processing.open_trade import OpenedTrade, open_trade
from shared.trading_shared.audit import append_audit
from shared.trading_shared.enums import (
    ActionType,
    EntityType,
    EventType,
    Severity,
)

_SERVICE_NAME = "trade-action-service"


def _stage(db, event_type, severity, message, **kwargs) -> None:
    """Stage one audit row on the handler's async session."""
    append_audit(
        db.session,
        event_type,
        message,
        service_name=_SERVICE_NAME,
        severity=severity,
        **kwargs,
    )


def _processing_audit(db, action_type, client_request_id) -> None:
    _stage(
        db,
        EventType.DB_CREATE,
        Severity.INFO,
        f"Processing trade action: {action_type} for client_request_id: {client_request_id}",
        entity_type=EntityType.TRADE.value,
        correlation_id=client_request_id,
        payload={"action_type": action_type, "client_request_id": client_request_id},
    )


async def _finish_open(db, opened: OpenedTrade, client_request_id, enqueued_at) -> None:
    # The row lock is released on this commit. The audit below joins the same
    # session, so the follow-up row does not check out a second connection.
    await db.commit()
    trade_values = opened.trade_values
    stamped = trade_values.get("reference_price_timestamp")
    _stage(
        db,
        EventType.DB_CREATE,
        Severity.INFO,
        f"Trade opened: {opened.trade_id}",
        entity_type=EntityType.TRADE.value,
        entity_id=str(opened.trade_id),
        correlation_id=client_request_id,
        payload={
            "book_id": opened.book_id,
            "symbol": opened.symbol,
            "asset_class": opened.asset_class,
            "source": trade_values.get("source"),
            "market_data_provider": trade_values.get("market_data_provider"),
            "reference_price": trade_values.get("reference_price"),
            "reference_price_timestamp": stamped.isoformat() if stamped else None,
        },
    )
    await db.commit()
    metrics.record_processed(
        ActionType.OPEN_TRADE.value,
        client_request_id,
        opened.symbol,
        opened.process_note,
        enqueued_at,
    )


async def _finish_close(db, data, closed: ClosedTrade, client_request_id, enqueued_at) -> None:
    await db.commit()
    trade = closed.trade
    reference = None
    if trade is not None and trade.reference_price is not None:
        reference = float(trade.reference_price)
    _stage(
        db,
        EventType.DB_CLOSE,
        Severity.INFO,
        f"Trade closed: {closed.trade_id}",
        entity_type=EntityType.TRADE.value,
        entity_id=str(closed.trade_id),
        correlation_id=client_request_id,
        payload={
            "close_price": data.get("close_price"),
            "realized_pnl": closed.realized_pnl,
            "source": trade.source if trade else None,
            "market_data_provider": trade.market_data_provider if trade else None,
            "reference_price": reference,
        },
    )
    await db.commit()
    metrics.record_processed(
        ActionType.CLOSE_TRADE.value,
        client_request_id,
        closed.symbol,
        closed.process_note,
        enqueued_at,
    )


async def _record_failure(db, data, exc, action_type, client_request_id, symbol, enqueued_at) -> None:
    await db.rollback()
    logging.error("WENT WRONG: %s", exc)
    try:
        _stage(
            db,
            EventType.DB_ERROR,
            Severity.ERROR,
            f"Trade action failed: {exc}",
            correlation_id=data.get("client_request_id"),
            payload={"action_type": data.get("action_type"), "error": str(exc)},
        )
        await db.commit()
    except Exception:
        logging.exception("Failed to persist error audit")
    metrics.record_error(
        action_type,
        client_request_id,
        symbol,
        str(exc)[:120],
        enqueued_at,
    )


async def trade_action_handler(data: dict) -> None:
    enqueued_at = data.get("_enqueued_at")
    action_type = data.get("action_type")
    client_request_id = data.get("client_request_id")
    symbol = data.get("symbol")

    async with AsyncDBSessionManager() as db:
        try:
            _processing_audit(db, action_type, client_request_id)
            if action_type == ActionType.OPEN_TRADE.value:
                opened = await open_trade(
                    db,
                    data,
                    action_type=action_type,
                    client_request_id=client_request_id,
                    symbol=symbol,
                    enqueued_at=enqueued_at,
                )
                if opened is None:
                    return
                await _finish_open(db, opened, client_request_id, enqueued_at)
                return
            if action_type == ActionType.CLOSE_TRADE.value:
                closed = await close_trade(
                    db,
                    data,
                    action_type=action_type,
                    client_request_id=client_request_id,
                    symbol=symbol,
                    enqueued_at=enqueued_at,
                )
                if closed is None:
                    return
                await _finish_close(db, data, closed, client_request_id, enqueued_at)
                return
            metrics.record_error(
                action_type,
                client_request_id,
                symbol,
                f"unsupported action_type: {action_type}",
                enqueued_at,
            )
        except Exception as exc:
            await _record_failure(
                db, data, exc, action_type, client_request_id, symbol, enqueued_at
            )
