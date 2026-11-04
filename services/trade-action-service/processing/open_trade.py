import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert

import metrics
from shared.trading_shared.audit import append_audit
from shared.trading_shared.enums import (
    AssetClass,
    EntityType,
    EventType,
    OptionRightType,
    Severity,
    TradeSource,
    TradeStatus,
)
from shared.trading_shared.models import Instrument, Trade

_SERVICE_NAME = "trade-action-service"


@dataclass(frozen=True)
class OpenedTrade:
    trade_id: object
    book_id: object
    asset_class: object
    symbol: object
    trade_values: dict
    process_note: str


@dataclass(frozen=True)
class _Terms:
    metadata: dict | None
    trade_price: object
    process_note: str


def _as_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_trade_source(source_value):
    """Keep TRADING_TICKET and GENERATED. Anything else is GENERATED."""
    normalized = (source_value or "").strip().upper()
    if normalized in (TradeSource.TRADING_TICKET.value, TradeSource.GENERATED.value):
        return normalized
    return TradeSource.GENERATED.value


def parse_reference_timestamp(value):
    """Parse an ISO-8601 timestamp. `Z` is UTC. Unparseable values are stored as NULL."""
    if not value or not isinstance(value, str):
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        logging.warning("Could not parse reference_price_timestamp: %s", value)
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


async def _reject_open_trade(db, note, action_type, client_request_id, symbol, enqueued_at) -> None:
    logging.error(
        "OPEN_TRADE rejected: %s for client_request_id %s",
        note,
        client_request_id,
    )
    await db.commit()
    metrics.record_error(action_type, client_request_id, symbol, note, enqueued_at)


async def _option_terms(db, data, action_type, client_request_id, symbol, enqueued_at) -> _Terms | None:
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
    if option_right_type not in (OptionRightType.CALL.value, OptionRightType.PUT.value):
        missing_option.append("option_right_type")
    if option_type != "EUROPEAN":
        missing_option.append("option_type")
    if missing_option:
        await _reject_open_trade(
            db,
            f"missing OPTION params {missing_option}",
            action_type,
            client_request_id,
            symbol,
            enqueued_at,
        )
        return None
    return _Terms(
        metadata={
            "spot": data.get("spot"),
            "strike": strike,
            "volatility": volatility,
            "maturity_years": maturity_years,
            "option_right_type": option_right_type,
            "option_type": option_type,
            "option_price": option_price,
        },
        trade_price=option_price,
        process_note="option opened",
    )


async def _irs_terms(db, data, action_type, client_request_id, symbol, enqueued_at) -> _Terms | None:
    # The curve prices an IRS. The contract lives in metadata, not as a spot.
    required_irs = ("notional", "fixed_rate", "maturity_years", "direction")
    missing_irs = [key for key in required_irs if data.get(key) is None]
    if missing_irs:
        logging.error(
            "OPEN_TRADE IRS rejected: missing params %s for client_request_id %s",
            missing_irs,
            client_request_id,
        )
        await db.commit()
        metrics.record_error(
            action_type,
            client_request_id,
            symbol,
            f"missing IRS params {missing_irs}",
            enqueued_at,
        )
        return None
    trade_price = data.get("trade_price")
    if trade_price is None:
        # An IRS opens with PV near 0.
        trade_price = 0
    return _Terms(
        metadata={
            "notional": data.get("notional"),
            "fixed_rate": data.get("fixed_rate"),
            "maturity_years": data.get("maturity_years"),
            "payments_per_year": data.get("payments_per_year", 1),
            "direction": data.get("direction"),
        },
        trade_price=trade_price,
        process_note="IRS metadata",
    )


async def _futures_terms(db, data, instrument, action_type, client_request_id, symbol, enqueued_at) -> _Terms | None:
    multiplier = _as_float(data.get("contract_multiplier"))
    trade_price = _as_float(data.get("trade_price"))
    if multiplier is None or multiplier <= 0 or trade_price is None:
        await _reject_open_trade(
            db,
            "missing FUTURES params contract_multiplier/trade_price",
            action_type,
            client_request_id,
            symbol,
            enqueued_at,
        )
        return None
    if instrument.asset_class == AssetClass.FUTURES.value:
        instrument.multiplier = multiplier
    return _Terms(
        metadata={
            "contract_multiplier": multiplier,
            "underlying_symbol": data.get("underlying_symbol"),
        },
        trade_price=trade_price,
        process_note="futures metadata",
    )


async def _contract_terms(db, data, instrument, action_type, client_request_id, symbol, enqueued_at) -> _Terms | None:
    asset_class = data.get("asset_class")
    if asset_class == AssetClass.OPTION.value:
        return await _option_terms(db, data, action_type, client_request_id, symbol, enqueued_at)
    if asset_class == AssetClass.IRS.value:
        return await _irs_terms(db, data, action_type, client_request_id, symbol, enqueued_at)
    if asset_class == AssetClass.FUTURES.value:
        return await _futures_terms(
            db, data, instrument, action_type, client_request_id, symbol, enqueued_at
        )
    return _Terms(metadata=None, trade_price=data.get("trade_price"), process_note="trade created")


async def _ignore_duplicate(db, action_type, client_request_id, symbol, enqueued_at) -> None:
    logging.warning(
        "Duplicate OPEN_TRADE ignored (unique conflict): client_request_id %s",
        client_request_id,
    )
    append_audit(
        db.session,
        EventType.DB_REJECT,
        f"Duplicate OPEN_TRADE ignored for client_request_id {client_request_id}",
        service_name=_SERVICE_NAME,
        severity=Severity.WARNING,
        entity_type=EntityType.TRADE.value,
        correlation_id=client_request_id,
    )
    await db.commit()
    metrics.record_duplicate(action_type, client_request_id, symbol, enqueued_at)


async def open_trade(db, data, *, action_type, client_request_id, symbol, enqueued_at) -> OpenedTrade | None:
    book_id = data.get("book_id")
    asset_class = data.get("asset_class")
    symbol = data.get("symbol")
    instrument = await db.instruments.get_by_symbol_async(symbol)
    if not instrument:
        logging.info("Instrument not found: %s. Creating new instrument.", symbol)
        instrument = Instrument(symbol=symbol, asset_class=asset_class, multiplier=1)
        db.instruments.add(instrument)
        await db.flush()

    terms = await _contract_terms(
        db, data, instrument, action_type, client_request_id, symbol, enqueued_at
    )
    if terms is None:
        return None

    now = datetime.now(timezone.utc)
    trade_values = {
        "trade_id": uuid.uuid4(),
        "client_request_id": client_request_id,
        "book_id": book_id,
        "asset_class": asset_class,
        "symbol": symbol,
        "instrument_id": instrument.instrument_id,
        "side": data.get("side"),
        "quantity": data.get("quantity"),
        "trade_price": terms.trade_price,
        "trade_currency": data.get("currency"),
        "trade_date": now,
        "status": TradeStatus.ACTIVE.value,
        "source": _normalize_trade_source(data.get("source")),
        "market_data_provider": data.get("market_data_provider"),
        "reference_price": data.get("reference_price"),
        "reference_price_timestamp": parse_reference_timestamp(data.get("reference_price_timestamp")),
        "metadata": terms.metadata if terms.metadata else None,
        "opened_at": now,
        "created_at": now,
        "updated_at": now,
    }
    insert_stmt = (
        pg_insert(Trade.__table__)
        .values(trade_values)
        .on_conflict_do_nothing(index_elements=["client_request_id"])
        .returning(Trade.__table__.c.trade_id)
    )
    inserted = (await db.execute(insert_stmt)).first()
    if inserted is None:
        await _ignore_duplicate(db, action_type, client_request_id, symbol, enqueued_at)
        return None

    logging.info("Opened trade %s", client_request_id)
    return OpenedTrade(
        trade_id=inserted[0],
        book_id=book_id,
        asset_class=asset_class,
        symbol=symbol,
        trade_values=trade_values,
        process_note=terms.process_note,
    )
