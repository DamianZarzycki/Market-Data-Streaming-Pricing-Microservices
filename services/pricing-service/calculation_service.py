from datetime import datetime, timezone
import logging
import queue
import threading
import uuid
import persistence
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.models import Valuation

pricing_lock = threading.Lock()
metrics_queue = queue.Queue()   # consumed by metrics_worker for internal stats
sse_queue = queue.Queue()       # consumed by /valuation-stream SSE endpoint

valuations_store = {}           # keyed by str(trade_id) -> latest valuation dict


def current_timestamp():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def calculate_bond_pv(bond_yield):
    """Present Value of one bond unit: face_value=1000, coupon_rate=5%, maturity=5yr."""
    face_value = 1000
    coupon_rate = 0.05
    maturity_years = 5
    coupon = face_value * coupon_rate
    pv = 0.0
    for year in range(1, maturity_years + 1):
        cash_flow = coupon if year < maturity_years else coupon + face_value
        pv += cash_flow / ((1 + bond_yield) ** year)
    return pv


def calculate_pnl(side, trade_price, current_price, quantity, multiplier=1.0):
    """Unrealized PnL: positive means profit."""
    trade_price = float(trade_price)
    current_price = float(current_price)
    quantity = float(quantity)
    multiplier = float(multiplier)
    if side == "BUY":
        return round((current_price - trade_price) * quantity * multiplier, 4)
    elif side == "SELL":
        return round((trade_price - current_price) * quantity * multiplier, 4)
    else:
        logging.error(f"Unknown trade side: {side}")
        raise ValueError(f"Unknown trade side: {side}")


def _price_trade(tick, trade, asset_type):
    """Calculate fair value and PnL for a single active trade, then persist and publish."""

    quantity = float(trade.quantity)
    trade_price = float(trade.trade_price)
    side = trade.side
    trade_id = str(trade.trade_id)
    symbol = trade.symbol
    multiplier = 1.0
    current_price = None
    fair_value = None
    market_value = None

    if asset_type == "EQUITY":
        bid = tick.get("bid")
        ask = tick.get("ask")
        if bid is None or ask is None:
            return
        current_price = (bid + ask) / 2.0
        fair_value = round(current_price * quantity, 4)
        market_value = fair_value

    elif asset_type == "BOND":
        bond_yield = tick.get("yield")
        if bond_yield is None:
            return
        current_price = calculate_bond_pv(bond_yield)
        fair_value = round(current_price * quantity, 4)
        market_value = fair_value

    elif asset_type == "FX":
        spot = tick.get("spot")
        domestic_rate = tick.get("domestic_rate")
        foreign_rate = tick.get("foreign_rate")
        tenor_years = tick.get("tenor_years", 1)
        if spot is None or domestic_rate is None or foreign_rate is None:
            return
        current_price = spot * (1 + domestic_rate * tenor_years) / (1 + foreign_rate * tenor_years)
        fair_value = round(current_price * quantity, 4)
        market_value = fair_value

    elif asset_type == "COMMODITY":
        spot = tick.get("spot") or tick.get("price")
        if spot is None:
            return
        current_price = float(spot)
        fair_value = round(current_price * quantity, 4)
        market_value = fair_value

    elif asset_type == "FUTURES":
        futures_price = tick.get("futures_price") or tick.get("price")
        contract_multiplier = tick.get("contract_multiplier", 1)
        if futures_price is None:
            return
        current_price = float(futures_price)
        multiplier = float(contract_multiplier)
        fair_value = round(current_price * multiplier * quantity, 4)
        market_value = fair_value

    current_price = round(current_price, 6)
    unrealized_pnl = calculate_pnl(side, trade_price, current_price, quantity, multiplier)
    realized_pnl = 0.0
    total_pnl = round(unrealized_pnl + realized_pnl, 4)
    valuation_time = current_timestamp()

    valuation_data = {
        "trade_id": trade_id,
        "book_id": str(trade.book_id),
        "asset_class": asset_type,
        "symbol": symbol,
        "side": side,
        "fair_value": fair_value,
        "market_value": market_value,
        "unrealized_pnl": unrealized_pnl,
        "realized_pnl": realized_pnl,
        "total_pnl": total_pnl,
        "currency": trade.trade_currency,
        "valuation_time": valuation_time,
    }

    with pricing_lock:
        valuations_store[trade_id] = valuation_data

    logging.info(
        f"Priced trade {trade_id} ({symbol}, {asset_type}): "
        f"fair_value={fair_value}, unrealized_pnl={unrealized_pnl}"
    )
    with DBSessionManager() as db:
            valuation_time = datetime.now(timezone.utc)
            try:
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

    metrics_queue.put({
        "type": "PRICING_DONE",
        "timestamp": valuation_time,
        "trade_id": trade_id,
        "instrument": trade_id,
        "value": fair_value,
    })
    sse_queue.put(valuation_data)


def recalculate_valuations(tick):
    """Find all active trades matching the incoming tick and reprice each one."""
    import cache_service

    asset_type = tick.get("asset_type")
    symbol = tick.get("symbol")

    if not asset_type or not symbol:
        return

    with cache_service.cache_lock:
        matching_trades = [
            t for t in cache_service.active_trades_cache.values()
            if t.symbol == symbol and t.asset_class == asset_type
        ]

    for trade in matching_trades:
        try:
            _price_trade(tick, trade, asset_type)
        except Exception as e:
            logging.error(f"Error pricing trade {trade.trade_id}: {e}")