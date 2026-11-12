import logging
from datetime import datetime, timezone

from shared.trading_shared.enums import TradeSide


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
    if side == TradeSide.BUY.value:
        return round((current_price - trade_price) * quantity * multiplier, 4)
    elif side == TradeSide.SELL.value:
        return round((trade_price - current_price) * quantity * multiplier, 4)
    else:
        logging.error(f"Unknown trade side: {side}")
        raise ValueError(f"Unknown trade side: {side}")


def _positive_multiplier(raw):
    try:
        multiplier = float(raw)
    except (TypeError, ValueError):
        return None
    if multiplier <= 0:
        return None
    return multiplier


def futures_multiplier(trade, tick=None):
    """Trade metadata wins, then the tick, then 1. Non-positive values are skipped."""
    params = trade.metadata_payload or {}
    candidates = [params.get("contract_multiplier")]
    if tick is not None:
        candidates.append(tick.get("contract_multiplier"))
    for raw in candidates:
        if raw is None:
            continue
        multiplier = _positive_multiplier(raw)
        if multiplier is not None:
            return multiplier
    return 1.0
