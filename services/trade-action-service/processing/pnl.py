from shared.trading_shared.enums import AssetClass


def futures_multiplier(asset_class, metadata) -> float:
    """Contract multiplier for a futures close. Every other asset class is 1."""
    if asset_class != AssetClass.FUTURES.value:
        return 1.0
    raw = (metadata or {}).get("contract_multiplier")
    try:
        multiplier = float(raw)
    except (TypeError, ValueError):
        return 1.0
    if multiplier <= 0:
        return 1.0
    return multiplier


def realized_pnl(is_long: bool, trade_price, close_price, quantity, multiplier: float = 1.0) -> float:
    """Realized PnL for a close. Zero is a real result and must be stored as 0."""
    trade_price = float(trade_price)
    close_price = float(close_price)
    quantity = float(quantity)
    multiplier = float(multiplier)
    if is_long:
        return round((close_price - trade_price) * quantity * multiplier, 4)
    return round((trade_price - close_price) * quantity * multiplier, 4)


def closing_market_value(close_price, quantity, multiplier: float = 1.0) -> float:
    return float(close_price) * float(quantity) * float(multiplier)
