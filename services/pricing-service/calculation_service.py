from datetime import datetime, timezone
import logging
import queue
import threading
from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import AssetClass, OptionRightType, TradeSide
from math import erf, sqrt, log

import valuation_service
from instruments_pricing.irs_pricing_service import price_irs
from instruments_pricing.option_pricing_service import (
    calculate_european_call_option_price,
    calculate_european_put_option_price,
)

pricing_lock = threading.Lock()
metrics_queue = queue.Queue()   # consumed by metrics_worker for internal stats
sse_subscribers = []            # list of per-client queues for /valuation-stream
sse_subscribers_lock = threading.Lock()

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
    if side == TradeSide.BUY.value:
        return round((current_price - trade_price) * quantity * multiplier, 4)
    elif side == TradeSide.SELL.value:
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
    
    elif asset_type == "OPTION":
        spot = tick.get("spot")
        volatility = tick.get("volatility")
        params = trade.metadata_payload or {}
        strike = params.get("strike")
        maturity_years = params.get("maturity_years")
        option_right_type = params.get("option_right_type")
        if (
            spot is None
            or volatility is None
            or strike is None
            or maturity_years is None
            or option_right_type is None
        ):
            logging.warning(
                f"Skipping OPTION trade {trade.trade_id}: missing inputs "
                f"(spot/volatility from tick, strike/maturity/right from metadata)"
            )
            return
        option_details = {
            "spot": float(spot),
            "strike": float(strike),
            "volatility": float(volatility),
            "maturity_years": float(maturity_years),
        }
        if option_right_type == OptionRightType.CALL.value:
            current_price = calculate_european_call_option_price(option_details)
        elif option_right_type == OptionRightType.PUT.value:
            current_price = calculate_european_put_option_price(option_details)
        else:
            logging.error(f"Unknown option right type: {option_right_type}")
            return
        fair_value = round(current_price * quantity, 4)
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

    store_and_publish(trade, valuation_data)


def store_and_publish(trade, valuation_data):
    trade_id = valuation_data["trade_id"]

    with pricing_lock:
        valuations_store[trade_id] = valuation_data

    logging.info(
        f"Priced trade {trade_id} ({valuation_data.get('symbol')}, {valuation_data.get('asset_class')}): "
        f"fair_value={valuation_data.get('fair_value')}, unrealized_pnl={valuation_data.get('unrealized_pnl')}"
    )

    valuation_service.save_valuation(trade, valuation_data)

    metrics_queue.put({
        "type": "PRICING_DONE",
        "timestamp": valuation_data["valuation_time"],
        "trade_id": trade_id,
        "instrument": trade_id,
        "symbol": valuation_data.get("symbol"),
        "asset_class": valuation_data.get("asset_class"),
        "value": valuation_data.get("fair_value"),
    })
    logging.info(f"Published valuation for trade {trade_id} to metrics queue.")
    with sse_subscribers_lock:
        for subscriber_queue in sse_subscribers:
            subscriber_queue.put(valuation_data)


def price_irs_trade(trade, curve):
    params = trade.metadata_payload or {}
    required = ("notional", "fixed_rate", "maturity_years", "payments_per_year", "direction")
    missing = [k for k in required if params.get(k) is None]
    if missing:
        logging.warning(
            f"Skipping IRS trade {trade.trade_id}: missing params {missing} in metadata_payload"
        )
        return

    result = price_irs(
        curve=curve,
        notional=float(params["notional"]),
        fixed_rate=float(params["fixed_rate"]),
        maturity_years=float(params["maturity_years"]),
        payments_per_year=int(params["payments_per_year"]),
        direction=params["direction"],
    )

    pv = round(result["pv"], 4)
    valuation_data = {
        "trade_id": str(trade.trade_id),
        "book_id": str(trade.book_id),
        "asset_class": AssetClass.IRS.value,
        "symbol": trade.symbol,
        "side": trade.side,
        "fair_value": pv,
        "market_value": pv,
        "unrealized_pnl": pv,
        "realized_pnl": 0.0,
        "total_pnl": pv,
        "currency": trade.trade_currency,
        "valuation_time": current_timestamp(),
        "pricing_details": {
            "fixed_leg_pv": round(result["fixed_leg_pv"], 4),
            "floating_leg_pv": round(result["floating_leg_pv"], 4),
            "receive_leg_pv": round(result["receive_leg_pv"], 4),
            "pay_leg_pv": round(result["pay_leg_pv"], 4),
            "direction": params["direction"],
            "curve_currency": curve.get("currency"),
            "tenors": curve.get("tenors"),
            "rates": curve.get("rates"),
        },
    }

    store_and_publish(trade, valuation_data)


def update_curve_and_reprice_irs(tick):
    import cache_service

    currency = tick.get("currency")
    curve_type = tick.get("curve_type")
    if not currency or not curve_type:
        logging.warning(f"Ignoring curve tick without curve_id: {tick}")
        return

    with DBSessionManager() as db:
        curve_row = db.market_data.get_curve(currency, curve_type)

    if curve_row is None:
        logging.warning(
            f"No {currency} {curve_type} curve in MarketDataCurve yet; skipping IRS repricing"
        )
        return

    currency = curve_row.currency
    curve = {
        "tenors": curve_row.tenors,
        "rates": curve_row.rates,
        "currency": currency,
    }

    with cache_service.cache_lock:
        irs_trades = [
            t for t in cache_service.active_trades_cache.values()
            if t.asset_class == AssetClass.IRS.value and t.trade_currency == currency
        ]

    logging.info(
        f"{currency} {curve_type} curve: repricing {len(irs_trades)} IRS trade(s)"
    )
    for trade in irs_trades:
        try:
            price_irs_trade(trade, curve)
        except Exception as e:
            logging.error(f"Error pricing IRS trade {trade.trade_id}: {e}")


def recalculate_valuations(tick):
    """Route an incoming market tick: curve ticks reprice IRS, spot ticks reprice by symbol."""
    import cache_service
    import book_metrics_service

    if tick.get("curve_id"):
        update_curve_and_reprice_irs(tick)
        return

    asset_type = tick.get("asset_type")
    symbol = tick.get("symbol")

    if not asset_type or not symbol:
        return

    # The benchmark index is not a tradable instrument: it only drives the
    # book-level alpha/beta computation, acting as the sampling clock.
    if asset_type == AssetClass.BENCHMARK.value:
        book_metrics_service.on_benchmark_tick(tick)
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

