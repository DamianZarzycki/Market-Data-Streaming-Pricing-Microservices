import logging
import queue
import threading
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from instruments_pricing.irs_pricing_service import price_irs
from instruments_pricing.option_pricing_service import (
    calculate_european_call_option_price,
    calculate_european_put_option_price,
)

from shared.trading_shared.db import DBSessionManager
from shared.trading_shared.enums import AssetClass, CurveType, OptionRightType, SIMULATED_PROVIDER, TradeSide
from shared.trading_shared.market_data import is_quote_stale

pricing_lock = threading.Lock()
metrics_queue = queue.Queue()  # consumed by metrics_worker for internal stats
sse_subscribers = []  # list of per-client queues for /valuation-stream
sse_subscribers_lock = threading.Lock()

valuations_store = {}  # keyed by str(trade_id) -> latest valuation dict
trade_valuation_queue = queue.Queue()


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
        "current_price": current_price,
        "entry_price": round(trade_price, 6),
        "unrealized_pnl": unrealized_pnl,
        "realized_pnl": realized_pnl,
        "total_pnl": total_pnl,
        "currency": trade.trade_currency,
        "valuation_time": valuation_time,
    }

    store_and_publish(trade, valuation_data)


def _json_safe(value):
    """Copy a valuation payload into types json.dumps and JSONB accept.

    Curve rows carry as_of_time as a datetime. Leaving it in the payload
    makes /valuation-stream and the valuations insert return 500.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def store_and_publish(trade, valuation_data):
    valuation_data = _json_safe(valuation_data)
    trade_id = valuation_data["trade_id"]
    trade_valuation_queue.put(valuation_data)

    with pricing_lock:
        valuations_store[trade_id] = valuation_data

    logging.info(
        f"Priced trade {trade_id} ({valuation_data.get('symbol')}, {valuation_data.get('asset_class')}): "
        f"fair_value={valuation_data.get('fair_value')}, unrealized_pnl={valuation_data.get('unrealized_pnl')}"
    )

    metrics_queue.put(
        {
            "type": "PRICING_DONE",
            "timestamp": valuation_data["valuation_time"],
            "trade_id": trade_id,
            "instrument": trade_id,
            "symbol": valuation_data.get("symbol"),
            "asset_class": valuation_data.get("asset_class"),
            "value": valuation_data.get("fair_value"),
        }
    )
    logging.info(f"Published valuation for trade {trade_id} to metrics queue.")
    with sse_subscribers_lock:
        for subscriber_queue in sse_subscribers:
            subscriber_queue.put(valuation_data)


def _exit_quote_price(side, quote):
    """Price a position could be exited at: bid for a long, ask for a short.

    `last` is only a fallback for providers that don't quote both sides.
    """
    last = quote.get("last")
    bid = quote.get("bid")
    ask = quote.get("ask")
    if side == TradeSide.BUY.value:
        return bid if bid is not None else last
    return ask if ask is not None else last


def _publish_provider_valuation(trade, quote, current_price, fair_value, unrealized_pnl):
    realized_pnl = 0.0
    total_pnl = round(unrealized_pnl + realized_pnl, 4)
    market_data_timestamp = quote.get("provider_timestamp") or quote.get("received_at")
    stale, age_seconds = is_quote_stale(trade.asset_class, market_data_timestamp)
    valuation_data = {
        "trade_id": str(trade.trade_id),
        "book_id": str(trade.book_id),
        "asset_class": trade.asset_class,
        "symbol": trade.symbol,
        "side": trade.side,
        "fair_value": fair_value,
        "market_value": fair_value,
        "current_price": current_price,
        "entry_price": round(float(trade.trade_price), 6),
        "unrealized_pnl": unrealized_pnl,
        "realized_pnl": realized_pnl,
        "total_pnl": total_pnl,
        "currency": trade.trade_currency,
        "valuation_time": current_timestamp(),
        "market_data_provider": quote.get("provider"),
        "market_data_timestamp": market_data_timestamp,
        "is_stale": stale,
        "market_data_age_seconds": round(age_seconds, 3) if age_seconds is not None else None,
    }
    store_and_publish(trade, valuation_data)


def price_trade_from_provider_quote(trade, quote):
    """Reprice a trade from the provider it was opened with.

    IRS is not handled here — curve_pricing_worker marks those from the
    yield curve. OPTION uses Black-Scholes on this quote as spot. FUTURES
    multiplies the quote by the contract multiplier stored on the trade.
    """
    if trade.asset_class == AssetClass.OPTION.value:
        _price_option_from_provider_quote(trade, quote)
        return
    if trade.asset_class == AssetClass.FUTURES.value:
        _price_futures_from_provider_quote(trade, quote)
        return
    _price_linear_from_provider_quote(trade, quote)


def _price_linear_from_provider_quote(trade, quote):
    quantity = float(trade.quantity)
    trade_price = float(trade.trade_price)
    side = trade.side
    quote_price = _exit_quote_price(side, quote)
    if quote_price is None:
        logging.warning(
            f"Skipping trade {trade.trade_id}: provider quote has no usable "
            f"{'bid' if side == TradeSide.BUY.value else 'ask'} nor last price"
        )
        return

    current_price = round(float(quote_price), 6)
    fair_value = round(current_price * quantity, 4)
    unrealized_pnl = calculate_pnl(side, trade_price, current_price, quantity)
    _publish_provider_valuation(trade, quote, current_price, fair_value, unrealized_pnl)


def _price_option_from_provider_quote(trade, quote):
    params = trade.metadata_payload or {}
    spot = _exit_quote_price(trade.side, quote)
    strike = params.get("strike")
    volatility = params.get("volatility")
    maturity_years = params.get("maturity_years")
    option_right_type = params.get("option_right_type")
    if (
        spot is None
        or strike is None
        or volatility is None
        or maturity_years is None
        or option_right_type is None
    ):
        logging.warning(
            f"Skipping OPTION trade {trade.trade_id}: missing inputs "
            f"(spot from quote, strike/volatility/maturity/right from metadata)"
        )
        return

    option_details = {
        "spot": float(spot),
        "strike": float(strike),
        "volatility": float(volatility),
        "maturity_years": float(maturity_years),
    }
    if option_right_type == OptionRightType.CALL.value:
        premium = calculate_european_call_option_price(option_details)
    elif option_right_type == OptionRightType.PUT.value:
        premium = calculate_european_put_option_price(option_details)
    else:
        logging.error(f"Unknown option right type: {option_right_type}")
        return

    quantity = float(trade.quantity)
    current_price = round(float(premium), 6)
    fair_value = round(current_price * quantity, 4)
    unrealized_pnl = calculate_pnl(trade.side, trade.trade_price, current_price, quantity)
    _publish_provider_valuation(trade, quote, current_price, fair_value, unrealized_pnl)


def _price_futures_from_provider_quote(trade, quote):
    quote_price = _exit_quote_price(trade.side, quote)
    if quote_price is None:
        logging.warning(
            f"Skipping FUTURES trade {trade.trade_id}: provider quote has no usable price"
        )
        return

    params = trade.metadata_payload or {}
    try:
        multiplier = float(params.get("contract_multiplier") or 1)
    except (TypeError, ValueError):
        multiplier = 1.0
    if multiplier <= 0:
        multiplier = 1.0

    quantity = float(trade.quantity)
    current_price = round(float(quote_price), 6)
    fair_value = round(current_price * multiplier * quantity, 4)
    unrealized_pnl = calculate_pnl(
        trade.side, trade.trade_price, current_price, quantity, multiplier
    )
    _publish_provider_valuation(trade, quote, current_price, fair_value, unrealized_pnl)


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
        "market_data_provider": curve.get("provider"),
        "market_data_timestamp": curve.get("as_of_time"),
        "pricing_details": {
            "fixed_leg_pv": round(result["fixed_leg_pv"], 4),
            "floating_leg_pv": round(result["floating_leg_pv"], 4),
            "receive_leg_pv": round(result["receive_leg_pv"], 4),
            "pay_leg_pv": round(result["pay_leg_pv"], 4),
            "direction": params["direction"],
            "curve_currency": curve.get("currency"),
            "curve_provider": curve.get("provider"),
            "curve_name": curve.get("curve_name"),
            "tenors": curve.get("tenors"),
            "rates": curve.get("rates"),
        },
    }

    store_and_publish(trade, valuation_data)


def _curve_payload_from_row(curve_row):
    return {
        "tenors": curve_row.tenors,
        "rates": curve_row.rates,
        "currency": curve_row.currency,
        "provider": curve_row.provider,
        "curve_name": curve_row.curve_name,
        "curve_type": curve_row.curve_type,
        "as_of_time": (
            curve_row.as_of_time.isoformat()
            if isinstance(curve_row.as_of_time, datetime)
            else curve_row.as_of_time
        ),
    }


def reprice_irs_for_currency(currency, curve):
    import cache_service

    with cache_service.cache_lock:
        irs_trades = [
            t
            for t in cache_service.active_trades_cache.values()
            if t.asset_class == AssetClass.IRS.value and t.trade_currency == currency
        ]

    if not irs_trades:
        return

    logging.info(
        "%s %s curve (%s): repricing %s IRS trade(s)",
        currency,
        curve.get("curve_type"),
        curve.get("provider"),
        len(irs_trades),
    )
    for trade in irs_trades:
        try:
            price_irs_trade(trade, curve)
        except Exception as e:
            logging.error(f"Error pricing IRS trade {trade.trade_id}: {e}")


def update_curve_and_reprice_irs(tick):
    currency = tick.get("currency")
    curve_type = tick.get("curve_type")
    if not currency or not curve_type:
        logging.warning(f"Ignoring curve tick missing currency/curve_type: {tick}")
        return
    if curve_type != CurveType.YIELD_CURVE.value:
        return

    with DBSessionManager() as db:
        external_curve = db.market_data.get_curve(
            currency,
            CurveType.YIELD_CURVE.value,
            exclude_providers=[SIMULATED_PROVIDER],
        )
        if external_curve is not None:
            return
        curve_row = db.market_data.get_curve(
            currency,
            curve_type,
            provider=tick.get("provider") or SIMULATED_PROVIDER,
        )

    if curve_row is None:
        logging.warning(
            f"No {currency} {curve_type} curve in MarketDataCurve yet; skipping IRS repricing"
        )
        return

    reprice_irs_for_currency(curve_row.currency, _curve_payload_from_row(curve_row))


def reprice_irs_from_persisted_curves():
    """Mark IRS from the latest non-simulated YIELD_CURVE per currency (FRED/ECB)."""
    with DBSessionManager() as db:
        curve_rows = db.market_data.get_latest_curves_by_currency(
            CurveType.YIELD_CURVE.value,
            exclude_providers=[SIMULATED_PROVIDER],
        )

    for curve_row in curve_rows:
        reprice_irs_for_currency(curve_row.currency, _curve_payload_from_row(curve_row))


def recalculate_valuations(tick):
    """Route an incoming market tick: curve ticks reprice IRS, spot ticks reprice by symbol."""
    from books import book_metrics_service
    import cache_service

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
        # TODO If data comes from external, we dont have tick for this generated
        book_metrics_service.on_benchmark_tick(tick)
        return

    with cache_service.cache_lock:
        matching_trades = [
            t
            for t in cache_service.active_trades_cache.values()
            # A trade with a market_data_provider assigned is priced
            # exclusively by price_trade_from_provider_quote() (see
            # provider_pricing_worker) — never mix it with simulator ticks.
            if t.symbol == symbol
            and t.asset_class == asset_type
            and not t.market_data_provider
        ]

    for trade in matching_trades:
        try:
            _price_trade(tick, trade, asset_type)
        except Exception as e:
            logging.error(f"Error pricing trade {trade.trade_id}: {e}")
