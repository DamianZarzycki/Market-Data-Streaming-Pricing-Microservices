import logging

from instruments_pricing.option_pricing_service import (
    calculate_european_call_option_price,
    calculate_european_put_option_price,
)

from shared.trading_shared.enums import AssetClass, OptionRightType, TradeSide
from shared.trading_shared.market_data import is_quote_stale

from calculation.formulas import calculate_pnl, current_timestamp, futures_multiplier
from calculation.valuation_feed import valuation_feed


class QuotePricing:
    """Marks a trade from the provider quote it was opened against."""

    def _exit_quote_price(self, side, quote):
        """Price a position could be exited at: bid for a long, ask for a short.

        `last` is only a fallback for providers that don't quote both sides.
        """
        last = quote.get("last")
        bid = quote.get("bid")
        ask = quote.get("ask")
        if side == TradeSide.BUY.value:
            return bid if bid is not None else last
        return ask if ask is not None else last

    def _publish(self, trade, quote, current_price, fair_value, unrealized_pnl):
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
        valuation_feed.store_and_publish(trade, valuation_data)

    def price(self, trade, quote):
        """Reprice a trade from the provider it was opened with.

        IRS is not handled here — the IRS curve service marks those from the
        yield curve. OPTION uses Black-Scholes on this quote as spot. FUTURES
        multiplies the quote by the contract multiplier stored on the trade.
        """
        if trade.asset_class == AssetClass.OPTION.value:
            self._price_option(trade, quote)
            return
        if trade.asset_class == AssetClass.FUTURES.value:
            self._price_futures(trade, quote)
            return
        self._price_linear(trade, quote)

    def _price_linear(self, trade, quote):
        quantity = float(trade.quantity)
        trade_price = float(trade.trade_price)
        side = trade.side
        quote_price = self._exit_quote_price(side, quote)
        if quote_price is None:
            logging.warning(
                f"Skipping trade {trade.trade_id}: provider quote has no usable "
                f"{'bid' if side == TradeSide.BUY.value else 'ask'} nor last price"
            )
            return

        current_price = round(float(quote_price), 6)
        fair_value = round(current_price * quantity, 4)
        unrealized_pnl = calculate_pnl(side, trade_price, current_price, quantity)
        self._publish(trade, quote, current_price, fair_value, unrealized_pnl)

    def _price_option(self, trade, quote):
        params = trade.metadata_payload or {}
        spot = self._exit_quote_price(trade.side, quote)
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
        self._publish(trade, quote, current_price, fair_value, unrealized_pnl)

    def _price_futures(self, trade, quote):
        quote_price = self._exit_quote_price(trade.side, quote)
        if quote_price is None:
            logging.warning(
                f"Skipping FUTURES trade {trade.trade_id}: provider quote has no usable price"
            )
            return

        multiplier = futures_multiplier(trade)
        quantity = float(trade.quantity)
        current_price = round(float(quote_price), 6)
        fair_value = round(current_price * multiplier * quantity, 4)
        unrealized_pnl = calculate_pnl(
            trade.side, trade.trade_price, current_price, quantity, multiplier
        )
        self._publish(trade, quote, current_price, fair_value, unrealized_pnl)


quote_pricing = QuotePricing()
