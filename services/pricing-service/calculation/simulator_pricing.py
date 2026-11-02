import logging

from instruments_pricing.option_pricing_service import (
    calculate_european_call_option_price,
    calculate_european_put_option_price,
)

from shared.trading_shared.enums import AssetClass, OptionRightType

import cache_service
from calculation.formulas import calculate_bond_pv, calculate_pnl, current_timestamp, futures_multiplier
from calculation.valuation_feed import valuation_feed

_PRICED_FROM_TICK = {
    AssetClass.EQUITY.value,
    AssetClass.BOND.value,
    AssetClass.FX.value,
    AssetClass.COMMODITY.value,
    AssetClass.FUTURES.value,
    AssetClass.OPTION.value,
}


class SimulatorPricing:
    """Marks trades from the simulator stream."""

    def price_trade(self, tick, trade, asset_type):
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

        if asset_type == AssetClass.EQUITY.value:
            bid = tick.get("bid")
            ask = tick.get("ask")
            if bid is None or ask is None:
                return
            current_price = (bid + ask) / 2.0
            fair_value = round(current_price * quantity, 4)
            market_value = fair_value

        elif asset_type == AssetClass.BOND.value:
            bond_yield = tick.get("yield")
            if bond_yield is None:
                return
            current_price = calculate_bond_pv(bond_yield)
            fair_value = round(current_price * quantity, 4)
            market_value = fair_value

        elif asset_type == AssetClass.FX.value:
            spot = tick.get("spot")
            if spot is None:
                return
            domestic_rate = tick.get("domestic_rate")
            foreign_rate = tick.get("foreign_rate")
            if domestic_rate is None or foreign_rate is None:
                current_price = float(spot)
            else:
                tenor_years = tick.get("tenor_years", 1)
                current_price = spot * (1 + domestic_rate * tenor_years) / (1 + foreign_rate * tenor_years)
            fair_value = round(current_price * quantity, 4)
            market_value = fair_value

        elif asset_type == AssetClass.COMMODITY.value:
            spot = tick.get("spot") or tick.get("price")
            if spot is None:
                return
            current_price = float(spot)
            fair_value = round(current_price * quantity, 4)
            market_value = fair_value

        elif asset_type == AssetClass.FUTURES.value:
            futures_price = tick.get("futures_price") or tick.get("price")
            if futures_price is None:
                return
            current_price = float(futures_price)
            multiplier = futures_multiplier(trade, tick)
            fair_value = round(current_price * multiplier * quantity, 4)
            market_value = fair_value

        elif asset_type == AssetClass.OPTION.value:
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

        else:
            return

        if current_price is None:
            return

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

        valuation_feed.store_and_publish(trade, valuation_data)

    def recalculate(self, tick):
        """Route a spot tick. Curve ticks are priced by update_curve_and_reprice_irs."""
        from books import book_metrics_service

        asset_type = tick.get("asset_type")
        symbol = tick.get("symbol")

        if not asset_type or not symbol:
            return

        # The benchmark index is not a tradable instrument: it only drives the
        # book-level alpha/beta computation, acting as the sampling clock.
        if asset_type == AssetClass.BENCHMARK.value:
            book_metrics_service.on_benchmark_tick(tick)
            return

        # IRS is marked from the yield curve, not from the symbol tick.
        if asset_type == AssetClass.IRS.value or asset_type not in _PRICED_FROM_TICK:
            return

        matching_trades = cache_service.simulator_trades(asset_type, symbol)

        for trade in matching_trades:
            try:
                self.price_trade(tick, trade, asset_type)
            except Exception as exc:
                logging.error(f"Error pricing trade {trade.trade_id}: {exc}")


simulator_pricing = SimulatorPricing()
