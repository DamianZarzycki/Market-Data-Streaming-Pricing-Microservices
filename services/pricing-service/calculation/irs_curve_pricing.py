import logging
from datetime import datetime

from instruments_pricing.irs_pricing_service import price_irs

from shared.trading_shared.db import AsyncDBSessionManager
from shared.trading_shared.enums import AssetClass, CurveType, SIMULATED_PROVIDER

import cache_service
from calculation.formulas import current_timestamp
from calculation.valuation_feed import valuation_feed
from calculation.valuation_state import valuation_state


class IrsCurvePricing:
    """Marks IRS trades from persisted yield curves."""

    def price_trade(self, trade, curve):
        params = trade.metadata_payload or {}
        required = ("notional", "fixed_rate", "maturity_years", "payments_per_year", "direction")
        missing = [key for key in required if params.get(key) is None]
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
                "as_of_time": curve.get("as_of_time"),
            },
        }

        valuation_feed.store_and_publish(trade, valuation_data)

    def _curve_payload_from_row(self, curve_row):
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

    def reprice_for_currency(self, currency, curve):
        irs_trades = [
            trade
            for trade in cache_service.all_trades()
            if trade.asset_class == AssetClass.IRS.value and trade.trade_currency == currency
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
                self.price_trade(trade, curve)
            except Exception as exc:
                logging.error(f"Error pricing IRS trade {trade.trade_id}: {exc}")

    # Async: awaits MarketDataRepository.get_curve_async (AsyncSession.execute).
    async def update_from_tick(self, tick):
        currency = tick.get("currency")
        curve_type = tick.get("curve_type")
        if not currency or not curve_type:
            logging.warning(f"Ignoring curve tick missing currency/curve_type: {tick}")
            return
        if curve_type != CurveType.YIELD_CURVE.value:
            return
        if valuation_state.has_external_yield_curve(currency):
            return

        async with AsyncDBSessionManager() as db:
            curve_row = await db.market_data.get_curve_async(
                currency,
                curve_type,
                provider=tick.get("provider") or SIMULATED_PROVIDER,
            )
            payload = self._curve_payload_from_row(curve_row) if curve_row is not None else None
            row_currency = curve_row.currency if curve_row is not None else None

        if payload is None:
            logging.warning(
                f"No {currency} {curve_type} curve in MarketDataCurve yet; skipping IRS repricing"
            )
            return

        self.reprice_for_currency(row_currency, payload)

    # Async: awaits MarketDataRepository.get_latest_curves_by_currency_async.
    async def reprice_persisted(self):
        """Mark IRS from the latest non-simulated YIELD_CURVE per currency (FRED/ECB)."""
        async with AsyncDBSessionManager() as db:
            curve_rows = await db.market_data.get_latest_curves_by_currency_async(
                CurveType.YIELD_CURVE.value,
                exclude_providers=[SIMULATED_PROVIDER],
            )
            payloads = [
                (curve_row.currency, self._curve_payload_from_row(curve_row))
                for curve_row in curve_rows
            ]

        valuation_state.set_external_yield_currencies({currency for currency, _payload in payloads})
        for currency, payload in payloads:
            self.reprice_for_currency(currency, payload)


irs_curve_pricing = IrsCurvePricing()
