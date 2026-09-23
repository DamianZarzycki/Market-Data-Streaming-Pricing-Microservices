from sqlalchemy import func

from shared.trading_shared.enums import TradeStatus
from shared.trading_shared.models import (
    AuditLog,
    Instrument,
    MarketDataCurve,
    MarketDataProviderLimits,
    MarketDataProviderQuote,
    MarketDataProviderRequest,
    MarketDataSnapshot,
    MarketDataSpotPrice,
    Trade,
    Valuation,
)
import logging

class AuditRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def add(self, audit_log: AuditLog):
        self.db_session.add(audit_log)


class TradeRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def get_by_id(self, trade_id):
        return self.db_session.query(Trade).filter(Trade.trade_id == trade_id).first()

    def get_active_trades(self):
        return (
            self.db_session.query(Trade)
            .filter(Trade.status == TradeStatus.ACTIVE.value)
            .all()
        )

    def get_trades(self, client_request_id=None, book_id=None, asset_class=None, status=None, symbol=None, source=None, first_only=False, page=1, limit=50):
        query = self.db_session.query(Trade)

        if client_request_id:
            query = query.filter(Trade.client_request_id == client_request_id)

        if book_id:
            query = query.filter(Trade.book_id == book_id)

        if asset_class:
            query = query.filter(Trade.asset_class == asset_class)

        if status:
            query = query.filter(Trade.status == status)

        if symbol:
            query = query.filter(Trade.symbol == symbol)

        if source:
            query = query.filter(Trade.source == source)

        if first_only:
            return query.first()
        _page = int(page) if page is not None else 1
        _limit = int(limit) if limit is not None else 50
        # Read-only listing must not take FOR UPDATE locks — that stalls the blotter UI.
        return (
            query.order_by(Trade.created_at.desc())
            .offset((_page - 1) * _limit)
            .limit(_limit)
            .all()
        )

    def add(self, trade):
        self.db_session.add(trade)


class ValuationRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def add(self, valuation):
        self.db_session.add(valuation)

    def add_all(self, valuations):
        self.db_session.add_all(valuations)

    def get_valuations_by_trade_id(self, trade_id, limit=50):
        query = (
            self.db_session.query(Valuation)
            .filter(Valuation.trade_id == trade_id)
            .order_by(Valuation.valuation_time.desc())
        )
        if limit is not None:
            query = query.limit(int(limit))
        return query.all()


class InstrumentRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def get_by_symbol(self, symbol):
        return (
            self.db_session.query(Instrument)
            .filter(Instrument.symbol == symbol)
            .first()
        )

    def add(self, instrument):
        self.db_session.add(instrument)


class MarketDataRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def get_spot_price(self, asset_class, symbol):
        return (
            self.db_session.query(MarketDataSpotPrice)
            .filter(
                MarketDataSpotPrice.asset_class == asset_class,
                MarketDataSpotPrice.symbol == symbol,
            )
            .first()
        )
        

    def get_curve(self, currency, curve_type, provider=None, exclude_providers=None):
        query = self._curves_with_tenors().filter(
            MarketDataCurve.currency == currency,
            MarketDataCurve.curve_type == curve_type,
        )
        if provider:
            query = query.filter(MarketDataCurve.provider == provider)
        elif exclude_providers:
            query = query.filter(~MarketDataCurve.provider.in_(exclude_providers))
        return query.order_by(MarketDataCurve.as_of_time.desc()).first()

    def get_latest_curves(self, curve_type=None, exclude_providers=None):
        """Latest row per (provider, currency, curve_type)."""
        query = self._curves_with_tenors()
        if curve_type:
            query = query.filter(MarketDataCurve.curve_type == curve_type)
        if exclude_providers:
            query = query.filter(~MarketDataCurve.provider.in_(exclude_providers))
        return (
            query.distinct(
                MarketDataCurve.provider,
                MarketDataCurve.currency,
                MarketDataCurve.curve_type,
            )
            .order_by(
                MarketDataCurve.provider,
                MarketDataCurve.currency,
                MarketDataCurve.curve_type,
                MarketDataCurve.as_of_time.desc(),
            )
            .all()
        )

    def get_latest_curves_by_currency(self, curve_type, exclude_providers=None):
        """Latest row per currency for a single curve_type (e.g. YIELD_CURVE)."""
        query = self._curves_with_tenors().filter(
            MarketDataCurve.curve_type == curve_type,
        )
        if exclude_providers:
            query = query.filter(~MarketDataCurve.provider.in_(exclude_providers))
        return (
            query.distinct(MarketDataCurve.currency)
            .order_by(
                MarketDataCurve.currency,
                MarketDataCurve.as_of_time.desc(),
            )
            .all()
        )

    def get_latest_for_provider(self, provider):
        return (
            self._curves_with_tenors()
            .filter(MarketDataCurve.provider == provider)
            .order_by(MarketDataCurve.as_of_time.desc())
            .first()
        )

    def _curves_with_tenors(self):
        """Empty tenor lists are failed fetches, not a curve a ticket can book."""
        return self.db_session.query(MarketDataCurve).filter(
            func.jsonb_typeof(MarketDataCurve.tenors) == "array",
            func.jsonb_array_length(MarketDataCurve.tenors) > 0,
        )

    def add_all(self, market_data_list):
        logging.info(f"Attempt to add {len(market_data_list)} market data records to DB")
        self.db_session.add_all(market_data_list)


class MarketDataProviderLimitsRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def list_all(self):
        return self.db_session.query(MarketDataProviderLimits).all()


class MarketDataProviderRequestRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def add(self, request: MarketDataProviderRequest):
        self.db_session.add(request)

    def list_since(self, since):
        return (
            self.db_session.query(MarketDataProviderRequest)
            .filter(MarketDataProviderRequest.requested_at >= since)
            .all()
        )


class MarketDataProviderQuoteRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def add(self, quote: MarketDataProviderQuote):
        self.db_session.add(quote)

    def get_latest(self, provider, symbol):
        return (
            self.db_session.query(MarketDataProviderQuote)
            .filter(
                MarketDataProviderQuote.provider == provider,
                MarketDataProviderQuote.symbol == symbol,
            )
            .order_by(MarketDataProviderQuote.received_at.desc())
            .first()
        )

    def get_latest_for_symbol(self, symbol, asset_class):
        return (
            self.db_session.query(MarketDataProviderQuote)
            .filter(
                MarketDataProviderQuote.symbol == symbol,
                MarketDataProviderQuote.asset_class == asset_class,
            )
            .distinct(MarketDataProviderQuote.provider)
            .order_by(
                MarketDataProviderQuote.provider,
                MarketDataProviderQuote.received_at.desc(),
            )
            .all()
        )

    def get_all_latest(self):
        """Latest quote per (provider, symbol) across everything on file —
        used to build GET /market-data/snapshot."""
        return (
            self.db_session.query(MarketDataProviderQuote)
            .distinct(
                MarketDataProviderQuote.provider,
                MarketDataProviderQuote.symbol,
            )
            .order_by(
                MarketDataProviderQuote.provider,
                MarketDataProviderQuote.symbol,
                MarketDataProviderQuote.received_at.desc(),
            )
            .all()
        )


class MarketDataSnapshotRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def add(self, snapshot: MarketDataSnapshot):
        self.db_session.add(snapshot)

