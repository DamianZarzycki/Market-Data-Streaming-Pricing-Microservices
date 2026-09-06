from shared.trading_shared.enums import TradeStatus
from shared.trading_shared.models import (
    AuditLog,
    Instrument,
    MarketDataCurve,
    MarketDataProviderLimits,
    MarketDataProviderQuote,
    MarketDataProviderRequest,
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

    def get_trades(self, client_request_id=None, book_id=None, asset_class=None, status=None, symbol=None, first_only=False, page=1, limit=50):
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
        

    def get_curve(self, currency, curve_type):
        return (
            self.db_session.query(MarketDataCurve)
            .filter(
                MarketDataCurve.currency == currency,
                MarketDataCurve.curve_type == curve_type,
            )
            .first()
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

