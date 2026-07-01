from shared.trading_shared.enums import TradeStatus
from shared.trading_shared.models import (
    Instrument,
    MarketDataCurve,
    MarketDataSpotPrice,
    Trade,
    Valuation,
)


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
        return query.offset((page - 1) * limit).limit(limit).all()

    def add(self, trade):
        self.db_session.add(trade)


class ValuationRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def add(self, valuation):
        self.db_session.add(valuation)

    def get_valuations_by_trade_id(self, trade_id):
        return (
            self.db_session.query(Valuation)
            .filter(Valuation.trade_id == trade_id)
            .all()
        )


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

    def get_curve(self, asset_class, symbol):
        return (
            self.db_session.query(MarketDataCurve)
            .filter(
                MarketDataCurve.asset_class == asset_class,
                MarketDataCurve.symbol == symbol,
            )
            .first()
        )

    def add_all(self, market_data_list):
        self.db_session.add_all(market_data_list)

