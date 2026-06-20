from shared.trading_shared.enums import TradeStatus
from shared.trading_shared.models import (
    Instrument,
    MarketDataCurve,
    MarketDataSpotPrice,
    Trade,
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

    def get_trades(self, status=None, symbol=None, side=None, first_only=False):

        query = self.db_session.query(Trade)

        if status:
            query = query.filter(Trade.status == status)

        if symbol:
            query = query.filter(Trade.symbol == symbol)

        if side:
            query = query.filter(Trade.side == side)

        if first_only:
            return query.first()
        return query.all()

    def add(self, trade):
        self.db_session.add(trade)


class ValuationRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def add(self, valuation):
        self.db_session.add(valuation)


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


class MarketDataSpotPriceRepository:
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


class MarketDataCurveRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def get_curve(self, asset_class, symbol):
        return (
            self.db_session.query(MarketDataCurve)
            .filter(
                MarketDataCurve.asset_class == asset_class,
                MarketDataCurve.symbol == symbol,
            )
            .first()
        )
