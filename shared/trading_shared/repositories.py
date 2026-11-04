from sqlalchemy import func, select
from sqlalchemy.orm import load_only

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

    # Async read: awaits AsyncSession.execute. Sync callers keep get_by_id.
    async def get_by_id_async(self, trade_id):
        result = await self.db_session.execute(
            select(Trade).where(Trade.trade_id == trade_id).limit(1)
        )
        return result.scalars().first()

    # Async read: same filters as get_trades, without FOR UPDATE.
    async def get_trades_async(
        self,
        client_request_id=None,
        book_id=None,
        asset_class=None,
        status=None,
        symbol=None,
        source=None,
        first_only=False,
        page=1,
        limit=50,
    ):
        statement = select(Trade)
        if client_request_id:
            statement = statement.where(Trade.client_request_id == client_request_id)
        if book_id:
            statement = statement.where(Trade.book_id == book_id)
        if asset_class:
            statement = statement.where(Trade.asset_class == asset_class)
        if status:
            statement = statement.where(Trade.status == status)
        if symbol:
            statement = statement.where(Trade.symbol == symbol)
        if source:
            statement = statement.where(Trade.source == source)
        if first_only:
            result = await self.db_session.execute(statement.limit(1))
            return result.scalars().first()
        _page = int(page) if page is not None else 1
        _limit = int(limit) if limit is not None else 50
        result = await self.db_session.execute(
            statement.order_by(Trade.created_at.desc())
            .offset((_page - 1) * _limit)
            .limit(_limit)
        )
        return result.scalars().all()

    def add(self, trade):
        self.db_session.add(trade)

    async def lock_trade(self, trade_id):
        """Lock the row for the rest of the transaction.

        FOR NO KEY UPDATE waits if another transaction holds the row, then the
        status and close_client_request_id checks run under that lock.
        """
        result = await self.db_session.execute(
            select(Trade).where(Trade.trade_id == trade_id).with_for_update(key_share=True)
        )
        return result.scalars().first()

    # Async read: awaits AsyncSession.execute. Sync services keep get_active_trades.
    async def list_active_for_pricing(self):
        """Every ACTIVE trade, with only the columns the pricer reads.

        get_trades paginates (default 50). Pricing has to mark the whole book.
        """
        statement = select(
            Trade.trade_id,
            Trade.book_id,
            Trade.symbol,
            Trade.asset_class,
            Trade.side,
            Trade.quantity,
            Trade.trade_price,
            Trade.trade_currency,
            Trade.metadata_payload,
            Trade.market_data_provider,
        ).where(Trade.status == TradeStatus.ACTIVE.value)
        result = await self.db_session.execute(statement)
        return result.all()


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

    # Async read: awaits AsyncSession.execute. Sync callers keep get_valuations_by_trade_id.
    async def get_valuations_by_trade_id_async(self, trade_id, limit=50):
        statement = (
            select(Valuation)
            .where(Valuation.trade_id == trade_id)
            .order_by(Valuation.valuation_time.desc())
        )
        if limit is not None:
            statement = statement.limit(int(limit))
        result = await self.db_session.execute(statement)
        return result.scalars().all()


class InstrumentRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def get_by_symbol(self, symbol):
        return (
            self.db_session.query(Instrument)
            .filter(Instrument.symbol == symbol)
            .first()
        )

    # Async read: awaits AsyncSession.execute. Sync services keep get_by_symbol.
    async def get_by_symbol_async(self, symbol):
        result = await self.db_session.execute(
            select(Instrument).where(Instrument.symbol == symbol).limit(1)
        )
        return result.scalars().first()

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

    def _curves_with_tenors_select(self):
        """Same tenor filter as _curves_with_tenors, for AsyncSession.execute."""
        return select(MarketDataCurve).where(
            func.jsonb_typeof(MarketDataCurve.tenors) == "array",
            func.jsonb_array_length(MarketDataCurve.tenors) > 0,
        )

    # Async read: awaits AsyncSession.execute. Sync services keep get_curve.
    async def get_curve_async(self, currency, curve_type, provider=None, exclude_providers=None):
        statement = self._curves_with_tenors_select().where(
            MarketDataCurve.currency == currency,
            MarketDataCurve.curve_type == curve_type,
        )
        if provider:
            statement = statement.where(MarketDataCurve.provider == provider)
        elif exclude_providers:
            statement = statement.where(~MarketDataCurve.provider.in_(exclude_providers))
        statement = statement.order_by(MarketDataCurve.as_of_time.desc()).limit(1)
        result = await self.db_session.execute(statement)
        return result.scalars().first()

    # Async read: awaits AsyncSession.execute. Sync services keep get_latest_curves_by_currency.
    async def get_latest_curves_by_currency_async(self, curve_type, exclude_providers=None):
        """Latest row per currency for a single curve_type (e.g. YIELD_CURVE)."""
        statement = self._curves_with_tenors_select().where(
            MarketDataCurve.curve_type == curve_type,
        )
        if exclude_providers:
            statement = statement.where(~MarketDataCurve.provider.in_(exclude_providers))
        statement = (
            statement.distinct(MarketDataCurve.currency)
            .order_by(
                MarketDataCurve.currency,
                MarketDataCurve.as_of_time.desc(),
            )
        )
        result = await self.db_session.execute(statement)
        return result.scalars().all()

    # Async read: awaits AsyncSession.execute. Sync services keep get_latest_curves.
    async def get_latest_curves_async(self, curve_type=None, exclude_providers=None):
        """Latest row per (provider, currency, curve_type)."""
        statement = self._curves_with_tenors_select()
        if curve_type:
            statement = statement.where(MarketDataCurve.curve_type == curve_type)
        if exclude_providers:
            statement = statement.where(~MarketDataCurve.provider.in_(exclude_providers))
        statement = (
            statement.distinct(
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
        )
        result = await self.db_session.execute(statement)
        return result.scalars().all()

    # Async read: awaits AsyncSession.execute. Sync services keep get_latest_for_provider.
    async def get_latest_for_provider_async(self, provider):
        statement = (
            self._curves_with_tenors_select()
            .where(MarketDataCurve.provider == provider)
            .order_by(MarketDataCurve.as_of_time.desc())
            .limit(1)
        )
        result = await self.db_session.execute(statement)
        return result.scalars().first()

    def add_all(self, market_data_list):
        logging.info(f"Attempt to add {len(market_data_list)} market data records to DB")
        self.db_session.add_all(market_data_list)


class MarketDataProviderLimitsRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def list_all(self):
        return self.db_session.query(MarketDataProviderLimits).all()

    # Async read: awaits AsyncSession.execute. Sync services keep list_all.
    async def list_all_async(self):
        result = await self.db_session.execute(select(MarketDataProviderLimits))
        return result.scalars().all()


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

    # Async read: awaits AsyncSession.execute. Sync services keep list_since.
    async def list_since_async(self, since):
        statement = (
            select(MarketDataProviderRequest)
            .where(MarketDataProviderRequest.requested_at >= since)
        )
        result = await self.db_session.execute(statement)
        return result.scalars().all()


class MarketDataProviderQuoteRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def add(self, quote: MarketDataProviderQuote):
        self.db_session.add(quote)

    def _latest_query(self, provider, symbol):
        return (
            self.db_session.query(MarketDataProviderQuote)
            .filter(
                MarketDataProviderQuote.provider == provider,
                MarketDataProviderQuote.symbol == symbol,
            )
            .order_by(MarketDataProviderQuote.received_at.desc())
        )

    def get_latest(self, provider, symbol):
        """Full quote row, including `raw_payload`. Snapshot reads use this."""
        return self._latest_query(provider, symbol).first()

    def get_latest_freshness(self, provider, symbol):
        """Age check. Skips `raw_payload` and the bid/ask book."""
        return (
            self._latest_query(provider, symbol)
            .options(
                load_only(
                    MarketDataProviderQuote.asset_class,
                    MarketDataProviderQuote.received_at,
                    MarketDataProviderQuote.provider_timestamp,
                )
            )
            .first()
        )

    def get_latest_close_price(self, provider, symbol):
        """Close pricing. Skips `raw_payload`."""
        return (
            self._latest_query(provider, symbol)
            .options(
                load_only(
                    MarketDataProviderQuote.bid,
                    MarketDataProviderQuote.ask,
                    MarketDataProviderQuote.last,
                    MarketDataProviderQuote.provider_timestamp,
                    MarketDataProviderQuote.received_at,
                )
            )
            .first()
        )

    def _latest_statement(self, provider, symbol):
        return (
            select(MarketDataProviderQuote)
            .where(
                MarketDataProviderQuote.provider == provider,
                MarketDataProviderQuote.symbol == symbol,
            )
            .order_by(MarketDataProviderQuote.received_at.desc())
            .limit(1)
        )

    # Async read: awaits AsyncSession.execute. Sync services keep get_latest_freshness.
    async def get_latest_freshness_async(self, provider, symbol):
        """Age check. Skips `raw_payload` and the bid/ask book."""
        statement = self._latest_statement(provider, symbol).options(
            load_only(
                MarketDataProviderQuote.asset_class,
                MarketDataProviderQuote.received_at,
                MarketDataProviderQuote.provider_timestamp,
            )
        )
        result = await self.db_session.execute(statement)
        return result.scalars().first()

    # Async read: awaits AsyncSession.execute. Sync services keep get_latest_close_price.
    async def get_latest_close_price_async(self, provider, symbol):
        """Close pricing. Skips `raw_payload`."""
        statement = self._latest_statement(provider, symbol).options(
            load_only(
                MarketDataProviderQuote.bid,
                MarketDataProviderQuote.ask,
                MarketDataProviderQuote.last,
                MarketDataProviderQuote.provider_timestamp,
                MarketDataProviderQuote.received_at,
            )
        )
        result = await self.db_session.execute(statement)
        return result.scalars().first()

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

    # Async read: awaits AsyncSession.execute. Sync services keep get_latest.
    async def get_latest_async(self, provider, symbol):
        statement = (
            select(MarketDataProviderQuote)
            .where(
                MarketDataProviderQuote.provider == provider,
                MarketDataProviderQuote.symbol == symbol,
            )
            .order_by(MarketDataProviderQuote.received_at.desc())
            .limit(1)
        )
        result = await self.db_session.execute(statement)
        return result.scalars().first()

    # Async read: awaits AsyncSession.execute. Sync services keep get_latest_for_symbol.
    async def get_latest_for_symbol_async(self, symbol, asset_class):
        statement = (
            select(MarketDataProviderQuote)
            .where(
                MarketDataProviderQuote.symbol == symbol,
                MarketDataProviderQuote.asset_class == asset_class,
            )
            .distinct(MarketDataProviderQuote.provider)
            .order_by(
                MarketDataProviderQuote.provider,
                MarketDataProviderQuote.received_at.desc(),
            )
        )
        result = await self.db_session.execute(statement)
        return result.scalars().all()

    # Async read: awaits AsyncSession.execute. Sync services keep get_all_latest.
    async def get_all_latest_async(self):
        """Latest quote per (provider, symbol). Backs GET /market-data/snapshot."""
        statement = (
            select(MarketDataProviderQuote)
            .distinct(
                MarketDataProviderQuote.provider,
                MarketDataProviderQuote.symbol,
            )
            .order_by(
                MarketDataProviderQuote.provider,
                MarketDataProviderQuote.symbol,
                MarketDataProviderQuote.received_at.desc(),
            )
        )
        result = await self.db_session.execute(statement)
        return result.scalars().all()


class MarketDataSnapshotRepository:
    def __init__(self, db_session):
        self.db_session = db_session

    def add(self, snapshot: MarketDataSnapshot):
        self.db_session.add(snapshot)

