"""Async SQLAlchemy session for the trade-action request and worker paths.

The shared `DBSessionManager` in `shared.trading_shared.db` stays synchronous
for the other services. This service drives Postgres through an `AsyncSession`
so the routes and the worker task never block the event loop.
"""

import os

from sqlalchemy import select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import load_only

from shared.trading_shared.models import Instrument, MarketDataProviderQuote, Trade

_engine = None
_session_factory: async_sessionmaker | None = None


def _ensure_factory() -> async_sessionmaker:
    """Build the async engine on first use.

    Not built at import so a service load with a sqlite URL (the unit tests)
    does not try to open an async engine that would need `aiosqlite`.
    """
    global _engine, _session_factory
    if _session_factory is None:
        _engine = create_async_engine(
            os.getenv("DATABASE_URL"),
            pool_pre_ping=True,
            pool_recycle=1800,
        )
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _session_factory


def engine_created() -> bool:
    return _engine is not None


async def dispose() -> None:
    """Dispose the async engine on shutdown if it was ever opened."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None


class _InstrumentRepo:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def get_by_symbol(self, symbol):
        result = await self._session.execute(
            select(Instrument).where(Instrument.symbol == symbol).limit(1)
        )
        return result.scalars().first()

    def add(self, instrument) -> None:
        self._session.add(instrument)


class _ValuationRepo:
    def __init__(self, session: AsyncSession):
        self._session = session

    def add(self, valuation) -> None:
        self._session.add(valuation)


class _QuoteRepo:
    def __init__(self, session: AsyncSession):
        self._session = session

    def _latest(self, provider, symbol):
        return (
            select(MarketDataProviderQuote)
            .where(
                MarketDataProviderQuote.provider == provider,
                MarketDataProviderQuote.symbol == symbol,
            )
            .order_by(MarketDataProviderQuote.received_at.desc())
            .limit(1)
        )

    async def get_latest_freshness(self, provider, symbol):
        """Age check. Skips `raw_payload` and the bid/ask book."""
        stmt = self._latest(provider, symbol).options(
            load_only(
                MarketDataProviderQuote.asset_class,
                MarketDataProviderQuote.received_at,
                MarketDataProviderQuote.provider_timestamp,
            )
        )
        result = await self._session.execute(stmt)
        return result.scalars().first()

    async def get_latest_close_price(self, provider, symbol):
        """Close pricing. Skips `raw_payload`."""
        stmt = self._latest(provider, symbol).options(
            load_only(
                MarketDataProviderQuote.bid,
                MarketDataProviderQuote.ask,
                MarketDataProviderQuote.last,
                MarketDataProviderQuote.provider_timestamp,
                MarketDataProviderQuote.received_at,
            )
        )
        result = await self._session.execute(stmt)
        return result.scalars().first()


class AsyncDBSessionManager:
    """`async with` session that mirrors the slice of the sync manager this
    service uses, so open/close change by adding `await`, not by restructuring."""

    async def __aenter__(self) -> "AsyncDBSessionManager":
        self.session: AsyncSession = _ensure_factory()()
        self.instruments = _InstrumentRepo(self.session)
        self.valuations = _ValuationRepo(self.session)
        self.provider_quotes = _QuoteRepo(self.session)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None:
            await self.session.rollback()
        await self.session.close()

    async def execute(self, stmt):
        return await self.session.execute(stmt)

    async def lock_trade(self, trade_id):
        # FOR NO KEY UPDATE waits if another transaction holds the row, then the
        # status and close_client_request_id checks run under that lock.
        result = await self.session.execute(
            select(Trade).where(Trade.trade_id == trade_id).with_for_update(key_share=True)
        )
        return result.scalars().first()

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()

    async def flush(self) -> None:
        await self.session.flush()
