import os
from typing import TYPE_CHECKING

from shared.trading_shared.repositories import (
    AuditRepository,
    InstrumentRepository,
    MarketDataProviderLimitsRepository,
    MarketDataProviderQuoteRepository,
    MarketDataProviderRequestRepository,
    MarketDataRepository,
    MarketDataSnapshotRepository,
    TradeRepository,
    ValuationRepository,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

DATABASE_URL = os.getenv("DATABASE_URL")

# pool_pre_ping discards a connection Postgres has already closed. pool_recycle
# replaces a connection before a server-side idle timeout does. Pool size stays
# at the SQLAlchemy default: eight services already sit near max_connections.
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
)

SessionLocal = sessionmaker(
    # more control over changes that are pending to be written to the database, allowing for better transaction management and error handling.
    autocommit=False,
    autoflush=False,
    bind=engine)


class DBSessionManager:
    def __init__(self):
        self.session_factory = SessionLocal

    def __enter__(self):
        self.session = self.session_factory()
        self.audit = AuditRepository(self.session)
        self.trades = TradeRepository(self.session)
        self.valuations = ValuationRepository(self.session)
        self.instruments = InstrumentRepository(self.session)
        self.market_data = MarketDataRepository(self.session)
        self.provider_quotes = MarketDataProviderQuoteRepository(self.session)
        self.provider_limits = MarketDataProviderLimitsRepository(self.session)
        self.provider_requests = MarketDataProviderRequestRepository(self.session)
        self.market_data_snapshots = MarketDataSnapshotRepository(self.session)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        self.session.close()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def close(self):
        self.session.close()

    def flush(self):
        self.session.flush()


_async_engine = None
_async_sessionmaker = None


def _async_session_factory():
    """Build the async engine on first use so other services can import this module without greenlet."""
    global _async_engine, _async_sessionmaker
    if _async_sessionmaker is None:
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        _async_engine = create_async_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_recycle=1800,
        )
        _async_sessionmaker = async_sessionmaker(
            _async_engine,
            expire_on_commit=False,
            autocommit=False,
            autoflush=False,
        )
    return _async_sessionmaker


async def dispose_async_engine() -> None:
    global _async_engine, _async_sessionmaker
    if _async_engine is None:
        return
    await _async_engine.dispose()
    _async_engine = None
    _async_sessionmaker = None


class AsyncDBSessionManager:
    """Async counterpart of DBSessionManager. add() only stages a row; commit() is awaited."""

    session: "AsyncSession"

    async def __aenter__(self) -> "AsyncDBSessionManager":
        self.session = _async_session_factory()()
        self.audit = AuditRepository(self.session)
        self.trades = TradeRepository(self.session)
        self.valuations = ValuationRepository(self.session)
        self.instruments = InstrumentRepository(self.session)
        self.market_data = MarketDataRepository(self.session)
        self.provider_quotes = MarketDataProviderQuoteRepository(self.session)
        self.provider_limits = MarketDataProviderLimitsRepository(self.session)
        self.provider_requests = MarketDataProviderRequestRepository(self.session)
        self.market_data_snapshots = MarketDataSnapshotRepository(self.session)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is not None:
                await self.rollback()
        finally:
            await self.session.close()

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()

    async def close(self):
        await self.session.close()
