from collections.abc import AsyncIterator, Iterator
from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from shared.trading_shared.env import env
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

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

# pool_pre_ping discards a connection Postgres has already closed. pool_recycle
# replaces a connection before a server-side idle timeout does. Pool size stays
# at the SQLAlchemy default: eight services already sit near max_connections.
_POOL_RECYCLE_SECONDS = 1800

_sync_engine = None
_sync_sessionmaker = None
_async_engine = None
_async_sessionmaker = None


def _sync_session_factory():
    """Build the sync engine on first checkout.

    Importing this module, including `AsyncDBSessionManager`, must not call
    `create_engine`. Async services never open this pool.
    """
    global _sync_engine, _sync_sessionmaker
    if _sync_sessionmaker is None:
        _sync_engine = create_engine(
            env("DATABASE_URL"),
            pool_pre_ping=True,
            pool_recycle=_POOL_RECYCLE_SECONDS,
        )
        _sync_sessionmaker = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=_sync_engine,
        )
    return _sync_sessionmaker


class _LazySessionLocal:
    """`SessionLocal()` checks out a sync session and builds the engine once."""

    def __call__(self):
        return _sync_session_factory()()


SessionLocal = _LazySessionLocal()


def __getattr__(name: str):
    # `from shared.trading_shared.db import engine` is the only sync-engine
    # import. Attribute access builds the pool; a plain import of this module does not.
    if name == "engine":
        _sync_session_factory()
        return _sync_engine
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _bind_repositories(manager, session) -> None:
    manager.session = session
    manager.audit = AuditRepository(session)
    manager.trades = TradeRepository(session)
    manager.valuations = ValuationRepository(session)
    manager.instruments = InstrumentRepository(session)
    manager.market_data = MarketDataRepository(session)
    manager.provider_quotes = MarketDataProviderQuoteRepository(session)
    manager.provider_limits = MarketDataProviderLimitsRepository(session)
    manager.provider_requests = MarketDataProviderRequestRepository(session)
    manager.market_data_snapshots = MarketDataSnapshotRepository(session)


class DBSessionManager:
    def __enter__(self):
        _bind_repositories(self, SessionLocal())
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


def get_db() -> Iterator[Session]:
    """One transaction per request. Commit on success, rollback and close on failure.

    A session left open stays idle in a transaction and Postgres cannot vacuum.
    """
    db = SessionLocal()
    try:
        yield db
        if db.in_transaction():
            db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


async def get_async_db() -> AsyncIterator["AsyncSession"]:
    """One transaction per request on the async engine.

    Commit on success, rollback and close on failure. The sync `get_db` stays
    for callers that still check out a blocking session.
    """
    db = _async_session_factory()()
    try:
        yield db
        if db.in_transaction():
            await db.commit()
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.close()


def _async_session_factory():
    """Build the async engine on first use so other services can import this module without greenlet."""
    global _async_engine, _async_sessionmaker
    if _async_sessionmaker is None:
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        _async_engine = create_async_engine(
            env("DATABASE_URL"),
            pool_pre_ping=True,
            pool_recycle=_POOL_RECYCLE_SECONDS,
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
        _bind_repositories(self, _async_session_factory()())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is not None:
                await self.rollback()
        finally:
            await self.session.close()

    async def execute(self, statement):
        return await self.session.execute(statement)

    async def commit(self):
        await self.session.commit()

    async def rollback(self):
        await self.session.rollback()

    async def close(self):
        await self.session.close()

    async def flush(self):
        await self.session.flush()
