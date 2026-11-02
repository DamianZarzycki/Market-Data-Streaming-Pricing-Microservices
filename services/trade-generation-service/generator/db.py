"""Async SQLAlchemy session for the close read and the worker audits.

The shared `DBSessionManager` stays synchronous for the other services. This
engine is built on first use so importing the service does not open a pool.
The generation lock allows one query at a time, so the pool stays at one
connection plus one overflow.
"""

import os

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

_engine = None
_session_factory: async_sessionmaker | None = None


def _ensure_factory() -> async_sessionmaker:
    global _engine, _session_factory
    if _session_factory is None:
        _engine = create_async_engine(
            os.getenv("DATABASE_URL"),
            pool_size=1,
            max_overflow=1,
            pool_pre_ping=True,
            pool_recycle=1800,
        )
        _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    return _session_factory


class session_scope:
    """One short async session. Close runs even when rollback fails."""

    async def __aenter__(self) -> AsyncSession:
        self.session = _ensure_factory()()
        return self.session

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            if exc_type is not None:
                await self.session.rollback()
        finally:
            await self.session.close()


async def dispose() -> None:
    """Dispose the async engine on shutdown if it was ever opened."""
    global _engine, _session_factory
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None
