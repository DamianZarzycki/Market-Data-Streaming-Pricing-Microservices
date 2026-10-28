import os
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
