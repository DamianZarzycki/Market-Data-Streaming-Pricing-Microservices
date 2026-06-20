import os
from shared.trading_shared.repositories import TradeRepository, ValuationRepository
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

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
        self.trades = TradeRepository(self.session)
        self.valuations = ValuationRepository(self.session)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        self.session.close()

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()