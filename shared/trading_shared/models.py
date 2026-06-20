import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, Text, Boolean, DateTime, Numeric, ForeignKey, BigInteger
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import declarative_base

# It registers all of the tables
Base = declarative_base()


def utc_now():
    return datetime.now(timezone.utc)


class Book(Base):
    __tablename__ = "books"

    book_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(Text, nullable=False, unique=True)
    description = Column(Text, nullable=True)
    expected_asset_class = Column(Text, nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = Column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )
    created_by = Column(Text, nullable=True)
    updated_by = Column(Text, nullable=True)


class Instrument(Base):
    __tablename__ = "instruments"

    instrument_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol = Column(Text, nullable=False)
    asset_class = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    multiplier = Column(Numeric, nullable=False, default=1)


class Trade(Base):
    __tablename__ = "trades"

    trade_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    book_id = Column(UUID(as_uuid=True), ForeignKey("books.book_id"), nullable=False)
    asset_class = Column(Text, nullable=False)
    instrument_id = Column(
        UUID(as_uuid=True), ForeignKey("instruments.instrument_id"), nullable=False
    )
    symbol = Column(Text, nullable=False)
    side = Column(Text, nullable=False)  # BUY / SELL
    quantity = Column(Numeric, nullable=False)
    trade_price = Column(Numeric, nullable=False)
    trade_currency = Column(Text, nullable=False)
    trade_date = Column(DateTime(timezone=True), nullable=False)
    status = Column(Text, nullable=False)  # ACTIVE / CLOSED / CANCELLED
    opened_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    closed_at = Column(DateTime(timezone=True), nullable=True)
    close_price = Column(Numeric, nullable=True)
    close_reason = Column(Text, nullable=True)
    source = Column(Text, nullable=False)  # GENERATED / MANUAL / SYSTEM
    client_request_id = Column(Text, nullable=True, unique=True)
    metadata_payload = Column(
        "metadata", JSONB, nullable=True
    )  # Słowo 'metadata' jest zarezerwowane w SQLAlchemy, więc używamy aliasu
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = Column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class Valuation(Base):
    __tablename__ = "valuations"

    valuation_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trade_id = Column(UUID(as_uuid=True), ForeignKey("trades.trade_id"), nullable=False)
    book_id = Column(UUID(as_uuid=True), ForeignKey("books.book_id"), nullable=False)
    asset_class = Column(Text, nullable=False)
    valuation_time = Column(DateTime(timezone=True), nullable=False)
    fair_value = Column(Numeric, nullable=False)
    market_value = Column(Numeric, nullable=True)
    unrealized_pnl = Column(Numeric, nullable=False, default=0)
    realized_pnl = Column(Numeric, nullable=False, default=0)
    total_pnl = Column(Numeric, nullable=False, default=0)
    currency = Column(Text, nullable=False)
    market_data_reference = Column(Text, nullable=True)
    valuation_payload = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)


class MarketDataSpotPrice(Base):
    __tablename__ = "market_data_spot_prices"

    market_data_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id = Column(BigInteger, nullable=True)
    symbol = Column(Text, nullable=False)
    asset_class = Column(Text, nullable=False)
    bid = Column(Numeric, nullable=True)
    ask = Column(Numeric, nullable=True)
    mid = Column(Numeric, nullable=True)
    last = Column(Numeric, nullable=True)
    spot = Column(Numeric, nullable=True)
    currency = Column(Text, nullable=True)
    source = Column(Text, nullable=False, default="SIMULATED")
    event_time = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    raw_payload = Column(JSONB, nullable=False)


class MarketDataCurve(Base):
    __tablename__ = "market_data_curves"

    curve_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id = Column(BigInteger, nullable=True)
    curve_name = Column(
        Text, nullable=False
    )  # YIELD_CURVE / FX_FORWARD_CURVE / DISCOUNT_CURVE
    curve_type = Column(Text, nullable=False)
    currency = Column(Text, nullable=True)
    tenors = Column(JSONB, nullable=False)  # e.g. ["1M", "3M", "1Y", "5Y"]
    rates = Column(JSONB, nullable=False)  # e.g. [0.041, 0.042, 0.044, 0.047]
    event_time = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    raw_payload = Column(JSONB, nullable=False)


class MarketDataSnapshot(Base):
    __tablename__ = "market_data_snapshots"

    snapshot_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id = Column(BigInteger, nullable=True)
    snapshot_type = Column(Text, nullable=False)  # FULL / SPOT / CURVE
    snapshot_time = Column(DateTime(timezone=True), nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
    payload = Column(JSONB, nullable=False)


class AuditLog(Base):
    __tablename__ = "audit_logs"

    audit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    service_name = Column(Text, nullable=False)
    event_type = Column(
        Text, nullable=False
    )  # CREATED / UPDATED / DELETED / ERROR / STREAM_CONNECTED etc.
    entity_type = Column(
        Text, nullable=True
    )  # TRADE / BOOK / VALUATION / MARKET_DATA etc.
    entity_id = Column(Text, nullable=True)
    correlation_id = Column(Text, nullable=True)
    severity = Column(Text, nullable=False)  # INFO / WARNING / ERROR
    message = Column(Text, nullable=False)
    payload = Column(JSONB, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utc_now)
