import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import Text, Boolean, DateTime, Numeric, ForeignKey, BigInteger, Index, Integer
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# It registers all of the tables
class Base(DeclarativeBase):
    pass


def utc_now():
    return datetime.now(timezone.utc)


class Book(Base):
    __tablename__ = "books"

    book_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    expected_asset_class: Mapped[str] = mapped_column(Text, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )
    created_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    assigned_to: Mapped[str | None] = mapped_column(Text, nullable=True)


class Instrument(Base):
    __tablename__ = "instruments"

    instrument_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    synthetic: Mapped[bool | None] = mapped_column(Boolean, nullable=True, default=False)
    asset_class: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    multiplier: Mapped[Decimal] = mapped_column(Numeric, nullable=False, default=1)


class Trade(Base):
    __tablename__ = "trades"

    trade_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    book_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("books.book_id"), nullable=False)
    asset_class: Mapped[str] = mapped_column(Text, nullable=False)
    instrument_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instruments.instrument_id"), nullable=False
    )
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    side: Mapped[str] = mapped_column(Text, nullable=False)  # BUY / SELL
    quantity: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    trade_price: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    trade_currency: Mapped[str] = mapped_column(Text, nullable=False)
    trade_date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)  # ACTIVE / CLOSED / CANCELLED
    opened_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    close_price: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    close_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)  # GENERATED / TRADING_TICKET
    market_data_provider: Mapped[str | None] = mapped_column(Text, nullable=True)
    reference_price: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    reference_price_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    client_request_id: Mapped[str | None] = mapped_column(Text, nullable=True, unique=True)
    # Słowo 'metadata' jest zarezerwowane w SQLAlchemy, więc używamy aliasu
    metadata_payload: Mapped[dict[str, Any] | None] = mapped_column("metadata", JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )

    __table_args__ = (
        Index("ix_trades_book_id_status", "book_id", "status"),
    )


class Valuation(Base):
    __tablename__ = "valuations"

    valuation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trade_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("trades.trade_id"), nullable=False)
    book_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("books.book_id"), nullable=False)
    asset_class: Mapped[str] = mapped_column(Text, nullable=False)
    valuation_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    fair_value: Mapped[Decimal] = mapped_column(Numeric, nullable=False)
    market_value: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    unrealized_pnl: Mapped[Decimal] = mapped_column(Numeric, nullable=False, default=0)
    realized_pnl: Mapped[Decimal] = mapped_column(Numeric, nullable=False, default=0)
    total_pnl: Mapped[Decimal] = mapped_column(Numeric, nullable=False, default=0)
    currency: Mapped[str] = mapped_column(Text, nullable=False)
    market_data_provider: Mapped[str | None] = mapped_column(Text, nullable=True)
    market_data_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    market_data_reference: Mapped[str | None] = mapped_column(Text, nullable=True)
    valuation_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)


class MarketDataProviderLimits(Base):
    __tablename__ = "market_data_provider_limits"

    provider: Mapped[str] = mapped_column(Text, primary_key=True)
    minute_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    hourly_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    daily_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    monthly_limit: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utc_now, onupdate=utc_now
    )


class MarketDataProviderRequest(Base):
    __tablename__ = "market_data_provider_requests"

    request_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str | None] = mapped_column(Text, nullable=True)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    __table_args__ = (
        Index("ix_provider_requests_provider_requested_at", "provider", "requested_at"),
    )


class MarketDataProviderQuote(Base):
    __tablename__ = "market_data_provider_quotes"

    quote_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    asset_class: Mapped[str] = mapped_column(Text, nullable=False)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    bid: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    ask: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    last: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    currency: Mapped[str | None] = mapped_column(Text, nullable=True)
    provider_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)


class MarketDataSpotPrice(Base):
    __tablename__ = "market_data_spot_prices"

    market_data_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    asset_class: Mapped[str] = mapped_column(Text, nullable=False)
    bid: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    ask: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    mid: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    last: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    spot: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    currency: Mapped[str | None] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(Text, nullable=False, default="SIMULATED")
    event_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class MarketDataCurve(Base):
    __tablename__ = "market_data_curves"

    curve_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    # YIELD_CURVE / FX_FORWARD_CURVE / DISCOUNT_CURVE
    curve_name: Mapped[str] = mapped_column(Text, nullable=False)
    curve_type: Mapped[str] = mapped_column(Text, nullable=False)
    currency: Mapped[str | None] = mapped_column(Text, nullable=True)
    tenors: Mapped[Any] = mapped_column(JSONB, nullable=False)  # e.g. ["1M", "3M", "1Y", "5Y"]
    rates: Mapped[Any] = mapped_column(JSONB, nullable=False)  # e.g. [0.041, 0.042, 0.044, 0.047]
    as_of_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class MarketDataSnapshot(Base):
    __tablename__ = "market_data_snapshots"

    snapshot_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    event_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    snapshot_type: Mapped[str] = mapped_column(Text, nullable=False)  # FULL / QUOTE / CURVE
    snapshot_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    # NULL for the old simulator's whole-state FULL dumps; populated for
    # per-provider QUOTE/CURVE snapshots written by market-data-service-integration.
    provider: Mapped[str | None] = mapped_column(Text, nullable=True)
    symbol_or_curve_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    normalized_payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)  # raw payload


class AuditLog(Base):
    __tablename__ = "audit_logs"

    audit_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    service_name: Mapped[str] = mapped_column(Text, nullable=False)
    # CREATED / UPDATED / DELETED / ERROR / STREAM_CONNECTED etc.
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    # TRADE / BOOK / VALUATION / MARKET_DATA etc.
    entity_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    entity_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    severity: Mapped[str] = mapped_column(Text, nullable=False)  # INFO / WARNING / ERROR
    message: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    __table_args__ = (
        Index("ix_audit_logs_created_at", "created_at"),
    )
