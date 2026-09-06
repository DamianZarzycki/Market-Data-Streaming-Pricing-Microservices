"""add provider requests (limits table already exists from c4e8f1a92b07)

Revision ID: c4e8b1a07f92
Revises: c4e8f1a92b07
Create Date: 2026-09-03 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy import inspect as sa_inspect


revision: str = "c4e8b1a07f92"
down_revision: Union[str, Sequence[str], None] = "c4e8f1a92b07"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    existing = set(sa_inspect(bind).get_table_names())

    if "market_data_provider_limits" not in existing:
        op.create_table(
            "market_data_provider_limits",
            sa.Column("provider", sa.Text(), nullable=False),
            sa.Column("minute_limit", sa.Integer(), nullable=True),
            sa.Column("hourly_limit", sa.Integer(), nullable=True),
            sa.Column("daily_limit", sa.Integer(), nullable=True),
            sa.Column("monthly_limit", sa.Integer(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("provider"),
        )

    if "market_data_provider_requests" not in existing:
        op.create_table(
            "market_data_provider_requests",
            sa.Column("request_id", postgresql.UUID(as_uuid=True), nullable=False),
            sa.Column("provider", sa.Text(), nullable=False),
            sa.Column("symbol", sa.Text(), nullable=True),
            sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
            sa.PrimaryKeyConstraint("request_id"),
        )
        op.create_index(
            "ix_provider_requests_provider_requested_at",
            "market_data_provider_requests",
            ["provider", "requested_at"],
        )


def downgrade() -> None:
    bind = op.get_bind()
    existing = set(sa_inspect(bind).get_table_names())
    if "market_data_provider_requests" in existing:
        op.drop_index(
            "ix_provider_requests_provider_requested_at",
            table_name="market_data_provider_requests",
        )
        op.drop_table("market_data_provider_requests")
    if "market_data_provider_limits" in existing:
        op.drop_table("market_data_provider_limits")
