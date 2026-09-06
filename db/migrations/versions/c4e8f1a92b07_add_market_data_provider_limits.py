"""add market_data_provider_limits

Revision ID: c4e8f1a92b07
Revises: 8f3c2a91b047
Create Date: 2026-09-03 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect


revision: str = "c4e8f1a92b07"
down_revision: Union[str, Sequence[str], None] = "8f3c2a91b047"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    existing = set(sa_inspect(bind).get_table_names())
    if "market_data_provider_limits" in existing:
        return

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


def downgrade() -> None:
    bind = op.get_bind()
    existing = set(sa_inspect(bind).get_table_names())
    if "market_data_provider_limits" in existing:
        op.drop_table("market_data_provider_limits")
