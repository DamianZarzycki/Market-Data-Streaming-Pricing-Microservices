"""add trade reference price and market data provider

Revision ID: d1a9e6f3c521
Revises: c4e8b1a07f92
Create Date: 2026-09-10 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect


revision: str = "d1a9e6f3c521"
down_revision: Union[str, Sequence[str], None] = "c4e8b1a07f92"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()
    existing_columns = {col["name"] for col in sa_inspect(bind).get_columns("trades")}

    if "market_data_provider" not in existing_columns:
        op.add_column(
            "trades", sa.Column("market_data_provider", sa.Text(), nullable=True)
        )
    if "reference_price" not in existing_columns:
        op.add_column(
            "trades", sa.Column("reference_price", sa.Numeric(), nullable=True)
        )
    if "reference_price_timestamp" not in existing_columns:
        op.add_column(
            "trades",
            sa.Column(
                "reference_price_timestamp", sa.DateTime(timezone=True), nullable=True
            ),
        )


def downgrade() -> None:
    bind = op.get_bind()
    existing_columns = {col["name"] for col in sa_inspect(bind).get_columns("trades")}

    if "reference_price_timestamp" in existing_columns:
        op.drop_column("trades", "reference_price_timestamp")
    if "reference_price" in existing_columns:
        op.drop_column("trades", "reference_price")
    if "market_data_provider" in existing_columns:
        op.drop_column("trades", "market_data_provider")
