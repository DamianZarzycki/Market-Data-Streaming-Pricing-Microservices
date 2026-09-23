"""add book assigned_to, valuation provider/timestamp, market data snapshot fields

Revision ID: f3a7c9e1b204
Revises: 8835118a786c
Create Date: 2026-09-19 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.dialects import postgresql


revision: str = "f3a7c9e1b204"
down_revision: Union[str, Sequence[str], None] = "8835118a786c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    bind = op.get_bind()

    books_columns = {col["name"] for col in sa_inspect(bind).get_columns("books")}
    if "assigned_to" not in books_columns:
        op.add_column("books", sa.Column("assigned_to", sa.Text(), nullable=True))

    valuations_columns = {col["name"] for col in sa_inspect(bind).get_columns("valuations")}
    if "provider" in valuations_columns:
        op.drop_column("valuations", "provider")
    if "market_data_provider" not in valuations_columns:
        op.add_column(
            "valuations", sa.Column("market_data_provider", sa.Text(), nullable=True)
        )
    if "market_data_timestamp" not in valuations_columns:
        op.add_column(
            "valuations",
            sa.Column("market_data_timestamp", sa.DateTime(timezone=True), nullable=True),
        )

    snapshot_columns = {
        col["name"] for col in sa_inspect(bind).get_columns("market_data_snapshots")
    }
    if "provider" not in snapshot_columns:
        op.add_column(
            "market_data_snapshots", sa.Column("provider", sa.Text(), nullable=True)
        )
    if "symbol_or_curve_name" not in snapshot_columns:
        op.add_column(
            "market_data_snapshots",
            sa.Column("symbol_or_curve_name", sa.Text(), nullable=True),
        )
    if "normalized_payload" not in snapshot_columns:
        op.add_column(
            "market_data_snapshots",
            sa.Column(
                "normalized_payload",
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=True,
            ),
        )


def downgrade() -> None:
    bind = op.get_bind()

    snapshot_columns = {
        col["name"] for col in sa_inspect(bind).get_columns("market_data_snapshots")
    }
    if "normalized_payload" in snapshot_columns:
        op.drop_column("market_data_snapshots", "normalized_payload")
    if "symbol_or_curve_name" in snapshot_columns:
        op.drop_column("market_data_snapshots", "symbol_or_curve_name")
    if "provider" in snapshot_columns:
        op.drop_column("market_data_snapshots", "provider")

    valuations_columns = {col["name"] for col in sa_inspect(bind).get_columns("valuations")}
    if "market_data_timestamp" in valuations_columns:
        op.drop_column("valuations", "market_data_timestamp")
    if "market_data_provider" in valuations_columns:
        op.drop_column("valuations", "market_data_provider")

    books_columns = {col["name"] for col in sa_inspect(bind).get_columns("books")}
    if "assigned_to" in books_columns:
        op.drop_column("books", "assigned_to")
