"""index active trades by status and valuations by time

Revision ID: e4b7c1a90d33
Revises: a91c0e7b4d22
Create Date: 2026-10-28 21:55:00.000000

"""
from typing import Sequence, Union

from alembic import op


revision: str = "e4b7c1a90d33"
down_revision: Union[str, Sequence[str], None] = "a91c0e7b4d22"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_trades_status_book_id",
        "trades",
        ["status", "book_id"],
        unique=False,
    )
    op.create_index(
        "ix_valuations_trade_id_valuation_time",
        "valuations",
        ["trade_id", "valuation_time"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_valuations_trade_id_valuation_time", table_name="valuations")
    op.drop_index("ix_trades_status_book_id", table_name="trades")
