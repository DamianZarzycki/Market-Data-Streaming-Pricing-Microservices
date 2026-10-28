"""trades book_id status index

Revision ID: c3a91e7b2f10
Revises: 040aaeb85cd4
Create Date: 2026-10-27 23:10:00.000000

"""
from typing import Sequence, Union

from alembic import op


revision: str = "c3a91e7b2f10"
down_revision: Union[str, Sequence[str], None] = "040aaeb85cd4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_trades_book_id_status",
        "trades",
        ["book_id", "status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_trades_book_id_status", table_name="trades")
