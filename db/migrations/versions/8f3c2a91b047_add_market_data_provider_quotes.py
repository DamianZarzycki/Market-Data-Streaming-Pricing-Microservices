"""add market_data_provider_quotes

Revision ID: 8f3c2a91b047
Revises: 662d91ed6853
Create Date: 2026-08-26 23:59:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "8f3c2a91b047"
down_revision: Union[str, Sequence[str], None] = "662d91ed6853"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "market_data_provider_quotes",
        sa.Column("quote_id", sa.UUID(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False),
        sa.Column("asset_class", sa.Text(), nullable=False),
        sa.Column("symbol", sa.Text(), nullable=False),
        sa.Column("bid", sa.Numeric(), nullable=True),
        sa.Column("ask", sa.Numeric(), nullable=True),
        sa.Column("last", sa.Numeric(), nullable=True),
        sa.Column("currency", sa.Text(), nullable=True),
        sa.Column("provider_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("raw_payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("quote_id"),
    )

def downgrade() -> None:
    op.drop_table("market_data_provider_quotes")
