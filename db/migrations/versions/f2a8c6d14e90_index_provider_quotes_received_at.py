"""index provider quotes by provider, symbol, and received_at

Revision ID: f2a8c6d14e90
Revises: e4b7c1a90d33
Create Date: 2026-10-28 23:30:00.000000

"""
from typing import Sequence, Union

from alembic import op


revision: str = "f2a8c6d14e90"
down_revision: Union[str, Sequence[str], None] = "e4b7c1a90d33"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Same name may already exist without DESC. IF NOT EXISTS would keep that
    # index and the latest-quote lookups would never get the descending key.
    op.execute("DROP INDEX IF EXISTS ix_provider_quotes_provider_symbol_received_at")
    op.execute(
        "CREATE INDEX ix_provider_quotes_provider_symbol_received_at "
        "ON market_data_provider_quotes (provider, symbol, received_at DESC)"
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_provider_quotes_provider_symbol_received_at")
