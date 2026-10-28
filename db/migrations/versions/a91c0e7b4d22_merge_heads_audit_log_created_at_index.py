"""merge heads and index audit_logs.created_at

Revision ID: a91c0e7b4d22
Revises: f3a7c9e1b204, c3a91e7b2f10
Create Date: 2026-10-27 23:59:00.000000

"""
from typing import Sequence, Union

from alembic import op


revision: str = "a91c0e7b4d22"
down_revision: Union[str, Sequence[str], None] = ("f3a7c9e1b204", "c3a91e7b2f10")
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_audit_logs_created_at",
        "audit_logs",
        ["created_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_audit_logs_created_at", table_name="audit_logs")
