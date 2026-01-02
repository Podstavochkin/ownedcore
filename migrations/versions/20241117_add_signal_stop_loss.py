"""add stop_loss column to signals

Revision ID: add_signal_stop_loss
Revises: add_signal_meta_fields
Create Date: 2025-11-17 15:55:00.000000
"""

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'add_signal_stop_loss'
down_revision = 'add_signal_meta_fields'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('signals', sa.Column('stop_loss', sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column('signals', 'stop_loss')
