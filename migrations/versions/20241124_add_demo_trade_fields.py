"""add demo trade tracking columns

Revision ID: add_demo_trade_fields
Revises: add_signal_stop_loss
Create Date: 2025-11-24 15:30:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_demo_trade_fields'
down_revision = 'add_signal_stop_loss'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('signals', sa.Column('demo_order_id', sa.String(length=100), nullable=True))
    op.add_column('signals', sa.Column('demo_status', sa.String(length=30), nullable=True))
    op.add_column('signals', sa.Column('demo_quantity', sa.Float(), nullable=True))
    op.add_column('signals', sa.Column('demo_tp_price', sa.Float(), nullable=True))
    op.add_column('signals', sa.Column('demo_sl_price', sa.Float(), nullable=True))
    op.add_column('signals', sa.Column('demo_error', sa.Text(), nullable=True))
    op.add_column('signals', sa.Column('demo_submitted_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('signals', sa.Column('demo_updated_at', sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column('signals', 'demo_updated_at')
    op.drop_column('signals', 'demo_submitted_at')
    op.drop_column('signals', 'demo_error')
    op.drop_column('signals', 'demo_sl_price')
    op.drop_column('signals', 'demo_tp_price')
    op.drop_column('signals', 'demo_quantity')
    op.drop_column('signals', 'demo_status')
    op.drop_column('signals', 'demo_order_id')

