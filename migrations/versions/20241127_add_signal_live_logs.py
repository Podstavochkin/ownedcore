"""add signal live logs and demo_filled_at

Revision ID: 20241127_add_signal_live_logs
Revises: 20241124_add_demo_trade_fields
Create Date: 2025-11-27
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20241127_add_signal_live_logs'
down_revision = 'add_demo_trade_fields'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'signals',
        sa.Column('demo_filled_at', sa.DateTime(timezone=True), nullable=True)
    )

    op.create_table(
        'signal_live_logs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('signal_id', sa.Integer(), sa.ForeignKey('signals.id', ondelete='CASCADE'), nullable=False),
        sa.Column('event_type', sa.String(length=50), nullable=True),
        sa.Column('status', sa.String(length=30), nullable=True),
        sa.Column('message', sa.Text(), nullable=False),
        sa.Column('details', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=True),
    )
    op.create_index('ix_signal_live_logs_signal_id', 'signal_live_logs', ['signal_id'])
    op.create_index('ix_signal_live_logs_created_at', 'signal_live_logs', ['created_at'])


def downgrade():
    op.drop_index('ix_signal_live_logs_created_at', table_name='signal_live_logs')
    op.drop_index('ix_signal_live_logs_signal_id', table_name='signal_live_logs')
    op.drop_table('signal_live_logs')
    op.drop_column('signals', 'demo_filled_at')

