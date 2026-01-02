"""add signal trade metadata fields

Revision ID: add_signal_meta_fields
Revises: ef922a450a11
Create Date: 2024-11-17 00:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_signal_meta_fields'
down_revision = 'ef922a450a11'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('signals', sa.Column('level_timeframe', sa.String(length=10), nullable=True))
    op.add_column('signals', sa.Column('historical_touches', sa.Integer(), nullable=True))
    op.add_column('signals', sa.Column('live_test_count', sa.Integer(), nullable=True))
    op.add_column('signals', sa.Column('level_score', sa.Float(), nullable=True))
    op.add_column('signals', sa.Column('distance_percent', sa.Float(), nullable=True))
    op.add_column('signals', sa.Column('exit_price', sa.Float(), nullable=True))
    op.add_column('signals', sa.Column('exit_timestamp', sa.DateTime(timezone=True), nullable=True))
    op.add_column('signals', sa.Column('exit_reason', sa.String(length=50), nullable=True))
    op.alter_column('signals', 'status',
        existing_type=sa.String(length=20),
        server_default='ACTIVE',
        existing_nullable=True)


def downgrade() -> None:
    op.alter_column('signals', 'status',
        existing_type=sa.String(length=20),
        server_default=None,
        existing_nullable=True)
    op.drop_column('signals', 'exit_reason')
    op.drop_column('signals', 'exit_timestamp')
    op.drop_column('signals', 'exit_price')
    op.drop_column('signals', 'distance_percent')
    op.drop_column('signals', 'level_score')
    op.drop_column('signals', 'live_test_count')
    op.drop_column('signals', 'historical_touches')
    op.drop_column('signals', 'level_timeframe')

