"""add_archived_fields_to_signals

Revision ID: 85f14bb513b4
Revises: 20241127_add_signal_live_logs
Create Date: 2025-11-30 00:04:36.650006

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '85f14bb513b4'
down_revision = '20241127_add_signal_live_logs'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем поля для архивации сигналов
    op.add_column('signals', sa.Column('archived', sa.Boolean(), server_default='false', nullable=False))
    op.add_column('signals', sa.Column('archived_at', sa.DateTime(timezone=True), nullable=True))
    # Создаем индекс для быстрого поиска неархивированных сигналов
    op.create_index('ix_signals_archived', 'signals', ['archived'])


def downgrade() -> None:
    # Удаляем индекс и поля
    op.drop_index('ix_signals_archived', table_name='signals')
    op.drop_column('signals', 'archived_at')
    op.drop_column('signals', 'archived')

