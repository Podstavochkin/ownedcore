"""add_ohlcv_table

Revision ID: 313f67cf0c5d
Revises: 9fedc0116b9b
Create Date: 2025-12-10 20:03:13.949810

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '313f67cf0c5d'
down_revision = '9fedc0116b9b'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Создаем таблицу для хранения свечных данных (OHLCV)
    op.create_table('ohlcv',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('symbol', sa.String(length=20), nullable=False),
        sa.Column('timeframe', sa.String(length=10), nullable=False),
        sa.Column('timestamp', sa.DateTime(timezone=True), nullable=False),
        sa.Column('open', sa.Float(), nullable=False),
        sa.Column('high', sa.Float(), nullable=False),
        sa.Column('low', sa.Float(), nullable=False),
        sa.Column('close', sa.Float(), nullable=False),
        sa.Column('volume', sa.Float(), nullable=False, server_default='0.0'),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=True),
        sa.Column('updated_at', sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Создаем индексы для быстрого поиска
    op.create_index('ix_ohlcv_id', 'ohlcv', ['id'], unique=False)
    op.create_index('ix_ohlcv_symbol', 'ohlcv', ['symbol'], unique=False)
    op.create_index('ix_ohlcv_timeframe', 'ohlcv', ['timeframe'], unique=False)
    op.create_index('ix_ohlcv_timestamp', 'ohlcv', ['timestamp'], unique=False)
    
    # Составной индекс для быстрого поиска по паре и таймфрейму
    op.create_index('idx_ohlcv_symbol_tf', 'ohlcv', ['symbol', 'timeframe'], unique=False)
    
    # Уникальный составной индекс: одна свеча = одна комбинация symbol+timeframe+timestamp
    op.create_index('idx_ohlcv_symbol_tf_ts', 'ohlcv', ['symbol', 'timeframe', 'timestamp'], unique=True)


def downgrade() -> None:
    # Удаляем индексы
    op.drop_index('idx_ohlcv_symbol_tf_ts', table_name='ohlcv')
    op.drop_index('idx_ohlcv_symbol_tf', table_name='ohlcv')
    op.drop_index('ix_ohlcv_timestamp', table_name='ohlcv')
    op.drop_index('ix_ohlcv_timeframe', table_name='ohlcv')
    op.drop_index('ix_ohlcv_symbol', table_name='ohlcv')
    op.drop_index('ix_ohlcv_id', table_name='ohlcv')
    
    # Удаляем таблицу
    op.drop_table('ohlcv')

