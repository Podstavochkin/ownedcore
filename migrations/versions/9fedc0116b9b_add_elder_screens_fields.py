"""add_elder_screens_fields

Revision ID: 9fedc0116b9b
Revises: 85f14bb513b4
Create Date: 2025-12-07 14:19:02.616837

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '9fedc0116b9b'
down_revision = '85f14bb513b4'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Добавляем поля для Elder's Triple Screen System
    op.add_column('signals', sa.Column('elder_screen_1_passed', sa.Boolean(), nullable=True))
    op.add_column('signals', sa.Column('elder_screen_1_blocked_reason', sa.Text(), nullable=True))
    op.add_column('signals', sa.Column('elder_screen_2_passed', sa.Boolean(), nullable=True))
    op.add_column('signals', sa.Column('elder_screen_2_blocked_reason', sa.Text(), nullable=True))
    op.add_column('signals', sa.Column('elder_screen_3_passed', sa.Boolean(), nullable=True))
    op.add_column('signals', sa.Column('elder_screen_3_blocked_reason', sa.Text(), nullable=True))
    op.add_column('signals', sa.Column('elder_screens_metadata', sa.JSON(), nullable=True))


def downgrade() -> None:
    # Удаляем поля Elder's Triple Screen System
    op.drop_column('signals', 'elder_screens_metadata')
    op.drop_column('signals', 'elder_screen_3_blocked_reason')
    op.drop_column('signals', 'elder_screen_3_passed')
    op.drop_column('signals', 'elder_screen_2_blocked_reason')
    op.drop_column('signals', 'elder_screen_2_passed')
    op.drop_column('signals', 'elder_screen_1_blocked_reason')
    op.drop_column('signals', 'elder_screen_1_passed')

