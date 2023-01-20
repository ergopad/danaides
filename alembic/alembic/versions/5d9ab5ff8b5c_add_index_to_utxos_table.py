"""add index to utxos table

Revision ID: 5d9ab5ff8b5c
Revises: 028c52b0d196
Create Date: 2023-01-20 10:20:46.111408

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = '5d9ab5ff8b5c'
down_revision = '028c52b0d196'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('utxos', sa.Column('index', sa.INTEGER(), nullable=True))
    op.add_column('utxos', sa.Column('assets_array', postgresql.ARRAY(postgresql.HSTORE()), nullable=True))
    op.create_index('idx_utxos_ergo_tree', 'utxos', ['ergo_tree'], unique=False)


def downgrade() -> None:
    op.drop_index('idx_utxos_ergo_tree')
    op.drop_column('utxos', 'assets_array')
    op.drop_column('utxos', 'index')
