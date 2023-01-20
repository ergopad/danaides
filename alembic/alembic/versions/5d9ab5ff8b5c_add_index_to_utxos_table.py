"""add index to utxos table

Revision ID: 5d9ab5ff8b5c
Revises: 028c52b0d196
Create Date: 2023-01-20 10:20:46.111408

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '5d9ab5ff8b5c'
down_revision = '028c52b0d196'
branch_labels = None
depends_on = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
