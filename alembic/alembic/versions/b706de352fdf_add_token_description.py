"""add_token_description

Revision ID: b706de352fdf
Revises: 5d9ab5ff8b5c
Create Date: 2023-01-29 04:20:22.626007

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = 'b706de352fdf'
down_revision = '5d9ab5ff8b5c'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column('tokens', sa.Column('token_description', sa.TEXT(), nullable=True))


def downgrade() -> None:
    op.drop_column('tokens', 'token_description')
