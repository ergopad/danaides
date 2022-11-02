"""added subcriptions

Revision ID: 2f030ff38600
Revises: d38e3c2cebba
Create Date: 2022-11-01 07:23:46.116432

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '2f030ff38600'
down_revision = 'd38e3c2cebba'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table('subscriptions',
    sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False),
    sa.Column('plan_owner', sa.VARCHAR(length=64), autoincrement=False, nullable=True),
    sa.Column('plan_price', sa.NUMERIC(precision=16, scale=2), autoincrement=False, nullable=True),
    sa.Column('plan_interval', sa.INTEGER(), autoincrement=False, nullable=True),
    sa.Column('start_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('end_date', postgresql.TIMESTAMP(), autoincrement=False, nullable=True),
    sa.Column('api_key', sa.VARCHAR(length=32), autoincrement=False, nullable=True),
    sa.Column('ipv4_addresses', postgresql.ARRAY(sa.VARCHAR(length=16)), autoincrement=False, nullable=True),
    sa.PrimaryKeyConstraint('id', name='subscriptions_pkey')
    )


def downgrade() -> None:
    op.drop_table('subscriptions')
