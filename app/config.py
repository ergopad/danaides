from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.dialects.postgresql import VARCHAR, BIGINT, INTEGER, TEXT

metadata_obj = MetaData()
TABLES = {}

TABLES['balances'] = Table('balances', metadata_obj,
    Column('id', INTEGER, primary_key=True),
    Column('address', VARCHAR(64), nullable=False),
    Column('nergs', BIGINT),
)

TABLES['assets'] = Table('assets', metadata_obj,
    Column('id', INTEGER, primary_key=True),
    Column('address', VARCHAR(64), nullable=False),
    Column('nergs', BIGINT),
)

TABLES['staking'] = Table('staking', metadata_obj,
    Column('id', INTEGER, primary_key=True),
    Column('address', VARCHAR(64), nullable=False),
    Column('token_id', VARCHAR(64), nullable=False),
    Column('box_id', VARCHAR(64), nullable=False),
    Column('stakekey_token_id', VARCHAR(64), nullable=False),
    Column('amount', BIGINT),
    Column('penalty', VARCHAR(64), nullable=False),
)

TABLES['vesting'] = Table('vesting', metadata_obj,
    Column('id', INTEGER, primary_key=True),
    Column('token_id', VARCHAR(64), nullable=False),
    Column('box_id', VARCHAR(64), nullable=False),
    Column('vesting_key_id', VARCHAR(64), nullable=False),
    Column('parameters', VARCHAR(1024), nullable=False),
    Column('remaining', BIGINT),
    Column('ergo_tree', TEXT),
)

# TABLES = {
#     'staking' : {'address': 'varchar(64)', 'token_id': 'varchar(64)', 'box_id': 'varchar(64)', 'stakekey_token_id': 'varchar(64)', 'amount': 'bigint', 'penalty': 'varchar(64)'},
#     'vesting' : {'address': 'varchar(64)', 'token_id': 'varchar(64)', 'box_id': 'varchar(64)', 'vesting_key_id': 'varchar(64)', 'parameters': 'varchar(1024)', 'remaining': 'bigint', 'ergo_tree': 'text'},
#     'assets'  : {'address': 'varchar(64)', 'token_id': 'varchar(64)', 'amount': 'bigint'},
#     'balances': {'address': 'varchar(64)', 'nergs': 'bigint'},
# }

for t in metadata_obj.sorted_tables:
    TABLES[t] = {}
    for c in t.columns:
        TABLES[t][c] = c.type
