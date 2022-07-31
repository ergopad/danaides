from sqlalchemy import MetaData, Table, Column
from sqlalchemy.dialects.postgresql import VARCHAR, BIGINT, INTEGER, TEXT, HSTORE, NUMERIC

# testing
# from os import getenv
# DB_DANAIDES = f"postgresql://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/dana"

# INIT
class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

def get_tables(eng): 
    metadata_obj = MetaData(eng)
    TABLES = {}

    TABLES['boxes'] = Table('boxes', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('box_id', VARCHAR(64), nullable=False),
        Column('height', INTEGER, nullable=False),
        Column('is_unspent', INTEGER, nullable=True),
        Column('nerg', BIGINT, nullable=True, default=0),
    )

    TABLES['utxos'] = Table('utxos', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('box_id', VARCHAR(64), nullable=False, unique=True),
        Column('ergo_tree', TEXT, nullable=True),
        Column('address', VARCHAR(64), nullable=False),
        Column('nergs', BIGINT, nullable=True, default=0),
        Column('registers', HSTORE, nullable=True),
        Column('assets', HSTORE, nullable=True),
        Column('transaction_id', VARCHAR(64), nullable=True),
        Column('creation_height', INTEGER, nullable=True),
        Column('height', INTEGER, nullable=False),
    )

    TABLES['prices'] = Table('prices', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('token_id', VARCHAR(64), nullable=False),
        Column('price', NUMERIC(32, 10), nullable=False, default=0.0),
    )

    TABLES['tokens'] = Table('tokens', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('token_id', VARCHAR(64), nullable=False),
        Column('decimals', BIGINT, nullable=True),
        Column('amount', BIGINT, nullable=True),
        Column('token_name', VARCHAR(64), nullable=True),
        Column('token_type', VARCHAR(64), nullable=True),
        Column('token_price', NUMERIC(32, 10), nullable=True, default=0.0),
        Column('height', INTEGER, nullable=True),
    )

    TABLES['balances'] = Table('balances', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('address', VARCHAR(64), nullable=False),
        Column('nergs', BIGINT),
    )

    TABLES['assets'] = Table('assets', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('address', VARCHAR(64), nullable=False),
        Column('token_id', VARCHAR(64), nullable=False),
        Column('amount', BIGINT),
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

    return TABLES