from sqlalchemy import MetaData, Table, Column, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import VARCHAR, BIGINT, INTEGER, TEXT, HSTORE, NUMERIC, TIMESTAMP, BOOLEAN

# testing
# from os import getenv
# DB_DANAIDES = f"postgresql://{getenv('DANAIDES_USER')}:{getenv('DANAIDES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/dana"

# INIT
class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

def get_tables(eng): 
    metadata_obj = MetaData(eng)
    TABLES = {}

    TABLES['audit_log'] = Table('audit_log', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('height', INTEGER, nullable=False),
        Column('created_at', TIMESTAMP, nullable=False, server_default=text('now()')),
        Column('service', VARCHAR(64), nullable=True),
        Column('notes', TEXT, nullable=True, default=0),
    )

    TABLES['boxes'] = Table('boxes', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('box_id', VARCHAR(64), nullable=False),
        Column('height', INTEGER, nullable=False),
        Column('is_unspent', BOOLEAN, nullable=True),
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
        Column('token_name', VARCHAR(1024), nullable=True),
        Column('token_type', VARCHAR(64), nullable=True),
        Column('token_price', NUMERIC(32, 10), nullable=True, default=0.0),
        Column('height', INTEGER, nullable=True),
    )

    TABLES['balances'] = Table('balances', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('address', VARCHAR(64), nullable=False, unique=True),
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
        Column('amount', NUMERIC(32, 10)),
        Column('penalty', VARCHAR(64), nullable=False),
        Column('project_name', VARCHAR(64), nullable=False),
    )

    TABLES['vesting'] = Table('vesting', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('address', VARCHAR(64), nullable=False),
        Column('token_id', VARCHAR(64), nullable=False),
        Column('box_id', VARCHAR(64), nullable=False),
        Column('vesting_key_id', VARCHAR(64), nullable=False),
        Column('parameters', VARCHAR(1024), nullable=False),
        Column('remaining', NUMERIC(32, 10)),
        Column('ergo_tree', TEXT),
    )

    TABLES['tokenomics_ergopad'] = Table('tokenomics_ergopad', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('token_id', VARCHAR(64), nullable=False),
        Column('decimals', INTEGER),
        Column('token_price', NUMERIC(32, 10)),
        Column('vested', BIGINT),
        Column('staked', BIGINT),
        Column('emitted', BIGINT),
        Column('stake_pool', BIGINT),
        Column('supply', BIGINT),
        Column('in_circulation', BIGINT),
        Column('vested_actual', NUMERIC(32, 10)),
        Column('staked_actual', NUMERIC(32, 10)),
        Column('emitted_actual', NUMERIC(32, 10)),
        Column('stake_pool_actual', NUMERIC(32, 10)),
        Column('supply_actual', NUMERIC(32, 10)),
        Column('in_circulation_actual', NUMERIC(32, 10)),
    )

    TABLES['tokenomics_paideia'] = Table('tokenomics_paideia', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('token_id', VARCHAR(64), nullable=False),
        Column('decimals', INTEGER),
        Column('token_price', NUMERIC(32, 10)),
        Column('vested', BIGINT),
        Column('staked', BIGINT),
        Column('emitted', BIGINT),
        Column('stake_pool', BIGINT),
        Column('supply', BIGINT),
        Column('in_circulation', BIGINT),
        Column('vested_actual', NUMERIC(32, 10)),
        Column('staked_actual', NUMERIC(32, 10)),
        Column('emitted_actual', NUMERIC(32, 10)),
        Column('stake_pool_actual', NUMERIC(32, 10)),
        Column('supply_actual', NUMERIC(32, 10)),
        Column('in_circulation_actual', NUMERIC(32, 10)),
    )

    TABLES['unspent_by_token'] = Table('unspent_by_token', metadata_obj,
        Column('id', INTEGER, primary_key=True),
        Column('token_id', VARCHAR(64), nullable=False),
        Column('box_id', VARCHAR(64), nullable=False),
    )
    
    return metadata_obj, TABLES