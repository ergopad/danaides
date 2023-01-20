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
        Column('index', INTEGER, nullable=true)
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
   
    return metadata_obj, TABLES