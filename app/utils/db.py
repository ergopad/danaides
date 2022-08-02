from os import path, listdir, getenv
from sqlalchemy import create_engine, inspect, text, MetaData, Table
from utils.logger import logger, Timer, printProgressBar
from config import get_tables
from string import ascii_uppercase, digits
from random import choices

# from dotenv import load_dotenv
# load_dotenv()

DB_DANAIDES = f"postgresql://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DBNM')}"
eng = create_engine(DB_DANAIDES)

# DB_ERGOPAD = f"postgresql://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/ergopad" # {getenv('ERGOPAD_DBNM')}"
# engErgopad = create_engine(DB_ERGOPAD)

async def dnp(tbl: str):
    try:
        if tbl not in ['staking', 'vesting', 'assets', 'balances']:
            return {'status': 'error', 'message': f'invalid request for table: "{tbl}"'}

        metadata_obj, TABLES = get_tables(eng)
        src = TABLES[tbl]
        tmp_table = f'tmp_{tbl}'
        eng._metadata = MetaData(bind=eng)
        eng._metadata.reflect(eng)
        tmp = Table(tbl, eng._metadata)
        tmp.name = tmp_table
        tmp.primary_key.name = f'''staking_pkey_{''.join(choices(ascii_uppercase+digits, k=10))}'''
        cols = ','.join([n.name for n in tmp.columns if n.name != 'id'])

        sqlInsertTmp = f'''
            insert into {tmp_table} ({cols})
                select {cols}
                from v_{tbl};
        '''
        sqlRowCount = f'''
            select count(*) as rc
            from {tbl}
        '''

        rc = {
            'before': 0,
            'after': 0,
        }
        # starting row count
        with eng.begin() as con:
            res = con.execute(sqlRowCount).fetchone()
            rc['before'] = res['rc']
            logger.warning(f'''row count before: {rc['before']}''')
        
        # check if tmp exists
        if inspect(eng).has_table(tmp_table):
            logger.warning(f'drop {tmp_table}')
            tmp.drop()

        # crate tmp
        logger.warning(f'create {tmp_table}')
        tmp.create()

        # populate tmp
        with eng.begin() as con:
            logger.warning(f'insert into {tmp_table} from v_{tbl}')
            con.execute(sqlInsertTmp)
        
        # drop src
        logger.warning(f'drop {tbl}')
        src.drop()
        
        # rename tmp to src
        with eng.begin() as con:
            logger.warning(f'rename {tmp_table} to {tbl}')
            con.execute(f'alter table {tmp_table} rename to {tbl};')

        # final row count
        with eng.begin() as con:
            res = con.execute(sqlRowCount).fetchone()
            rc['after'] = res['rc']
            logger.warning(f'''row count after: {rc['after']}''')

        return {
            'status': 'success', 
            'message': tbl, 
            'row_count_before': rc['before'], 
            'row_count_after': rc['after'],
        }

    except Exception as e:
        logger.error(f'ERR: {e}')
        return {'status': 'error', 'message': f'ERR: dnp, {e}'}

# create db objects if they don't exists
async def init_db():
    # if new db, make sure hstore extension exists
    try:
        sql = 'create extension if not exists hstore;'
        with eng.begin() as con:
            con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: {e}')
        pass

    # build tables, if needed
    try:
        # metadata_obj = MetaData(eng)
        metadata_obj, TABLES = get_tables(eng)
        metadata_obj.create_all(eng)

    except Exception as e:
        logger.error(f'ERR: {e}')

    # build views
    try:
        view_dir = '/app/sql/views'
        with eng.begin() as con:
            views = listdir(view_dir)
            for v in views:
                if v.startswith('v_') and v.endswith('.sql'):
                    with open(path.join(view_dir, v), 'r') as f:
                        sql = f.read()
                    con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: {e}')

# create tmp version of table 
async def build_tmp(tbl:str):
    try:
        metadata_obj, TABLES = get_tables(eng)
        tmp = TABLES[tbl]
        tmp.name = f'tmp_{tbl}'
        tmp.create() 
        
    except Exception as e:
        logger.error(f'ERR: {e}')
