from os import path, listdir, getenv
from sqlalchemy import create_engine, inspect, text, MetaData, Table
from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles
from utils.logger import logger, Timer, printProgressBar
from config import get_tables
from string import ascii_uppercase, digits
from random import choices
from time import sleep

DB_DANAIDES = f"postgresql://{getenv('DANAIDES_USER')}:{getenv('DANAIDES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DB')}"
eng = create_engine(DB_DANAIDES)

@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"

def dnp(tbl: str):
    try:
        if tbl not in ['staking', 'vesting', 'assets', 'balances', 'tokenomics_ergopad', 'tokenomics_paideia', 'unspent_by_token', 'token_status']:
            return {
                'status': 'error', 
                'message': f'invalid request for table: "{tbl}"', 
                'row_count_before': -1, 
                'row_count_after': -1,
            }

        metadata_obj, TABLES = get_tables(eng)
        src = TABLES[tbl]
        tmp_table = f'tmp_{tbl}'
        eng._metadata = MetaData(bind=eng)
        eng._metadata.reflect(eng)
        tmp = Table(tbl, eng._metadata)

        rndstr = '__'+(''.join(choices(ascii_uppercase+digits, k=10)))
        tmp.name = tmp_table
        cols = ','.join([n.name for n in tmp.columns if n.name != 'id'])
        if tmp.primary_key.name is not None:
            tmp.primary_key.name = (tmp.primary_key.name.split('__'))[0] + rndstr

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
        logger.debug(f'before rc')
        with eng.begin() as con:
            try:
                res = con.execute(sqlRowCount).fetchone()
                rc['before'] = res['rc']
            except:
                pass
            logger.warning(f'''row count before: {rc['before']}''')
        
        # avoid dups with index names
        for i in tmp.indexes:
            i.name = (i.name.split('__'))[0] + rndstr

        # avoid dups with constraint names
        for c in tmp.constraints:
            c.name = (c.name.split('__'))[0] + rndstr

        # check if tmp exists
        logger.debug(f'check tmp')
        if inspect(eng).has_table(tmp_table):
            logger.warning(f'drop {tmp_table}')
            tmp.drop()

        # create tmp
        logger.warning(f'create {tmp_table}')
        tmp.create()

        # populate tmp
        with eng.begin() as con:
            logger.warning(f'insert into {tmp_table} from v_{tbl}')
            con.execute(sqlInsertTmp)
        
        # drop src
        if inspect(eng).has_table(tbl):
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

        # fin
        return {
            'status': 'success', 
            'message': tbl, 
            'row_count_before': rc['before'], 
            'row_count_after': rc['after'],
        }

    except Exception as e:
        logger.error(f'ERR: {e}')
        return {
            'status': 'error', 
            'message': f'ERR: dnp, {e}', 
            'row_count_before': -1, 
            'row_count_after': -1,
        }

# create db objects if they don't exists
async def init_db():
    # if new db, make sure hstore extension exists
    attempt = 5
    while attempt > 0:
        try:
            sql = 'select service from audit_log where height = -1'
            with eng.begin() as con:
                con.execute(sql)
            attempt = 0

        except Exception as e:
            attempt -= 1
            if attempt == 0:
                logger.error(f'ERR: Initializing database in 5 attempts; {e}')
            else:
                logger.warning(f'Failed to access database (attempt {5-attempt})')
            sleep(1)

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

# build all indexes
async def build_indexes():
    try:
        index_dir = '/app/sql/indexes'
        with eng.begin() as con:
            indexes = listdir(index_dir)
            for i in indexes:
                # if v.startswith('i_') and  and i.endswith('.sql'):
                if i.endswith('.sql'):
                    with open(path.join(index_dir, i), 'r') as f:
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
