from os import path, listdir, getenv
from sqlalchemy import create_engine, text
from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles
from utils.logger import logger, myself
from config import get_tables
from time import sleep
# from sqlalchemy import create_engine, inspect, text, MetaData, Table
# from string import ascii_uppercase, digits
# from random import choices

DB_DANAIDES = f"postgresql://{getenv('DANAIDES_USER')}:{getenv('DANAIDES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DB')}"
eng = create_engine(DB_DANAIDES)

@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"

# create db objects if they don't exists
def init_db():
    # if new db, make sure hstore extension exists
    attempt = 5
    while attempt > 0:
        try:
            # sql = 'select service from audit_log where height = -1'
            sql = 'select count(*) as i alembic_version'
            res = None
            with eng.begin() as con:
                res = con.execute(sql).fetchone()

            # init database; need to run alembic
            if res == 0:
                logger.error('ERR::blank database; likely need to run alembic upgrade head')
                exit(1)

            attempt = 0

        except Exception as e:
            attempt -= 1
            if attempt == 0:
                logger.error(f'ERR: Initializing database in 5 attempts; {e}')
            else:
                logger.warning(f'Failed to access database (attempt {5-attempt})')
            sleep(1)

    # add extensions
    try:
        logger.debug(f'creating hstore extension')
        sql = 'create extension if not exists hstore'
        with eng.begin() as con:
            con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: {e}')
        pass

    # create alt schemas
    try:
        logger.debug(f'creating schema checkpoint')
        sql = 'create schema if not exists checkpoint'
        with eng.begin() as con:
            con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: {e}')
        pass


    # build tables, if needed
    try:
        logger.debug(f'getting metadata')
        # metadata_obj = MetaData(eng)
        metadata_obj, TABLES = get_tables(eng)
        metadata_obj.create_all(eng)

    except Exception as e:
        logger.error(f'ERR: {e}')

    # build views
    try:
        logger.debug(f'building views')
        # remove all materialized views (start with d_)
        sql = f'''
            select matviewname
            from pg_matviews 
            where schemaname = 'public'
                -- remove all? or, identify through naming convention?
                -- and left(matviewname, 2) = 'd_'
        '''
        with eng.begin() as con:
            res = con.execute(sql).fetchall()
            for r in res:
                mv = r['matviewname']
                logger.debug(f'drop matview {mv}')
                sql = f'''drop materialized view if exists {mv} cascade'''
                con.execute(sql)
        
        # create mat views
        view_dir = '/app/sql/views'
        with eng.begin() as con:
            views = listdir(view_dir)
            for v in sorted(views):
                if v.startswith('d_') and v.endswith('.sql'):
                    logger.debug(f'creating matview {v}')
                    with open(path.join(view_dir, v), 'r') as f:
                        sql = f.read()
                    con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: {e}')

def refresh_views(concurrently=True):
    try:
        # refresh all the views in background thread? (will take some time)
        # after first refresh, concurrently can be used
        # remove all materialized views (start with d_)
        sql = f'''
            select matviewname
            from pg_matviews 
            where schemaname = 'public' 
                -- and left(matviewname, 2) = 'd_'
        '''
        with eng.begin() as con:
            res = con.execute(sql).fetchall()
            for mv in res:
                sql = f'''refresh materialized view {('', 'concurrently')[concurrently]} {mv['matviewname']}'''
                con.execute(sql)

        logger.debug('refresh materialized views complete.')

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')

# build all indexes
async def build_indexes():
    try:
        index_dir = '/app/sql/indexes'
        with eng.begin() as con:
            indexes = listdir(index_dir)
            for i in indexes:
                # if v.startswith('d_') and  and i.endswith('.sql'):
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
        logger.error(f'ERR: {myself()}; {e}')
