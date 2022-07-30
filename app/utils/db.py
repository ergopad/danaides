from sqlalchemy import create_engine, text
from os import getenv
from utils.logger import logger, Timer, printProgressBar
from config import TABLES

# from dotenv import load_dotenv
# load_dotenv()

DB_DANAIDES = f"postgresql://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DBNM')}"
eng = create_engine(DB_DANAIDES)

DB_ERGOPAD = f"postgresql://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/ergopad" # {getenv('ERGOPAD_DBNM')}"
engErgopad = create_engine(DB_ERGOPAD)

async def dnp(tbl: str):
    try:
        if tbl not in ['staking', 'vesting', 'assets', 'balances']:
            return {'status': 'error', 'message': f'invalid request for table: "{tbl}"'}

        sqlCreateTmp = f'''
            create table tmp_{tbl} (
                id serial not null primary key,
                {','.join([f'{k} {v}' for k, v in TABLES[tbl].items()])}
            );
        '''
        sqlInsertTmp = f'''
            insert into tmp_{tbl} ({','.join([k for k in TABLES[tbl].keys()])})
                select {','.join([k for k in TABLES[tbl].keys()])}
                from v_{tbl};
        '''
        sqlDropTable = f'''
            drop table if exists {tbl};
        '''
        sqlRenameTmp = f'''
            alter table tmp_{tbl} rename to {tbl};
        '''
        sqlRowCount = f'''
            select count(*) as row_count 
            from {tbl}
        '''

        row_count = 0
        with eng.begin() as con:
            con.execute(sqlCreateTmp)
            con.execute(sqlInsertTmp)
            con.execute(sqlDropTable)
            con.execute(sqlRenameTmp)
            res = con.execute(sqlRowCount).fetchone()
            row_count = res['row_count']

        return {'status': 'success', 'message': tbl, 'row_count': row_count}

    except Exception as e:
        logger.error(f'ERR: {e}')
        return {'status': 'error', 'message': f'ERR: dnp, {e}'}
