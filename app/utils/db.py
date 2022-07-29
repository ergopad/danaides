from sqlalchemy import create_engine, text
from os import getenv
from utils.logger import logger, Timer, printProgressBar

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

        columns = {
            'staking' : {'address': 'varchar(64)', 'token_id': 'varchar(64)', 'box_id': 'varchar(64)', 'stakekey_token_id': 'varchar(64)', 'amount': 'bigint', 'penalty': 'varchar(64)'},
            'vesting' : {'address': 'varchar(64)', 'token_id': 'varchar(64)', 'box_id': 'varchar(64)', 'vesting_key_id': 'varchar(64)', 'parameters': 'varchar(1024)', 'remaining': 'bigint', 'ergo_tree': 'text'},
            'assets'  : {'address': 'varchar(64)', 'token_id': 'varchar(64)', 'amount': 'bigint'},
            'balances': {'address': 'varchar(64)', 'nergs': 'bigint'},
        }

        sqlCreateTmp = f'''
            create table tmp_{tbl} (
                id serial not null primary key,
                {','.join([f'{k} {v}' for k, v in columns[tbl].items()])}
            );
        '''
        sqlInsertTmp = f'''
            insert into tmp_{tbl} ({','.join([k for k in columns[tbl].keys()])})
                select {','.join([k for k in columns[tbl].keys()])}
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
