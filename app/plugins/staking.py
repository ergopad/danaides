import asyncio
import argparse

from time import sleep 
# from sqlalchemy import create_engine, text
from utils.logger import logger, Timer, printProgressBar
from utils.db import eng, text

#region INIT
PRETTYPRINT = False
VERBOSE = False
#endregion INIT

#region FUNCTIONS
async def process(is_plugin=False, args=None):
    t = Timer()
    t.start()

    # handle globals when process called from as plugin
    if is_plugin:
        if args.prettyprint: PRETTYPRINT = True
        if args.juxtapose: JUXTAPOSE = args.juxtapose
        else: JUXTAPOSE = 'staking'

    try:
        sqlCreateTmp = f'''
            create table tmp_staking (
                id serial not null primary key,
                address varchar(64),
                token_id varchar(64),
                box_id varchar(64),
                stakekey_token_id varchar(64),
                amount bigint,
                penalty varchar(64)
            );
        '''
        sqlInsertTmp = f'''
            insert into tmp_staking (address, token_id, box_id, stakekey_token_id, amount, penalty)
                select address, token_id, box_id, stakekey_token_id, amount, penalty from v_staking;
        '''
        sqlDropStaking = f'''
            drop table staking;
        '''
        sqlRenameTmp = f'''
            alter table tmp_staking rename to staking;
        '''

        with eng.begin() as con:
            con.execute(sqlCreateTmp)
            con.execute(sqlInsertTmp)
            con.execute(sqlDropStaking)
            con.execute(sqlRenameTmp)

        sqlDropTmp = f'''drop table if exists tmp_staking'''
        with eng.begin() as con:
            con.execute(sqlDropTmp)

    except KeyError as e:
        logger.error(f'ERR (KeyError): {e}')
        pass
    
    except Exception as e:
        logger.error(f'ERR: {e}')
        return {
            'status': 'error',
            'message': e,
        }

    sec = t.stop()
    logger.debug(f'Processing complete: {sec:0.4f}s')

    return {
        'status': 'success',
    }
#endregion FUNCTIONS

#region MAIN
async def main(args):
    res = await process()
    if res['status'] != 'success':
        logger.error(f'''ERR: creating STAKING table {res['message']}''')
    else:
        logger.debug('Created STAKING table successfully...')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-J", "--juxtapose", help="Alternative table name", default='staking')
    parser.add_argument("-P", "--prettyprint", help="Begin at this height", action='store_true')
    args = parser.parse_args()

    PRETTYPRINT = args.prettyprint
    JUXTAPOSE = args.juxtapose
    
    res = asyncio.run(main(args))
#endregion MAIN