import asyncio
import argparse

from time import sleep 
# from sqlalchemy import create_engine, text
from utils.logger import logger, Timer, printProgressBar
from utils.db import eng, text, dnp

#region INIT
PRETTYPRINT = False
VERBOSE = False
LINELEN = 100
#endregion INIT

#region FUNCTIONS
async def process(is_plugin=False, args=None):
    t = Timer()
    t.start()

    # handle globals when process called from as plugin
    if is_plugin:
        if args.prettyprint: PRETTYPRINT = True

    try:
        res = dnp('staking')
        suffix = f'''STATUS: {res['status']}'''
        if PRETTYPRINT: printProgressBar(100, 100, prefix='[STAKING]', suffix=f'{suffix}{" "*(LINELEN-len(suffix))}', length=50)
        else: logger.info(suffix)

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