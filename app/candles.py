import asyncio
import os, sys, signal
import pandas as pd
import argparse
# import json

from time import sleep, time
from utils.logger import logger, Timer, printProgressBar
from utils.db import eng, text
from utils.ergo import headers, NODE_API
from utils.ergodex import getErgodexTokenPriceByTokenId
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
from requests import get, post
# from ergo_python_appkit.appkit import ErgoValue

sys.path.insert(0, '/app')

#region INIT
PRETTYPRINT = False
LINELEN = 100
VERBOSE = False
NERGS2ERGS = 10**9
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 5000 # save progress every X blocks
CLEANUP_NEEDED = False
TOKEN_ID=None
ERGODEX_POOL = 'https://api.ergodex.io/v1/amm/pool'

# token_id : pool_id
'''
    -- find paideia; pool is asset key where value == 1
    select box_id as "boxId", assets::json
    from utxos 
    where ergo_tree = '1999030f0400040204020404040405feffffffffffffffff0105feffffffffffffffff01050004d00f040004000406050005000580dac409d819d601b2a5730000d602e4c6a70404d603db63087201d604db6308a7d605b27203730100d606b27204730200d607b27203730300d608b27204730400d6099973058c720602d60a999973068c7205027209d60bc17201d60cc1a7d60d99720b720cd60e91720d7307d60f8c720802d6107e720f06d6117e720d06d612998c720702720fd6137e720c06d6147308d6157e721206d6167e720a06d6177e720906d6189c72117217d6199c72157217d1ededededededed93c27201c2a793e4c672010404720293b27203730900b27204730a00938c7205018c720601938c7207018c72080193b17203730b9593720a730c95720e929c9c721072117e7202069c7ef07212069a9c72137e7214067e9c720d7e72020506929c9c721372157e7202069c7ef0720d069a9c72107e7214067e9c72127e7202050695ed720e917212730d907216a19d721872139d72197210ed9272189c721672139272199c7216721091720b730e'
        and registers ? 'R4'
        and assets ? '1fd6e032e8476c4aa54c18c1a308dce83940e8f4a28f576440513ed7326ad489'
    order by nergs desc 
    limit 1
'''
POOLS = {
    # Paideia
    '1fd6e032e8476c4aa54c18c1a308dce83940e8f4a28f576440513ed7326ad489': '666be5df835a48b99c40a395a8aa3ea6ce39ede2cd77c02921d629b9baad8200',
}

import inspect
myself = lambda: inspect.stack()[1][3]
#endregion INIT

#region FUNCTIONS
async def checkpoint(tkn, mkt, df):
    try:
        logger.debug(f'checkpoint...')
        
        # stuff it
        df.to_sql('ohlc', eng, schema='checkpoint', if_exists='replace')
        sql = f'''
            delete from checkpoint.ohlc
            where timestamp <= (select max(timestamp) from ohlc);

            insert into ohlc (date, timestamp, price, token_id, market)
                select date, timestamp, price, '{tkn}', '{mkt}'
                from checkpoint.ohlc;
        '''
        with eng.begin() as con:
            con.execute(sql)

        return {}

    except Exception as e:
        logger.error(f'ERR::{myself()}::{e}')
        pass

async def alt_checkpoint(tkn, df):
    try:
        logger.debug(f'checkpoint...')

        # save these timeframes
        for tf in ['1h', '1d', '1W-MON', '1M']:
            logger.info(f'ohlc {tf}...')
            
            # method 1 
            dfn = df.set_index('date')['price'].resample(tf).ohlc()
            
            # method 2; timeframe based on strftime
            # df['hr'] = df['date'].dt.strftime('%Y-%m-%d %H:00')
            # dfn = df.groupby('hr')['price'].ohlc()
            
            # stuff it
            dfn.to_sql('ohlc', eng, schema='checkpoint', if_exists='replace')
            sql = f'''
                -- ignore first and last (avoid incomplete sums)
                delete from checkpoint.ohlc
                where date = (select min(date) from checkpoint.ohlc);
                delete from checkpoint.ohlc
                where date = (select max(date) from checkpoint.ohlc);

                -- avoid dups
                delete from "ohlc_{tf}"
                where date >= (select min(date) from checkpoint.ohlc);

                -- 
                insert into "ohlc_{tf}" (date, open, high, low, close, token_id)
                    select date, open, high, low, close, '{tkn}'
                    from checkpoint.ohlc;
            '''
            with eng.begin() as con:
                con.execute(sql)

        return {}

    except Exception as e:
        logger.error(f'ERR::{myself()}::{e}')
        pass

async def process(args, t):
    try:
        # query spectrum.fi for candles
        frm = (int(time()) - 60*60*24*91)*1000 # 91 days ago, in milliseconds
        for token_id, pool_id in POOLS.items():
            logger.debug(f'pool: {pool_id}')
            url = f'''{ERGODEX_POOL}/{pool_id}/chart?from={frm}&resolution=1'''
            res = get(url)
            if res.ok:
                logger.debug(f'found {len(res.json())} items...')
                prices = pd.DataFrame(res.json())
                prices['date'] = pd.to_datetime(prices['timestamp'], unit='ms', origin='unix')
                res = await checkpoint(token_id, 'spectrum', prices)
            else:
                logger.error(f'ERR::invalid response to: {url}\n{res.status_code}::{res.text}')

            logger.info(f'split: {t.split()}')

        logger.debug('processing complete.')
        return {}

    except Exception as e:
        logger.error(f'ERR::{myself()}::{e}')
        pass

async def alt_process(args, t):
    try:
        logger.debug(f'processing...')
        # handle standalone (not called as plugin)
        # is there a match for token -> pool?
        # if TOKEN_ID not in POOLS:
        #     logger.error(f'invalid pool or does not exist for token, {TOKEN_ID}')
        #     return None

        # query spectrum.fi for candles
        frm = (int(time()) - 60*60*24*91)*1000 # 91 days ago, in milliseconds
        for token_id, pool_id in POOLS.items():
            logger.debug(f'pool: {pool_id}')
            url = f'''{ERGODEX_POOL}/{pool_id}/chart?from={frm}&resolution=1'''
            res = get(url)
            if res.ok:
                logger.debug(f'found {len(res.json())} items...')
                prices = pd.DataFrame(res.json())
                prices['date'] = pd.to_datetime(prices['timestamp'], unit='ms', origin='unix')
                res = await checkpoint(token_id, prices)
            else:
                logger.error(f'ERR::invalid response to: {url}\n{res.status_code}::{res.text}')

            logger.info(f'split: {t.split()}')

        logger.debug('processing complete.')
        return {}

    except Exception as e:
        logger.error(f'ERR::{myself()}::{e}')
        pass
#endregion FUNCTIONS

#region APP
class App:
    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print('Received:', signum)
        self.shutdown = True

    def init(self):
        logger.info("main.app:: Ready, go...")

    def stop(self):
        logger.info("main.app:: Fin.")

    async def process(self, args):
        # init timer
        t = Timer()
        t.start()

        # process
        res = await process(args, t)
        
        # timer
        sec = t.stop()
        logger.debug(f'main.app::process took {sec:0.4f}s...')
        
        return res
#endregion APP

#region CLI
def cli():
    global PRETTYPRINT
    global VERBOSE
    global TOKEN_ID

    parser = argparse.ArgumentParser()
    
    parser.add_argument("-P", "--prettyprint", help="Progress bar vs. scrolling", action='store_true')
    parser.add_argument("-V", "--verbose", help="Be wordy", action='store_true')
    parser.add_argument("-T", "--tokenid", help="Ergo Token Id", default='')
    
    args = parser.parse_args()

    if args.prettyprint: logger.warning(f'Pretty print...')
    if args.verbose: logger.warning(f'Verbose...')
    if args.tokenid != '': logger.warning(f'Token Id: {args.tokenid}...')

    PRETTYPRINT = args.prettyprint
    VERBOSE = args.verbose
    TOKEN_ID = args.tokenid

    return args
#encregion CLI

#region MAIN
# paideia: 1fd6e032e8476c4aa54c18c1a308dce83940e8f4a28f576440513ed7326ad489
if __name__ == '__main__':

    # params
    args = cli()

    # sanity check    
    try: 
        logger.info(f'sanity check: {TOKEN_ID}')
        sql = text(f'''
            select token_id 
            from tokens 
            where token_id = :token_id
        ''')
        with eng.begin() as con:
            res = con.execute(sql, {'token_id': list(POOLS)[0]}).fetchone()            

        if res == None:
            logger.error(f'no results from danaides.tokens...')
            try: sys.exit(0)
            except SystemExit: os._exit(0)          

        if res['token_id'] != list(POOLS)[0]:
            logger.error(f'dne in tokens table...')
            try: sys.exit(0)
            except SystemExit: os._exit(0)            

    except OperationalError as e:
        logger.error(f'unable to find token...')
        try: sys.exit(0)
        except SystemExit: os._exit(0)            

    # create app
    app = App()
    app.init()

    logger.info(f'starting app...')
    if TOKEN_ID == '':
        while True:
            res = asyncio.run(app.process(TOKEN_ID))
            logger.info(f'sleeping...')
            # sleep(5*60) # 5 mins
            sleep(30) # 5 mins
    else:
        res = asyncio.run(app.process(TOKEN_ID))

    # fin
    app.stop()
    try: sys.exit(0)
    except SystemExit: os._exit(0)            
#endregion MAIN
