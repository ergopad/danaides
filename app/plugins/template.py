import asyncio
import os, sys, signal
import pandas as pd
import argparse
import json

from time import sleep, time
from utils.logger import logger, Timer, printProgressBar
from utils.db import eng, text
from utils.ergo import headers, NODE_API
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
from ergo_python_appkit.appkit import ErgoValue

#region INIT
PRETTYPRINT = False
LINELEN = 100
VERBOSE = False
NERGS2ERGS = 10**9
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 5000 # save progress every X blocks
CLEANUP_NEEDED = False
TOKEN_ID=None

import inspect
myself = lambda: inspect.stack()[1][3]
#endregion INIT

#region FUNCTIONS
async def checkpoint():
    try:
        logger.debug(f'checkpoint...')

    except Exception as e:
        logger.error(f'ERR::{myself()}::{e}')
        pass

async def process(args, t):
    try:
        logger.debug(f'processing...')

        # process
        for i in range(3):
            logger.warning(f'processing {i}: {t.split()}...')
            sleep(1)

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
        logger.debug(f'main.app::  process took {sec:0.4f}s...')
        
        return {}
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
if __name__ == '__main__':

    # sanity check    
    try: 
        sql = text(f'''
            select token_id 
            from tokens 
            where token_id = :token_id
        ''')
        with eng.begin() as con:
            res = con.execute(sql, {'token_id': TOKEN_ID}).fetchone()            

        if res['token_id'] != TOKEN_ID:
            logger.error(f'unable to find token...')
            try: sys.exit(0)
            except SystemExit: os._exit(0)            

    except OperationalError as e:
        logger.error(f'unable to find token...')
        try: sys.exit(0)
        except SystemExit: os._exit(0)            

    # setup
    args = cli()

    # create app
    app = App()
    app.init()

    res = asyncio.run(app.process(TOKEN_ID))

    # fin
    app.stop()
    try: sys.exit(0)
    except SystemExit: os._exit(0)            
#endregion MAIN
