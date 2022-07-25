import asyncio
import os, sys, time, signal
import pandas as pd
import argparse

from utils.db import eng, text
from utils.logger import logger, myself, Timer, printProgressBar, LEIF
from utils.ergo import get_node_info, get_genesis_block, NODE_URL, NODE_APIKEY
from utils.aioreq import get_json_ordered
from ergo_python_appkit.appkit import ErgoAppKit, ErgoValue

#region INIT
PRETTYPRINT = False
VERBOSE = False # args.verbose
NERGS2ERGS = 10**9
BOXES = 'boxes'
TOKENS = 'tokens'

# upsert current chunk
async def checkpoint(height, tokens):
    try:
        suffix = f'''TOKENS: Found {len(tokens)} tokens this block...                      '''
        # if PRETTYPRINT: printProgressBar(last_height, height, prefix='[TOKENS]', suffix=suffix, length = 50)
        # else: logger.info(suffix)
        df = pd.DataFrame.from_dict({
            'token_id': list(tokens.keys()), 
            'height': [n['height'] for n in tokens.values()], 
            'amount': [n['amount'] for n in tokens.values()],
            'token_name': [n['token_name'] for n in tokens.values()],
            'decimals': [n['decimals'] for n in tokens.values()], 
        })
        # logger.info(df)
        df.to_sql(f'checkpoint_{TOKENS}', eng, if_exists='replace')

        # execute as transaction
        with eng.begin() as con:
            # add unspent
            sql = f'''
                insert into {TOKENS} (token_id, height, amount, token_name, decimals)
                    select c.token_id, c.height, c.amount, c.token_name, c.decimals
                    from checkpoint_{TOKENS} c
                        left join {TOKENS} t on t.token_id = c.token_id
                    -- unique constraint; but want all others
                    where t.token_id is null
                    ;
            '''
            if VERBOSE: logger.debug(sql)
            con.execute(sql)

            sql = f'''
                insert into audit_log (height, service, notes)
                values ({int(height)-1}, '{TOKENS}', '{len(tokens)} found')
            '''
            if VERBOSE: logger.debug(sql)
            con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: checkpointing {e}')
        pass  

# 
async def process(transactions: dict, tokens: dict, height: int, is_plugin: bool=False) -> dict:
    try:
        # find all input box ids
        new = tokens
        for tx in transactions:
            input_boxes = [i['boxId'] for i in tx['inputs']]
            for o in tx['outputs']:
                for a in o['assets']:
                    try:
                        if a['tokenId'] in input_boxes:
                            token_id = a['tokenId'] # this is also the box_id
                            
                            # if already exists, don't redo work
                            if token_id not in new:
                                try: token_name = ''.join([chr(r) for r in ErgoAppKit.deserializeLongArray(o['additionalRegisters']['R4'])])
                                except: token_name = ''
                                if 'R6' in o['additionalRegisters']:
                                    try: decimals = ''.join([chr(r) for r in ErgoAppKit.deserializeLongArray(o['additionalRegisters']['R6'])])
                                    except: decimals = ErgoValue.fromHex(o['additionalRegisters']['R6']).getValue()
                                else:
                                    decimals = 0
                                try: decimals = int(decimals)
                                except: decimals = 0
                                try: amount = int(a['amount'])
                                except: pass
                                # some funky deserialization issues
                                if type(amount) == int:
                                    if VERBOSE: logger.debug(f'''token found: {token_name}/{decimals}/{token_id}/{amount}''')
                                    new[token_id] = {
                                        'height': height,
                                        'token_name': token_name,
                                        'decimals': decimals,
                                        'amount': amount
                                    }
                    except Exception as e:
                        # BLIPS.append({'asset': a, 'height': height, 'msg': f'invalid asset while looking for tokens'})
                        logger.warning(f'invalid asset, {a} at height {height} while fetching tokens {e}')
                        pass

        return new

    except Exception as e:
        logger.error(f'ERR: find tokens {e}')
        return {}

def cli():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("-J", "--juxtapose", help="Alternative table name", default='boxes')
    parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
    parser.add_argument("-P", "--prettyprint", help="Progress bar vs. scrolling", action='store_true')
    parser.add_argument("-O", "--once", help="When complete, finish", action='store_true')
    parser.add_argument("-V", "--verbose", help="Be wordy", action='store_true')
    
    args = parser.parse_args()

    if args.juxtapose != 'boxes': 
        logger.warning(f'Using alt boxes table: {args.juxtapose}...')
        BOXES = ''.join([i for i in args.juxtapose if i.isalpha()]) # only-alpha tablename
    if args.height != -1: logger.warning(f'Starting at height: {args.height}...')
    if args.prettyprint: logger.warning(f'Pretty print...')
    if args.once: logger.warning(f'Processing once, then exit...')
    if args.verbose: logger.warning(f'Verbose...')

    PRETTYPRINT = args.prettyprint
    VERBOSE = args.verbose
    FETCH_INTERVAL = args.fetchinterval    

    return args

class App:
    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum):
        print('Received:', signum)
        self.shutdown = True

    def init(self):
        logger.info("Ready, go...")

    def stop(self):
        logger.info("Fin.")

    # find all new currnet blocks
    async def process(self, args, height):
        # init timer
        t = Timer()
        t.start()

        # process
        res = await process(args, t, height)
        
        # timer
        sec = t.stop()
        logger.debug(f'Danaides update took {sec:0.4f}s...')
        
        return res['current_height']

    # wait for new height
    async def hibernate(self, last_height):

### MAIN
if __name__ == '__main__':
    
    # setup
    args = cli()
    
    # create app
    app = App()
    app.init()
    
    # process loop
    height = args.height # first time
    while not app.shutdown:
        try:
            # build boxes, tokens tables
            height = asyncio.run(app.process(args, height))

            ##
            ## PLUGINS
            ##            

            # UTXOS - process first; builds utxos table
            if PLUGINS.utxo:
                logger.warning('PLUGIN: Utxo...')
                # call last_height-1 as node info returns currently in progress block height, and plugin needs to process the previous block
                asyncio.run(utxo.process(is_plugin=True, args=args))

            # PRICES - builds prices table
            if PLUGINS.prices:
                logger.warning('PLUGIN: Prices...')
                asyncio.run(prices.process(is_plugin=True, args=args))

            # STAKING
            if PLUGINS.staking:
                logger.warning('PLUGIN: Staking...')
                sql = f'''
                    select height 
                    from audit_log 
                    where service = 'staking'
                    order by created_at desc 
                    limit 1
                '''
                res = eng.execute(sql).fetchone()
                last_staking_block = 0  
                if res is not None: 
                    logger.info('Existing staking info found...')
                    last_staking_block = res['height']
                else:
                    logger.info('No staking info found; rebuiding from height 0...')
                use_checkpoint = last_staking_block == height
                logger.debug('Sync main and staking plugin...')
                asyncio.run(staking.process(last_staking_block, use_checkpoint=use_checkpoint))

            # quit or wait for next block
            if args.once:
                app.stop()
                try: sys.exit(0)
                except SystemExit: os._exit(0)            
            else:
                asyncio.run(app.hibernate(height))

        except KeyboardInterrupt:
            logger.debug('Interrupted.')
            app.stop()
            try: sys.exit(0)
            except SystemExit: os._exit(0)            

        except Exception as e:
            logger.error(f'ERR: {myself()}, {e}')

    # fin
    app.stop()
    try: sys.exit(0)
    except SystemExit: os._exit(0)            

