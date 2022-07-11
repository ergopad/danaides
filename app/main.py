import asyncio
import os, sys, time, signal
import pandas as pd
import argparse

from utils.db import eng, text
from utils.logger import logger, myself, Timer, printProgressBar, LEIF
from utils.ergo import get_node_info, get_genesis_block, headers, NODE_URL, NODE_APIKEY
from utils.aioreq import get_json_ordered
from plugins import staking, tokenomics, utxo, prices
from ergo_python_appkit.appkit import ErgoAppKit, ErgoValue

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

PLUGINS = dotdict({
    'staking': True, 
    'tokenomics': True,
    'utxo': True,
    'prices': True,
})

parser = argparse.ArgumentParser()
# parser.add_argument("-V", "--verbose", help="Wordy", action='store_true')
parser.add_argument("-J", "--juxtapose", help="Alternative table name", default='boxes')
parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
parser.add_argument("-T", "--truncate", help="Truncate boxes table", action='store_true')
parser.add_argument("-P", "--prettyprint", help="Progress bar vs. scrolling", action='store_true')
parser.add_argument("-O", "--once", help="When complete, finish", action='store_true')
args = parser.parse_args()

PRETTYPRINT = args.prettyprint
VERBOSE = False
FETCH_INTERVAL = 1500

sql = f'''
    select ergo_tree, token_id
    from token_agg
'''
logger.debug('Find config...')
config = eng.execute(sql).fetchall()
TOKEN_AMOUNTS = {}
for cfg in config:
    TOKEN_AMOUNTS[cfg['ergo_tree']] = cfg['token_id']

headers = {'Content-Type': 'application/json', 'api_key': NODE_APIKEY}
blips = []

#region FUNCTIONS
# remove all inputs from current block
async def del_inputs(inputs: dict, unspent: dict, height: int = -1) -> dict:
    new = unspent
    for i in inputs:
        box_id = i['boxId']
        try:
            # new[box_id] = height
            new[box_id] = {
                'height': height,
                'nergs': 0
            }
        except Exception as e:
            blips.append({'box_id': box_id, 'height': height, 'msg': f'cant remove'})
            if VERBOSE: logger.warning(f'cant find {box_id} at height {height} while removing from unspent {e}')
    return new

# add all outputs from current block
async def add_outputs(outputs: dict, unspent: dict, height: int = -1) -> dict:
    new = unspent
    for o in outputs:
        box_id = o['boxId']
        nergs = o['value']
        # amount = o['value']
        try:
            # new[box_id] = height
            new[box_id] = {
                'height': height,
                'nergs': nergs
            }
        except Exception as e:
            blips.append({'box_id': box_id, 'height': height, 'msg': f'cant add'})
            if VERBOSE: logger.warning(f'{box_id} exists at height {height} while adding to unspent {e}')
    return new

async def find_tokens(transactions: dict, tokens: dict, height: int = -1) -> dict:
    new = tokens
    
    # find all input box ids
    for tx in transactions:
        input_boxes = [i['boxId'] for i in tx['inputs']]
        for o in tx['outputs']:
            for a in o['assets']:
                try:
                    if a['tokenId'] in input_boxes:
                        token_id = a['tokenId'] # this is also the box_id
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
                    blips.append({'asset': a, 'height': height, 'msg': f'invalid asset while looking for tokens'})
                    logger.warning(f'invalid asset, {a} at height {height} while fetching tokens {e}')
                    pass

    return new

# upsert current chunk
async def checkpoint(height, unspent, tokens, boxes_tablename='boxes', tokens_tablename='boxes'):
    try:
        # unspent
        df = pd.DataFrame.from_dict({
            'box_id': list(unspent.keys()), 
            'height': [n['height'] for n in unspent.values()], # list(map(int, unspent.values())), # 'nergs': list(map(int, [v[0] for v in unspent.values()])), 
            'nerg': [n['nergs'] for n in unspent.values()],
            'is_unspent': [n['height']!=-1 for n in unspent.values()], # [b!=-1 for b in list(unspent.values())]
        })
        # logger.info(df)
        df.to_sql(f'checkpoint_{boxes_tablename}', eng, if_exists='replace')

        # execute as transaction
        with eng.begin() as con:
            # remove spent
            sql = f'''
                with spent as (
                    select box_id
                    from checkpoint_{boxes_tablename}
                    where is_unspent::boolean = false
                )
                delete from {boxes_tablename} t
                using spent s
                where s.box_id = t.box_id
            '''
            if VERBOSE: logger.debug(sql)
            con.execute(sql)

            # add unspent
            sql = f'''
                insert into {boxes_tablename} (box_id, height, is_unspent, nerg)
                    select c.box_id, c.height, c.is_unspent, c.nerg
                    from checkpoint_{boxes_tablename} c
                        left join {boxes_tablename} b on b.box_id = c.box_id
                    where c.is_unspent::boolean = true
                        and b.box_id is null
                    ;
            '''
            if VERBOSE: logger.debug(sql)
            con.execute(sql)

            sql = f'''
                insert into audit_log (height, service)
                values ({int(height)-1}, '{boxes_tablename}')
            '''
            if VERBOSE: logger.debug(sql)
            con.execute(sql)

        # tokens
        df = pd.DataFrame.from_dict({
            'token_id': list(tokens.keys()), 
            'height': [n['height'] for n in tokens.values()], 
            'amount': [n['amount'] for n in tokens.values()],
            'token_name': [n['token_name'] for n in tokens.values()],
            'decimals': [n['decimals'] for n in tokens.values()], 
        })
        # logger.info(df)
        df.to_sql(f'checkpoint_{tokens_tablename}', eng, if_exists='replace')

        # execute as transaction
        with eng.begin() as con:
            # add unspent
            sql = f'''
                insert into {tokens_tablename} (token_id, height, amount, token_name, decimals)
                    select c.token_id, c.height, c.amount, c.token_name, c.decimals
                    from checkpoint_{tokens_tablename} c
                    ;
            '''
            if VERBOSE: logger.debug(sql)
            con.execute(sql)

            sql = f'''
                insert into audit_log (height, service)
                values ({int(height)-1}, '{tokens_tablename}')
            '''
            if VERBOSE: logger.debug(sql)
            con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: checkpointing {e}')
        pass        

### MAIN
async def process(args, t):
    # find unspent boxes at current height
    node_info = get_node_info()
    current_height = node_info['fullHeight']
    unspent = {}
    tokens = {}
    last_height = -1
    tokens_tablename = 'tokens_alt'
    boxes_tablename = ''.join([i for i in args.juxtapose if i.isalpha()]) # only-alpha tablename

    # init or pull boxes from sql into unspent    
    if args.truncate:
        logger.warning('Truncate requested...')
        sql = text(f'''truncate table {boxes_tablename}''')
        eng.execute(sql)
        sql = text(f'''insert into audit_log (height, service) values (0, '{boxes_tablename}')''')
        eng.execute(sql)
    
    # begin at blockchain height from cli
    if args.height >= 0:
        # start from argparse
        logger.info(f'Starting at block; resetting tables {boxes_tablename}, {tokens_tablename}: {args.height}...')
        last_height = args.height
        with eng.begin() as con:
            sql = text(f'''delete from {tokens_tablename} where height >= {args.height}''')
            con.execute(sql)
            sql = text(f'''delete from {boxes_tablename} where height >= {args.height}''')
            con.execute(sql)

    # where to begin reading blockchain
    else:
        # check existing height in audit_log
        sql = f'''
            select height 
            from audit_log 
            where service in ('{boxes_tablename}', '{tokens_tablename}')
            order by created_at desc 
            limit 1
        '''
        ht = eng.execute(sql).fetchone()
        if ht is not None:
            if ht['height'] > 0:
                last_height = ht['height']+1
                logger.info(f'Existing boxes found, starting at {last_height}...')
    
        # nothing found, start from beginning
        else:
            res = get_genesis_block()
            genesis_blocks = res.json()
            for gen in genesis_blocks:
                box_id = gen['boxId']
                unspent[box_id] = {} # height 0

    # lets gooooo...
    try:
        if last_height >= current_height:
            logger.warning('Already caught up...')
        while last_height < current_height:
            next_height = last_height+FETCH_INTERVAL
            if next_height > current_height:
                next_height = current_height
            batch_order = range(last_height, next_height)

            # find block headers
            suffix = f'''BLOCKS: last: {last_height}/{next_height}, max: {current_height}             '''
            if PRETTYPRINT: printProgressBar(last_height, current_height, prefix=f'{t.split()}s', suffix=suffix, length = 50)
            else: logger.info(suffix)
            urls = [[blk, f'{NODE_URL}/blocks/at/{blk}'] for blk in batch_order]
            block_headers = await get_json_ordered(urls, headers)

            # find transactions
            suffix = f'''Transactions: {last_height}/{next_height}..{current_height}             '''
            if PRETTYPRINT: printProgressBar(int(last_height+(FETCH_INTERVAL/3)), current_height, prefix=f'{t.split()}s', suffix=suffix, length = 50)
            else: logger.info(suffix)
            urls = [[hdr[1], f'''{NODE_URL}/blocks/{hdr[2][0]}/transactions'''] for hdr in block_headers if hdr[1] != 0]
            blocks = await get_json_ordered(urls, headers)

            # recreate blockchain (must put together in order)
            suffix = f'''Filter Unspent: {last_height}/{next_height}..{current_height}              '''
            if PRETTYPRINT: printProgressBar(int(last_height+(2*FETCH_INTERVAL/3)), current_height, prefix=f'{t.split()}s', suffix=suffix, length = 50)
            else: logger.info(suffix)
            for blk, transactions in sorted([[b[1], b[2]] for b in blocks]):
                for tx in transactions['transactions']:
                    unspent = await del_inputs(tx['inputs'], unspent)
                    unspent = await add_outputs(tx['outputs'], unspent, blk)
                    tokens = await find_tokens(transactions['transactions'], tokens, blk)

            # checkpoint
            if VERBOSE: logger.debug('Checkpointing...')
            last_height += FETCH_INTERVAL
            suffix = f'Checkpoint at {next_height}...                  '
            if PRETTYPRINT: printProgressBar(next_height, current_height, prefix=f'{t.split()}s', suffix=suffix, length=50)
            else: logger.info(suffix)
            await checkpoint(next_height, unspent, tokens, boxes_tablename, tokens_tablename)
                
            unspent = {}

    except KeyboardInterrupt:
        logger.error('Interrupted.')
        try: sys.exit(0)
        except SystemExit: os._exit(0)         

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')

    return {
        'current_height' : current_height,
        'blips': blips
    }

class App:
    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print('Received:', signum)
        self.shutdown = True

    def start(self):
        logger.info("Begin...")

    def stop(self):
        logger.info("Fin.")

    # find all new currnet blocks
    async def process_unspent(self, args):
        # init timer
        t = Timer()
        t.start()

        # process
        res = await process(args, t)
        
        # timer
        sec = t.stop()
        logger.debug(f'Danaides update took {sec:0.4f}s...')
        
        return res['current_height']

    # wait for new height
    async def hibernate(self, last_height):
        current_height = last_height
        infinity_counter = 0
        t = Timer()
        t.start()

        # admin overview
        sql = f'''
                  select count(*) as i, 0 as j, 'assets' as tbl from assets
            union select count(*), 0, 'tokenomics' from tokens_tokenomics
            union select count(*), max(height), 'staking' from addresses_staking
            union select count(*), 0, 'vesting' from vesting
			union select count(*), max(height), 'utxos' from utxos
            union select count(*), max(height), 'boxes' from boxes
            union select count(*), max(height), 'tokens' from tokens_alt
        '''
        with eng.begin() as con:
            res = con.execute(sql) 
        logger.warning('\n'.join([f'''{r['tbl']}: {r['i']} rows, {r['j']} max height''' for r in res]))

        # chill for next block
        logger.info('Waiting for next block...')
        while last_height == current_height:
            inf = get_node_info()
            current_height = inf['fullHeight']

            if PRETTYPRINT: 
                print(f'''\r({current_height}) {t.split()} Waiting for next block{'.'*(infinity_counter%4)}    ''', end = "\r")
                infinity_counter += 1

            time.sleep(1)

        # cleanup audit log
        logger.debug('Cleanup audit log...')
        sql = f'''
            delete 
            -- select *
            from audit_log 
            where created_at < now() - interval '3 days';            
        '''
        with eng.begin() as con:
            con.execute(sql)

        sec = t.stop()
        logger.log(LEIF, f'Block took {sec:0.4f}s...')        

### MAIN
if __name__ == '__main__':
    app = App()
    app.start()
    
    # main loop
    # args.juxtapose = 'jux' # testing
    # args.once = True # testing
    # logger.debug(args); exit(1)
    while not app.shutdown:
        try:
            # process unspent
            last_height = asyncio.run(app.process_unspent(args))
            args.height = -1 # tell process to use audit_log

            ##
            ## PLUGINS
            ##            

            # PRICES
            if PLUGINS.prices:
                logger.warning('PLUGIN: Prices...')
                asyncio.run(prices.process())

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
                use_checkpoint = last_staking_block == last_height
                logger.debug('Sync main and staking plugin...')
                asyncio.run(staking.process(last_staking_block, use_checkpoint=use_checkpoint, boxes_tablename=args.juxtapose))

            # TOKENOMICS
            if PLUGINS.tokenomics:
                logger.warning('PLUGIN: Tokenomics...')
                asyncio.run(tokenomics.process(use_checkpoint=True))

            # UTXOS
            if PLUGINS.utxo:
                logger.warning('PLUGIN: Utxo...')
                # call last_height-1 as node info returns currently in progress block height, and plugin needs to process the previous block
                asyncio.run(utxo.process(last_height=last_height-1, use_checkpoint=True, boxes_tablename=args.juxtapose))

            # quit or wait for next block
            if args.once:
                app.stop()
                try: sys.exit(0)
                except SystemExit: os._exit(0)            
            else:
                asyncio.run(app.hibernate(last_height))

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
