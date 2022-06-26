import asyncio
import os, sys, time, signal
import pandas as pd
import argparse

from utils.db import eng, text
from utils.logger import logger, myself, Timer, printProgressBar
from plugins import staking
from requests import get
from os import getenv
from base58 import b58encode
# from pydantic import BaseModel

from plugins import staking
PLUGINS = {}
# PLUGINS = {'staking': 1}

parser = argparse.ArgumentParser()
parser.add_argument("-J", "--juxtapose", help="Alternative table name", default='boxes')
parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
parser.add_argument("-T", "--truncate", help="Truncate boxes table", action='store_true')
parser.add_argument("-P", "--prettyprint", help="Progress bar vs. scrolling", action='store_true')
parser.add_argument("-O", "--once", help="When complete, finish", action='store_true')
args = parser.parse_args()

PRETTYPRINT = args.prettyprint
VERBOSE = False

NODE_APIKEY = getenv('ERGOPAD_APIKEY')
NODE_URL = f'''http://{getenv('NODE_URL')}:{getenv('NODE_PORT')}'''
NERGS2ERGS = 10**9
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 1000 # save progress every X blocks

headers = {'Content-Type': 'application/json', 'api_key': NODE_APIKEY}
blips = []

#region CLASSES
class UTXO():
    
    height = -1

    def __init__(self) -> None:
        res = get(f'{NODE_URL}/utxo/genesis', headers=headers, timeout=2)
        genesis_blocks = res.json()
        for gen in genesis_blocks:
            self.unspent[gen['boxId']] = gen['value']

    def restore(self) -> None:
        # restore from previous state (sql)
        # set self.height
        pass

    def process(self) -> None:
        # update unspent with next block
        # check if current state is max (current height of node)
        pass

#region FUNCTIONS
def b58(n): 
    return b58encode(bytes.fromhex(n)).decode('utf-8')

def get_node_info():
    res = get(f'{NODE_URL}/info', headers=headers, timeout=2)
    node_info = None
    if not res.ok:
        logger.error(f'unable to retrieve node info: {res.text}')
        exit()
    else:
        node_info = res.json()
        if VERBOSE: logger.debug(node_info)
    
    # return 10000 # testing
    return node_info

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

# upsert current chunk
async def checkpoint(blk, current_height, unspent, eng, boxes_tablename='boxes'):
    suffix = f'Checkpoint at {blk}...'
    if PRETTYPRINT: printProgressBar(blk, current_height, prefix='Progress:', suffix=suffix, length=50)
    else: logger.info(suffix)
    if VERBOSE: logger.info(blk)
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
            delete from {boxes_tablename}
            where box_id in (
                select box_id
                from checkpoint_{boxes_tablename}
                where is_unspent::boolean = false
            );
        '''
        con.execute(sql)

        # track spent
        # sql = f'''
        #     insert into spent_{boxes_tablename} (box_id)
        #         select box_id
        #         from checkpoint_{boxes_tablename}
        #         where is_unspent::boolean = false
        #     ;
        # '''
        # con.execute(sql)

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
        con.execute(sql)

        sql = f'''
            insert into audit_log (height, service)
            values ({int(blk)}, '{boxes_tablename}')
        '''
        con.execute(sql)

### MAIN
async def process_boxes(args, t):
    # find unspent boxes at current height
    node_info = get_node_info()
    current_height = node_info['fullHeight']
    node_version = node_info['appVersion']
    node_network = node_info['network']
    unspent = {}

    # only-alpha tablename
    boxes_tablename = ''.join([i for i in args.juxtapose if i.isalpha()])

    # init or pull boxes from sql into unspent    
    if args.truncate:
        logger.warning('Truncate requested...')
        sql = text(f'''truncate table {boxes_tablename}''')
        eng.execute(sql)
        sql = text(f'''insert into audit_log (height, service) values (0, 'main')''')
        eng.execute(sql)
    
    last_height = -1
    if args.height >= 0:
        # start from argparse
        logger.info(f'Rollback requested to block: {args.height}...')
        last_height = args.height
        sql = text(f'''delete from {boxes_tablename} where height > {args.height}''')
        eng.execute(sql)

    else:
        sql = text(f'''
            select height 
            from audit_log 
            where service = '{boxes_tablename}'
            order by created_at desc 
            limit 1
        ''')
        ht = eng.execute(sql).fetchone()
        
        # start from saved, if exists
        if ht is not None:
            if ht['height'] > 0:
                last_height = ht['height']
                logger.info(f'Existing boxes found, starting at {last_height}...')
    
        # if all else fails, from scratch
        else:
            # in the beginning...
            res = get(f'{NODE_URL}/utxo/genesis', headers=headers, timeout=2)
            if not res.ok:
                logger.error(f'unable to determine genesis blocks {res.text}')

            # init unspent blocks
            genesis_blocks = res.json()
            for gen in genesis_blocks:
                box_id = gen['boxId']
                unspent[box_id] = True # height 0

    # lets gooooo...
    unspent_counter = 0
    if last_height <= current_height:
        if PRETTYPRINT: printProgressBar(last_height, current_height, prefix = 'Progress:', length = 50)
        else: logger.info(f'''Find Unspent Boxes between: {last_height+1}..{current_height} (node: {node_network}/{node_version})''')

        # +1 to include both starting and current in range
        # starting is last block processed, don't reprocess
        for blk in range(last_height+1, current_height+1):
            if VERBOSE: logger.debug(f'{blk}: {len(unspent.keys())}')
            res = get(f'{NODE_URL}/blocks/at/{blk}', headers=headers, timeout=2)
            if not res.ok:
                logger.warning(f'block header request failed for block {blk}; {res.text}')
            else:
                block_headers = res.json()
                for hdr in block_headers:
                    res = get(f'{NODE_URL}/blocks/{hdr}/transactions', headers=headers, timeout=2)
                    if not res.ok:
                        logger.warning(f'block transaction request failed for header {hdr}; {res.text}')
                    else:
                        block_transactions = res.json()['transactions']
                        for tx in block_transactions:
                            if VERBOSE: logger.debug(f'  removing inputs')
                            unspent = await del_inputs(tx['inputs'], unspent)
                            if VERBOSE: logger.debug(f'  adding outputs')
                            unspent = await add_outputs(tx['outputs'], unspent, blk)
                            unspent_counter += len(unspent)
            
            # update progress bar on screen
            if blk%UPDATE_INTERVAL == 0:
                suffix = f'''{blk}/{len(unspent.keys())}/{t.split()} ({len(blips)} blips)'''
                if PRETTYPRINT: printProgressBar(blk, current_height, prefix='Progress:', suffix=suffix, length=50)
                else: logger.info(suffix)                

            # save current unspent to sql
            if (blk%CHECKPOINT_INTERVAL == 0) or (blk == current_height):
                await checkpoint(blk, current_height, unspent, eng, boxes_tablename)
                unspent = {}

                # plugins
                plugin_timer = Timer()
                plugin_timer.start()
                if ('staking' in PLUGINS) and ():
                    suffix = f'Processing Plugin: Staking'
                    if PRETTYPRINT: printProgressBar(blk, current_height, prefix='Progress:', suffix=suffix, length=50)
                    else: logger.debug(suffix)
                    await staking.process(-1, plugin_timer, use_checkpoint=True, boxes_tablename=f'checkpoint_{boxes_tablename}')
                    await staking.process(-1, plugin_timer, use_checkpoint=True, boxes_tablename=f'checkpoint_{boxes_tablename}')
                # if plugin['vesting']:
                #     if PRETTYPRINT: printProgressBar(blk, current_height, prefix='Progress:', suffix='Processing Plugin: Vesting', length=50)
                #     await vesting.process(-1, plugin_timer, use_checkpoint=True, boxes_tablename=f'checkpoint_{boxes_tablename}')
                sec = plugin_timer.stop()

    return {
        'current_height' : current_height,
        'num_unspent_boxes': unspent_counter,
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

    # find all new currnet blocks
    async def process_unspent(self, args):
        t = Timer()
        t.start()
        res = await process_boxes(args, t)
        args.height = -1 # ignore this after first loop
        last_block = res['current_height']        
        if res['num_unspent_boxes'] == 0:
            logger.info(f'''Nothing to process at {last_block}.''')
        else: 
            logger.info(f'''Processing complete for height {last_block}; {res['num_unspent_boxes']} new unspent''')
        sec = t.stop()
        logger.debug(f'Danaides update took {sec:0.4f}s...')
        
        return last_block

    # wait for new height
    async def hibernate(self, last_block):
        current_height = last_block
        infinity_counter = 0
        t = Timer()
        t.start()

        logger.info('Waiting for next block...')
        while last_block == current_height:
            inf = get_node_info()
            current_height = inf['fullHeight']

            if PRETTYPRINT: 
                print(f'''\r({current_height}) {t.split()} Waiting for next block{'.'*(infinity_counter%4)}    ''', end = "\r")
                infinity_counter += 1

            time.sleep(1)

        sec = t.stop()
        logger.debug(f'Block took {sec:0.4f}s...')        

    def stop(self):
        logger.info("Fin.")

### MAIN
if __name__ == '__main__':
    app = App()
    app.start()
    
    # main loop
    # args.juxtapose = 'jux' # testing
    # args.oneanddone = True # testing
    # logger.debug(args); exit(1)
    while not app.shutdown:
        try:
            last_block = asyncio.run(app.process_unspent(args))
            if args.oneanddone:
                app.shutdown()
            else:
                asyncio.run(app.hibernate(last_block))
        except KeyboardInterrupt:
            logger.debug('Interrupted.')
            app.stop()
            try: sys.exit(0)
            except SystemExit: os._exit(0)            
        except Exception as e:
            logger.error(f'ERR: {myself}, {e}')

    # fin
    app.stop()
    try: sys.exit(0)
    except SystemExit: os._exit(0)            
