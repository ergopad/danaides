import asyncio
import pandas as pd
import argparse

from time import sleep 
# from sqlalchemy import create_engine, text
from utils.logger import logger, Timer, printProgressBar
from utils.db import eng, text
from utils.ergo import get_node_info, headers, NODE_APIKEY, NODE_URL
from utils.aioreq import get_json, get_json_ordered
from requests import get
from os import getenv
from base58 import b58encode
from pydantic import BaseModel
from ergo_python_appkit.appkit import ErgoValue

parser = argparse.ArgumentParser()
parser.add_argument("-J", "--juxtapose", help="Alternative table name", default='boxes')
parser.add_argument("-T", "--truncate", help="Truncate boxes table", action='store_true')
parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
parser.add_argument("-E", "--endat", help="End at this height", type=int, default=10**10)
parser.add_argument("-P", "--prettyprint", help="Begin at this height", action='store_true')
args = parser.parse_args()

# ready, go
PRETTYPRINT = args.prettyprint
VERBOSE = False
NERGS2ERGS = 10**9
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 5000 # save progress every X blocks

blips = []

async def checkpoint(balances, eng, checkpoint_tablename='balances'):
    # addresses
    addrtokens = {'address': [], 'nergs': [], 'box_id': [], 'height': []}
    addr_converter = {}
    for raw, tokens in balances.items():
        r2a = get(f'''{NODE_URL}/utils/rawToAddress/{raw}''', headers=headers, timeout=2)
        pubkey = ''
        if r2a.ok:
            pubkey = r2a.json()['address']
            addr_converter[raw] = pubkey

        for token in tokens:            
            addrtokens['address'].append(pubkey)
            addrtokens['nergs'].append(token['nergs'])
            addrtokens['box_id'].append(token['box_id'])
            addrtokens['height'].append(token['height'])
    df_balances = pd.DataFrame().from_dict(addrtokens)
    df_balances.to_sql(f'checkpoint_{checkpoint_tablename}', eng, if_exists='replace')

    with eng.begin() as con:
        # balances
        sql = f'''
            delete from balances_by_address where box_id in (
                select box_id::text
                from checkpoint_{checkpoint_tablename}
            )
        '''
        # con.execute(sql)

        sql = f'''
            insert into balances_by_address (address, nergs, box_id, height)
                select address, nergs, box_id, height
                from checkpoint_{checkpoint_tablename}
        '''
        con.execute(sql)

async def process(last_height, boxes_tablename:str = 'boxes', box_override=''):
    t = Timer()
    t.start()

    # manual boxes tablename
    # box_override = '331a963bbb33542f347aac7be1259980b08284e9a54dcf21e60342104820ba65'
    # box_override = 'ef7365a0d1817873e1f8e537ed0cc4dd32f80beb7f3f71799fb1a7da5f7d1802'
    boxes_tablename = ''.join([i for i in boxes_tablename if i.isalpha()]) # only-alpha tablename

    sql = f'''
        select height 
        from audit_log 
        where service = 'balances'
        order by created_at desc 
        limit 1
    '''
    res = eng.execute(sql).fetchone()
    if res is not None:
        last_height = res['height']
    else:
        last_height = 0  

    # remove spent boxes from balances tables
    logger.info('Remove spent boxes from balances tables...')
    with eng.begin() as con:
        # balances            
        sql = f'''
            with spent as (
                select a.box_id, a.height
                from balances_by_address a
                    left join {boxes_tablename} b on a.box_id = b.box_id
                where b.box_id is null
            )
            delete from balances_by_address t
            using spent s
            where s.box_id = t.box_id
                and s.height = t.height
        '''
        if VERBOSE: logger.debug(sql)
        con.execute(sql)

        '''
        to find unspent boxes to process
        1. if there is a an override set, grab that box
        2. if incremental, pull boxes above certain height
        4. or, just pull all unspent boxes and process in batches
        '''
        sql = f'''
            select box_id, height, row_number() over(partition by is_unspent order by height) as r
            from {boxes_tablename}
        '''
        if box_override != '':
            sql += f'''where box_id in ('{box_override}')'''
        elif True:
            sql += f'''where height >= {last_height}'''

    # find boxes from checkpoint or standard sql query
    if VERBOSE: logger.debug(sql)
    boxes = eng.execute(sql).fetchall()
    box_count = len(boxes)    

    balance_counter = 0
    max_height = 0
    last_r = 1

    logger.info(f'Begin processing, {box_count} boxes total...')

    # process all new, unspent boxes
    for r in range(last_r-1, box_count, CHECKPOINT_INTERVAL):
        next_r = r+CHECKPOINT_INTERVAL-1
        if next_r > box_count:
            next_r = box_count

        suffix = f'''{t.split()} :: ({balance_counter}) Process ...'''+(' '*80)
        if PRETTYPRINT: printProgressBar(r, box_count, prefix='Progress:', suffix=suffix, length=50)
        else: logger.debug(suffix)

        try:
            balances = {}

            urls = [[box['height'], f'''{NODE_URL}/utxo/byId/{box['box_id']}'''] for box in boxes[r:next_r]]

            # ------------
            # primary loop
            # ------------
            utxo = await get_json_ordered(urls, headers)
            for address, box_id, nergs, height in [[u[2]['ergoTree'], u[2]['boxId'], u[2]['value'], u[1]] for u in utxo if u[0] == 200]:
                # find height for audit (and checkpoint?)
                if VERBOSE: logger.debug(f'box: {box_id}')
                if height > max_height:
                    max_height = height
                
                # find balances
                retries = 0
                while retries < 5:
                    if retries > 0: logger.warning(f'retry: {retries}')
                    if VERBOSE: logger.debug(f'address: {address}')
                    
                    # wallet
                    if address[:6] == '0008cd':
                        raw = address[6:]
                        if VERBOSE: logger.debug(f'found: {raw}')
                        if raw not in balances:
                            balances[raw] = []

                        balances[raw].append({
                                'nergs': nergs,
                                'address': raw,
                                'box_id': box_id,
                                'height': height,
                            })

                    retries = 5

            balance_counter += len(balances)

            # save current unspent to sql
            suffix = f'{t.split()} :: ({balance_counter}) Checkpoint ({len(balances)} new adrs)...'+(' '*80)
            if PRETTYPRINT: printProgressBar(next_r, box_count, prefix='Progress:', suffix=suffix, length=50)
            else: logger.info(suffix)
            await checkpoint(balances, eng)

            # track balances height here (since looping through boxes)
            notes = f'''{len(balances)} balances'''
            sql = f'''
                insert into audit_log (height, service, notes)
                values ({max_height}, 'balances', '{notes}')
            '''
            eng.execute(sql)

            # reset for outer loop: height range
            last_r = r
            balances = {}
            if box_override != '':
                exit(1)

        except KeyError as e:
            logger.error(f'ERR (KeyError): {e}; {box_id}')
            pass
        
        except Exception as e:
            logger.error(f'ERR: {e}; {box_id}')
            retries += 1
            sleep(1)
            pass

    # cleanup
    try:
        logger.debug('Cleanup balances tables...')
        sql = f'''
            with dup as (    
                select id, address, box_id, nergs, height, row_number() over(partition by address, box_id, nergs, height order by id desc) as r
                from balances_by_address 
                -- where address != ''
            )
            delete from balances_by_address where id in (
                select id
                from dup 
                where r > 1 
            )
        '''
        eng.execute(sql)

    except Exception as e:
        logger.debug(f'ERR: cleaning dups {e}')
        pass

    sec = t.stop()
    logger.debug(f'Processing complete: {sec:0.4f}s...                ')

    eng.dispose()
    return {
        'box_count': box_count,
    }

async def hibernate():
    inf = get_node_info()
    last_height = inf['fullHeight']
    current_height = last_height
    hibernate_timer = Timer()
    hibernate_timer.start()

    logger.info('Waiting for next block...')
    infinity_counter = 0
    while last_height == current_height:
        inf = get_node_info()
        current_height = inf['fullHeight']

        if PRETTYPRINT: 
            print(f'''\r{current_height} :: {hibernate_timer.split()} Waiting for next block{'.'*(infinity_counter%4)}    ''', end = "\r")
            infinity_counter += 1

        sleep(1)

    sec = hibernate_timer.stop()
    logger.debug(f'Next block {sec:0.4f}s...')     
    sleep(5) # hack, make sure we wait long enough to let unspent boxes to refresh
    return last_height

#endregion FUNCTIONS

async def main(args):
    args.juxtapose = 'jux'
    last_height = args.height

    while True:
        res = await process(last_height, boxes_tablename=args.juxtapose)
        last_height = await hibernate()

if __name__ == '__main__':
    res = asyncio.run(main(args))
