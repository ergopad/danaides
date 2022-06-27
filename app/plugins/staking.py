import asyncio
import pandas as pd
import argparse

from time import sleep 
from sqlalchemy import create_engine, text
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
CHECKPOINT_INTERVAL = 250 # save progress every X blocks

blips = []

async def checkpoint(addresses, keys_found, eng, staking_tablename='staking'):
    # addresses
    addrtokens = {'address': [], 'token_id': [], 'amount': [], 'box_id': [], 'height': []}
    addr_counter = {}
    addr_converter = {}
    for raw, tokens in addresses.items():
        r2a = get(f'''{NODE_URL}/utils/rawToAddress/{raw}''', headers=headers, timeout=2)
        pubkey = ''
        if r2a.ok:
            pubkey = r2a.json()['address']
            addr_converter[raw] = pubkey

        for token in tokens:            
            addrtokens['address'].append(pubkey)
            addrtokens['token_id'].append(token['token_id'])
            addrtokens['amount'].append(token['amount'])
            addrtokens['box_id'].append(token['box_id'])
            addrtokens['height'].append(token['height'])
            addr_counter[pubkey] = 1
    df_addresses = pd.DataFrame().from_dict(addrtokens)
    df_addresses.to_sql(f'checkpoint_addresses_{staking_tablename}', eng, if_exists='replace')

    # stake keys
    df_keys_staking = pd.DataFrame().from_dict({
        'box_id': list(keys_found.keys()), 
        'token_id': [x['token_id'] for x in keys_found.values()],
        'amount': [x['amount'] for x in keys_found.values()],
        'penalty': [x['penalty'] for x in keys_found.values()],
        'address': [addr_converter[x['address']] for x in keys_found.values()],
        'stakekey_token_id': [x['stakekey_token_id'] for x in keys_found.values()],
        'height': [x['height'] for x in keys_found.values()],
    })
    df_keys_staking.to_sql(f'checkpoint_keys_{staking_tablename}', eng, if_exists='replace')

    with eng.begin() as con:
        # addresses
        sql = f'''
            delete from addresses_staking where box_id in (
                select box_id::text
                from checkpoint_addresses_{staking_tablename}
            )
        '''
        con.execute(sql)

        sql = f'''
            insert into addresses_staking (address, token_id, amount, box_id)
                select address, token_id, amount, box_id
                from checkpoint_addresses_{staking_tablename}
        '''
        con.execute(sql)

        # staking (keys)
        sql = f'''
            delete from keys_staking where box_id in (
                select box_id::text
                from checkpoint_keys_{staking_tablename}
            )
        '''
        con.execute(sql)

        sql = f'''
            insert into keys_staking (box_id, token_id, amount, stakekey_token_id, penalty, address)
                select box_id, token_id, amount, stakekey_token_id, penalty, address
                from checkpoint_keys_{staking_tablename}
        '''
        con.execute(sql)

async def process(last_height, use_checkpoint = False, boxes_tablename:str = 'boxes', box_override=''):
    args.juxtapose = 'jux'
    t = Timer()
    t.start()

    boxes_tablename = ''.join([i for i in args.juxtapose if i.isalpha()]) # only-alpha tablename
    sql = '''
        select stake_ergotree, stake_token_id, token_name, token_id, token_type, emission_amount, decimals 
        from tokens
    '''

    # find all stake keys
    STAKE_KEYS = {}
    res = eng.execute(sql).fetchall()
    for key in res:
        STAKE_KEYS[key['stake_ergotree']] = {
            'stake_token_id': key['stake_token_id'],
            'token_name': key['token_name'],
            'token_type': key['token_type'],
            'emission_amount': key['emission_amount'],
            'decimals': key['decimals'],
        }

    # call as library
    if use_checkpoint:
        # remove spent boxes from staking tables
        logger.info('Remove spent boxes...')
        with eng.begin() as con:
            # addresses
            sql = f'''
                -- remove spent boxes from addresses_staking from current boxes checkpoint
                delete 
                from addresses_staking 
                where box_id in (
                    select box_id
                    from checkpoint_{boxes_tablename}
                    -- where is_unspent::boolean = false -- remove all; unspent will reprocess below??
                )
            '''
            con.execute(sql)

            sql = f'''
                -- remove spent boxes from keys_staking from current boxes checkpoint
                delete 
                from keys_staking 
                where box_id in (
                    select box_id
                    from checkpoint_{boxes_tablename}
                    -- where is_unspent::boolean = false -- remove all; unspent will reprocess below??
                )
            '''
            con.execute(sql)

            # find newly unspent boxes
            sql = f'''
                select distinct b.box_id, height
                from checkpoint_{boxes_tablename}
                    where is_unspent::boolean = true
            '''
            logger.info(f'Fetching all unspent boxes{msg}...')
            boxes = eng.execute(sql).fetchall()

    # process as standalone call
    else:
        # box_override is for testing
        # box_override = '331a963bbb33542f347aac7be1259980b08284e9a54dcf21e60342104820ba65'
        # box_override = 'ef7365a0d1817873e1f8e537ed0cc4dd32f80beb7f3f71799fb1a7da5f7d1802'
        if box_override == '':
            # where to start? find boxes with height > ...
            #   1. if height from cli, use it
            #   2. else check audit_log/staking for last height
            #   3. otherwise do all boxes
            if last_height >= 0:
                logger.info(f'Block: {last_height}...')

            else:
                sql = f'''
                    select height 
                    from audit_log 
                    where service = 'staking'
                    order by created_at desc 
                    limit 1
                '''
                res = eng.execute(sql).fetchone()
                if res is not None:
                    last_height = res['height']
                else:
                    last_height = -1   

            sql = f'''
                select box_id, b.height
                from {boxes_tablename}
                where height > {last_height}
            '''

        # !! used for testing; just processes single box
        else:
            # box override
            logger.info(f'Box override {box_override}...')
            sql = f'''
                select box_id, height
                from {boxes_tablename} 
                where box_id in ('{box_override}')
            '''

        # from last_height to new_height; by boxes
        sql = f'''
            select max(height) as height
            from {boxes_tablename}
        '''
        max_height = eng.execute(sql).fetchone()['height']

        # remove spent boxes from staking tables
        logger.info('Remove spent boxes...')
        with eng.begin() as con:
            # addresses
            sql = f'''
                -- remove spent boxes from addresses
                delete 
                from addresses_staking 
                where box_id in (
                    select a.box_id 
                    from addresses_staking a
                        left join boxes b on b.box_id = a.box_id
                    where b.box_id is null
                )
            '''
            con.execute(sql)

            sql = f'''
                -- remove spent boxes from keys_staking
                delete 
                from keys_staking 
                where box_id in (
                    select a.box_id 
                    from keys_staking a
                        left join boxes b on b.box_id = a.box_id
                    where b.box_id is null
                )
            '''
            con.execute(sql)

    '''
    for all unspent boxes
    1. find all legit stake keys (using stake tree/id), and amount
    2. find all addresses with a token
    3. find ergs
    4. last updated
    '''
    suffix = f'''Begin processing at: {last_height}; current node height: {max_height}'''
    if PRETTYPRINT: printProgressBar(last_height, max_height, prefix='Progress:', suffix=suffix, length=50)
    else: logger.debug(suffix)

    stakekey_counter = 0    
    box_count = 0
    key_counter = 0
    address_counter = 0
    for current_height in range(last_height+CHECKPOINT_INTERVAL, max_height+CHECKPOINT_INTERVAL, CHECKPOINT_INTERVAL):
        # find newly unspent boxes
        next_height = current_height-1
        if next_height > max_height:
            next_height = max_height-1
        logger.debug(f'Find unspent boxes between {last_height} and {next_height}')
        sql = f'''
            select box_id, height
            from {boxes_tablename}
            where height between {last_height} and {next_height}
        '''
        if box_override != '':
            sql = f'''
                    select box_id, height
                    from {boxes_tablename} 
                    where box_id in ('{box_override}')
                '''
        # find boxes to search
        boxes = eng.execute(sql).fetchall()
        box_count = len(boxes)

        suffix = f'''Find stake keys {last_height}/{next_height} [{key_counter} keys/{address_counter} adrs] {t.split()}                '''
        if PRETTYPRINT: printProgressBar(last_height, max_height, prefix='Progress:', suffix=suffix, length=50)
        else: logger.debug(suffix)

        try:
            keys_found = {}
            addresses = {}

            urls = [[box['height'], f'''{NODE_URL}/utxo/byId/{box['box_id']}'''] for box in boxes]

            utxo = await get_json_ordered(urls, headers)
            for address, box_id, assets, registers, height in [[u[2]['ergoTree'], u[2]['boxId'], u[2]['assets'], u[2]['additionalRegisters'], u[1]] for u in utxo if u[0] == 200]:
                retries = 0
                while retries < 5:
                    if retries > 0:
                        logger.warning(f'retry: {retries}')
                    if VERBOSE: logger.debug(address)
                    raw = address[6:]
                    if VERBOSE: logger.warning(address)
                    if address in STAKE_KEYS:   
                        stake_token_id = STAKE_KEYS[address]['stake_token_id']
                        # found ergopad staking key
                        if VERBOSE: logger.warning(assets[0]['tokenId'])
                        if assets[0]['tokenId'] == stake_token_id:
                            logger.debug(f'found ergopad staking token in box: {box_id}')
                            stakekey_counter += 1
                            try: R4_1 = ErgoValue.fromHex(registers['R4']).getValue().apply(1)
                            except: logger.warning(f'R4 not found: {box_id}')
                            keys_found[box_id] = {
                                'stakekey_token_id': registers['R5'][4:], # TODO: validate that this starts with 0e20 ??
                                'amount': assets[1]['amount'],
                                'token_id': stake_token_id,
                                'penalty': int(R4_1),
                                'address': raw,
                                'height': height,
                            }                            

                    # store assets by address
                    if len(assets) > 0:
                        # init for address
                        if raw not in addresses:
                            addresses[raw] = []
                        
                        # save assets
                        for asset in assets:
                            addresses[raw].append({
                                'token_id': asset['tokenId'], 
                                'amount': asset['amount'],
                                'box_id': box_id,
                                'height': height,
                            })
                    
                    retries = 5

            key_counter += len(keys_found)
            address_counter += len(addresses)

            # save current unspent to sql
            suffix = f'Checkpoint at: {next_height}...'+(' '*80)
            if PRETTYPRINT: printProgressBar(next_height, max_height, prefix='Progress:', suffix=suffix, length=50)
            else: logger.info(suffix)
            await checkpoint(addresses, keys_found, eng)

            # track staking height here (since looping through boxes)
            notes = f'''{len(addresses)} addresses, {len(keys_found)} keys'''
            sql = f'''
                insert into audit_log (height, service, notes)
                values ({next_height}, 'staking', '{notes}')
            '''
            eng.execute(sql)

            # reset for outer loop: height range
            last_height = current_height
            addresses = {}
            keys_found = {}
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
    last_height = args.height
    end_height = args.endat

    while True:
        res = await process(last_height)
        last_height = await hibernate()

if __name__ == '__main__':
    res = asyncio.run(main(args))
