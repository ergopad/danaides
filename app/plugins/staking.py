import asyncio
import pandas as pd
import argparse

from time import sleep 
from sqlalchemy import create_engine, text
from utils.logger import logger, Timer, printProgressBar
from requests import get
from os import getenv
from base58 import b58encode
from pydantic import BaseModel
from ergo_python_appkit.appkit import ErgoValue

parser = argparse.ArgumentParser()
parser.add_argument("-T", "--truncate", help="Truncate boxes table", action='store_true')
parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
parser.add_argument("-E", "--endat", help="End at this height", type=int, default=10**10)
parser.add_argument("-P", "--prettyprint", help="Begin at this height", action='store_true')
args = parser.parse_args()

PRETTYPRINT = args.prettyprint
VERBOSE = False

DB_DANAIDES = f"postgresql://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DBNM')}"
NODE_APIKEY = getenv('ERGOPAD_APIKEY')
NODE_URL = f'''http://{getenv('NODE_URL')}:{getenv('NODE_PORT')}'''
NERGS2ERGS = 10**9

# ready, go
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 1000 # save progress every X blocks

headers = {'Content-Type': 'application/json', 'api_key': NODE_APIKEY}
blips = []

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

async def checkpoint(blk, box_count, addresses, keys_found, eng, staking_tablename='staking'):
    suffix = f'Checkpoint at {blk}...'+(' '*80)
    if PRETTYPRINT: printProgressBar(blk, box_count, prefix='Progress:', suffix=suffix, length=50)
    else: logger.info(suffix)

    # addresses
    addrtokens = {'address': [], 'token_id': [], 'amount': [], 'box_id': []}
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

        notes = f'''{len(addr_counter)} addresses, {len(keys_found)} keys'''
        sql = f'''
            insert into audit_log (height, service, notes)
            values ({int(blk)}, 'staking', '{notes}')
        '''
        con.execute(sql)

async def process(last_height, t, use_checkpoint = False, boxes_tablename:str = 'boxes', box_override=''):
    eng = create_engine(DB_DANAIDES)
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
    keys_found = {}
    addresses = {}
    # logger.debug(STAKE_KEYS); exit(1)

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
            
            # find newly unspent boxes
            # sql = f'''
            #     select distinct b.box_id, b.height
            #     from boxes b
            #         left join addresses_staking a on a.box_id = b.box_id
            #         left join keys_staking k on k.box_id = b.box_id
            #     where is_unspent = true
            #         and (a.box_id is null or k.box_id is null)
            # '''
            # msg = ''
            # if last_height > 0:
            #     sql += f'and b.height > {last_height}' # be safe until this is chained behind box imports
            #     msg = f', beginning at height {last_height}' # ?? TODO: how to make height work with only box ids
            sql = f'''
                select box_id, b.height
                from boxes
                where height > {last_height}
            '''

        else:
            # box override
            logger.info(f'Box override {box_override}...')
            sql = f'''
                select box_id, height
                from boxes 
                where box_id in ('{box_override}')
            '''

        # from last_height to new_height; by boxes
        sql = f'''
            select max(height) as height
            from boxes
        '''
        max_height = eng.execute(sql).fetchone()['height']

        # find boxes to search
        boxes = eng.execute(sql).fetchall()
        box_count = len(boxes)

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
                    -- select box_id from keys_staking
                    -- except select box_id from boxes
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
    blip_counter = 0
    stakekey_counter = 0    
    box_count = 0
    for current_height in range(last_height+CHECKPOINT_INTERVAL, max_height+CHECKPOINT_INTERVAL, CHECKPOINT_INTERVAL):
        blk = 0
        # find newly unspent boxes
        next_height = current_height-1
        if next_height > max_height:
            next_height = max_height-1
        logger.debug(f'Find unspent boxes between {last_height} and {next_height}')
        sql = f'''
            select box_id, height
            from boxes
            where height between {last_height} and {next_height}
        '''
        # find boxes to search
        boxes = eng.execute(sql).fetchall()
        box_count = len(boxes)

        for box in boxes:
            box_id = box['box_id']
            # box_id = 'c9c622ddce1e9d3a8ee07c16575a5aeefd33fa5557b93f26ff22cf879eeb7f21'
            suffix = f'''stake keys: {stakekey_counter}/addresses: {len(addresses)}/blips: {blip_counter}/{box_id} {t.split()}'''
            if PRETTYPRINT: printProgressBar(blk, box_count, prefix='Progress:', suffix=suffix, length=50)
            else: logger.debug(suffix)
            blk += 1

            retries = 0
            while retries < 5:
                try:
                    with get(f'''{NODE_URL}/utxo/byId/{box_id}''', headers=headers, timeout=2) as res:
                        # box became unspent during boxes loop (likely new height)
                        if res.status_code == 404:
                            if VERBOSE: logger.warning(f'BLIP: {box_id}')
                            blip_counter += 1
                            pass
                            # logger.error(f'Only unspent boxes allowed on this endpoint; consider updating boxes table: {box}')

                        # logger.warning(res.text); sleep(5)
                        if res.ok:
                            utxo = res.json()
                            address = utxo['ergoTree']
                            assets = utxo['assets']
                            nergs = utxo['value']
                            raw = address[6:]
                            if address in STAKE_KEYS:   
                                stake_token_id = STAKE_KEYS[address]['stake_token_id']
                                # found ergopad staking key
                                if assets[0]['tokenId'] == stake_token_id:
                                    if VERBOSE: logger.debug(f'found ergopad staking token in box: {box}')
                                    stakekey_counter += 1
                                    R4_1 = ErgoValue.fromHex(utxo['additionalRegisters']['R4']).getValue().apply(1)
                                    keys_found[box_id] = {
                                        'stakekey_token_id': utxo['additionalRegisters']['R5'][4:], # TODO: validate that this starts with 0e20 ??
                                        'amount': assets[1]['amount'],
                                        'token_id': stake_token_id,
                                        'penalty': int(R4_1),
                                        'address': raw
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
                                    })

                                if VERBOSE: logger.debug(addresses[raw])

                        else:
                            if VERBOSE: logger.error('bonk')    

                        retries = 5
                
                except Exception as e:
                    logger.error(f'ERR: {e}')
                    retries += 1
                    sleep(1)
                    pass

            # update progress bar on screen
            if blk%UPDATE_INTERVAL == 0:
                suffix = f'''{blk}/addr:{len(addresses.keys())}/keys:{len(keys_found.keys())}/{t.split()} ({len(blips)} blips)'''
                if PRETTYPRINT: printProgressBar(blk, box_count, prefix='Progress:', suffix=suffix, length=50)
                else: logger.info(suffix)                

        # save current unspent to sql
        checkpoint_height = last_height+CHECKPOINT_INTERVAL
        if checkpoint_height > max_height:
            checkpoint_height = max_height-1
        await checkpoint(checkpoint_height, box_count, addresses, keys_found, eng)

        # track staking height here (since looping through boxes)
        notes = f'''{len(addresses)} addresses, {len(keys_found)} keys'''
        sql = f'''
            insert into audit_log (height, service, notes)
            values ({last_height}, 'staking', '{notes}')
        '''
        eng.execute(sql)

        # reset for outer loop: height range
        last_height = current_height
        addresses = {}
        keys_found = {}

    logger.debug('Complete.')

    eng.dispose()
    return {
        'box_count': box_count,
    }

async def hibernate():
    inf = get_node_info()
    last_height = inf['fullHeight']
    current_height = last_height
    t = Timer()
    t.start()

    logger.info('Waiting for next block...')
    infinity_counter = 0
    while last_height == current_height:
        inf = get_node_info()
        current_height = inf['fullHeight']

        if PRETTYPRINT: 
            print(f'''\r({current_height}) {t.split()} Waiting for next block{'.'*(infinity_counter%4)}    ''', end = "\r")
            infinity_counter += 1

        sleep(1)

    sec = t.stop()
    logger.debug(f'Block took {sec:0.4f}s...')     
    sleep(5) # hack, make sure we wait long enough to let unspent boxes to refresh
    return last_height

#endregion FUNCTIONS

async def main(args):
    last_height = args.height
    end_height = args.endat

    while True:
        t = Timer()
        t.start()

        res = await process(last_height, t)
        last_height = await hibernate()

        sec = t.stop()
        logger.debug(f'Block took {sec:0.4f}s...')

if __name__ == '__main__':
    res = asyncio.run(main(args))
