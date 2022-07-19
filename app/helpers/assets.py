import asyncio
import pandas as pd
import argparse
import json

from time import sleep 
# from sqlalchemy import create_engine, text
from utils.logger import logger, Timer, printProgressBar
from utils.db import eng, text
from utils.ergo import get_node_info, headers, NODE_APIKEY, NODE_URL
from utils.aioreq import get_json, get_json_ordered
from requests import get
from ergo_python_appkit.appkit import ErgoValue

# ready, go
PRETTYPRINT = False
VERBOSE = False
NERGS2ERGS = 10**9
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 5000 # save progress every X blocks
CLEANUP_NEEDED = False

blips = []

async def checkpoint(assets):
    try:
        addr_converter = {}

        # assets
        contents = {'address': [], 'value': [], 'assets': [], 'registers': [], 'box_id': [], 'height': []}
        for box_id, content in assets.items():
            if content['address'] not in addr_converter:
                r2a = get(f'''{NODE_URL}/utils/rawToAddress/{content['address']}''', headers=headers, timeout=2)
                if r2a.ok:
                    pubkey = r2a.json()['address']
                    addr_converter[content['address']] = pubkey
                else: 
                    addr_converter[content['address']] = ''
            contents['address'].append(addr_converter[content['address']])
            contents['value'].append(content['value'])
            contents['assets'].append(json.dumps(content['assets']))
            contents['registers'].append(json.dumps(content['assets']))
            contents['box_id'].append(box_id)
            contents['height'].append(content['height'])
        df_box_contents = pd.DataFrame().from_dict(contents)
        df_box_contents.to_sql(f'checkpoint_box_contents', eng, if_exists='replace') # , dtype={"new_json_col": sqlalchemy.types.JSON},

        with eng.begin() as con:
            # assets
            sql = f'''
                insert into box_contents (address, value, assets, registers, box_id, height)
                    select c.address, c.value, c.assets::json, c.registers::json, c.box_id, c.height
                    from checkpoint_box_contents c
                        left join box_contents a on a.box_id = c.box_id::varchar(64)
                            -- and a.height = c.height
                    where c.address::text != '' -- unusable, don't store
                        and a.box_id::varchar(64) is null -- avoid duplicates
            '''
            con.execute(sql)

    except Exception as e:
        logger.debug(f'ERR: checkpoint {e}')
        pass

async def x_checkpoint(assets, balances, eng):
    try:
        addr_converter = {}

        # balances
        addrbalances = {'address': [], 'value': [], 'box_id': [], 'height': []}
        for raw, balance in balances.items():
            if raw not in addr_converter:
                r2a = get(f'''{NODE_URL}/utils/rawToAddress/{raw}''', headers=headers, timeout=2)
                if r2a.ok:
                    pubkey = r2a.json()['address']
                    addr_converter[raw] = pubkey
                else: 
                    addr_converter[raw] = ''
            for bal in balance:
                addrbalances['address'].append(addr_converter[raw])
                addrbalances['value'].append(bal['value'])
                addrbalances['box_id'].append(bal['box_id'])
                addrbalances['height'].append(bal['height'])
        df_balances = pd.DataFrame().from_dict(addrbalances)
        df_balances.to_sql(f'checkpoint_balances', eng, if_exists='replace')

        # assets
        addrtokens = {'address': [], 'amount': [], 'token_id': [], 'box_id': [], 'height': []}
        for raw, asset in assets.items():
            if raw not in addr_converter:
                r2a = get(f'''{NODE_URL}/utils/rawToAddress/{raw}''', headers=headers, timeout=2)
                if r2a.ok:
                    pubkey = r2a.json()['address']
                    addr_converter[raw] = pubkey
                else: 
                    addr_converter[raw] = ''
            for ast in asset:
                addrtokens['address'].append(addr_converter[raw])
                addrtokens['amount'].append(ast['amount'])
                addrtokens['token_id'].append(ast['token_id'])
                addrtokens['box_id'].append(ast['box_id'])
                addrtokens['height'].append(ast['height'])
        df_assets = pd.DataFrame().from_dict(addrtokens)
        df_assets.to_sql(f'checkpoint_assets', eng, if_exists='replace')

        with eng.begin() as con:
            # assets
            sql = f'''
                insert into assets (address, token_id, amount, box_id, height)
                    select c.address, c.token_id, c.amount, c.box_id, c.height
                    from checkpoint_assets c
                        left join assets a on a.address = c.address::text
                            and a.token_id = c.token_id::varchar(64)
                            and a.box_id = a.box_id::varchar(64)
                            and a.height = c.height
                    where c.address::text != '' -- unusable, don't store
                        and a.box_id::varchar(64) is null -- avoid duplicates
            '''
            con.execute(sql)

            # balances
            sql = f'''
                insert into balances (address, value, box_id, height)
                    select c.address, c.value, c.box_id, c.height
                    from checkpoint_balances c
                        left join balances a on a.address = c.address::text
                            and a.box_id = a.box_id::varchar(64)
                            and a.height = c.height
                    where c.address::text != '' -- unusable, don't store
                        and a.box_id::varchar(64) is null -- avoid duplicates
            '''
            con.execute(sql)

    except Exception as e:
        logger.debug(f'ERR: checkpoint {e}')
        pass

async def process(last_height, use_checkpoint = False, boxes_tablename:str = 'boxes', box_override=''):
    t = Timer()
    t.start()

    # manual boxes tablename
    # box_override = '331a963bbb33542f347aac7be1259980b08284e9a54dcf21e60342104820ba65'
    # box_override = 'ef7365a0d1817873e1f8e537ed0cc4dd32f80beb7f3f71799fb1a7da5f7d1802'
    boxes_tablename = ''.join([i for i in boxes_tablename if i.isalpha()]) # only-alpha tablename

    # remove spent boxes from assets tables
    logger.info('Remove spent boxes from assets tables...')
    with eng.begin() as con:
        # assets            
        sql = text(f'''
            with spent as (
                select a.box_id, a.height
                from assets a
                    left join {boxes_tablename} b on a.box_id = b.box_id
                where b.box_id is null
            )
            delete from assets t
            using spent s
            where s.box_id = t.box_id
                and s.height = t.height
        ''')
        if VERBOSE: logger.debug(sql)
        con.execute(sql)

        # balances            
        sql = text(f'''
            with spent as (
                select a.box_id, a.height
                from balances a
                    left join {boxes_tablename} b on a.box_id = b.box_id
                where b.box_id is null
            )
            delete from balances t
            using spent s
            where s.box_id = t.box_id
                and s.height = t.height
        ''')
        if VERBOSE: logger.debug(sql)
        con.execute(sql)

    if use_checkpoint:
        logger.debug('Using checkpoint')

        # find newly unspent boxes
        sql = f'''
            select box_id, height, row_number() over(partition by is_unspent order by height) as r 
            from checkpoint_{boxes_tablename}
                where is_unspent::boolean = true
        '''

    # process as standalone call
    else:
        logger.info('Sleeping to make sure boxes are processed...')
        sleep(2)
        logger.info('Finding boxes...')
        if last_height >= 0:
            logger.info(f'Above block height: {last_height}...')

        else:
            sql = f'''
                select height 
                from audit_log 
                where service = 'assets'
                order by created_at desc 
                limit 1
            '''
            res = eng.execute(sql).fetchone()
            if res is not None:
                last_height = res['height']
            else:
                last_height = 0  

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

    asset_counter = 0
    max_height = 0
    last_r = 1

    logger.info(f'Begin processing, {box_count} boxes total...')

    # process all new, unspent boxes
    for r in range(last_r-1, box_count, CHECKPOINT_INTERVAL):
        next_r = r+CHECKPOINT_INTERVAL
        if next_r > box_count:
            next_r = box_count

        suffix = f'''{t.split()} :: ({asset_counter}) Process ...'''+(' '*20)
        if PRETTYPRINT: printProgressBar(r, box_count, prefix='Progress:', suffix=suffix, length=50)
        else: logger.debug(suffix)

        try:
            assets = {}
            # balances = {}

            urls = [[box['height'], f'''{NODE_URL}/utxo/byId/{box['box_id']}'''] for box in boxes[r:next_r]]
            if VERBOSE: logger.debug(f'slice: {r}:{next_r} / up to height: {boxes[next_r-1]["height"]}')
            # ------------
            # primary loop
            # ------------
            # using get_json_ordered so that the height can be tagged to the box_id
            # .. although the height exists in the utxo, the height used is from what is stored in the database
            # .. these 2 heights should be the same, but not spent the time to validate.  May be able to simplify if these are always the same
            # from this loop, look for keys, assets, etc..
            # sometimes the node gets overwhelmed so using a retry counter (TODO: is there a better way?)
            utxo = await get_json_ordered(urls, headers)
            for ergo_tree, box_id, box_assets, registers, nergs, createion_height, transaction_id, height in [[u[2]['ergoTree'], u[2]['boxId'], u[2]['assets'], u[2]['additionalRegisters'], u[2]['value'], u[2]['creationHeight'], u[2]['transactionId'], u[1]] for u in utxo if u[0] == 200]:
                # find height for audit (and checkpoint?)
                address = ergo_tree
                if height > max_height:
                    max_height = height

                sql = text(f'''
                    insert into utxo (box_id, ergo_tree, address, nergs, registers, assets, transaction_id, creation_height, height)
                    values (:box_id, :ergo_tree, :address, :nergs, :registers, :assets, :transaction_id, :creation_height, :height)
                ''')

                val = {
                    'box_id': box_id,
                    'ergo_tree': ergo_tree,
                    'address': ergo_tree,
                    'nergs': nergs,
                    'registers': ','.join([f'''{i}=>{j}''' for i, j in registers.items()]),
                    'assets': ','.join([f'''{a['tokenId']}=>{a['amount']}''' for a in box_assets]),
                    'transaction_id': transaction_id,
                    'creation_height': createion_height,
                    'height': height,
                }          
                
                eng.execute(sql, val)
                
                # build keys and assets objext
                retries = 0
                while retries < 5:
                    if retries > 0:
                        logger.warning(f'retry: {retries}')
                    if VERBOSE: logger.debug(address)

                    # raw = str(address[6:])
                    # if raw not in assets:
                    #     assets[raw] = []
                    # if raw not in balances:
                    #     balances[raw] = []

                    assets[box_id] = {
                        'address': address[6:],
                        'value': value,
                        'assets': box_assets,
                        'registers': registers,
                        'box_id': box_id,
                        'height': height,
                    }

                    # if VERBOSE: logger.warning(f'balance: {raw}')
                    # balances[raw].append({                                
                    #     'value': value or 0,
                    #     'box_id': box_id,
                    #     'height': height,
                    # })

                    # store assets by address
                    # if VERBOSE: logger.warning(f'asset: {raw}')
                    # if len(box_assets) > 0:
                    #     # save assets
                    #     for asset in box_assets:
                    #         if VERBOSE: logger.debug(f'{asset}')
                    #         assets[raw].append({
                    #             'token_id': asset['tokenId'], 
                    #             'amount': asset['amount'],
                    #             'box_id': box_id,
                    #             'height': height,
                    #         })
                    
                    retries = 5

                asset_counter += len(box_assets)

            # save current unspent to sql
            suffix = f'{t.split()} :: ({asset_counter}) Checkpoint...'+(' '*20)
            if PRETTYPRINT: printProgressBar(next_r, box_count, prefix='Progress:', suffix=suffix, length=50)
            else: logger.info(suffix)
            await checkpoint(assets)

            # track assets height here (since looping through boxes)
            notes = f'''{len(assets)} assets'''
            sql = f'''
                insert into audit_log (height, service, notes)
                values ({max_height}, 'assets', '{notes}')
            '''
            eng.execute(sql)

            # reset for outer loop: height range
            last_r = r
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
        if CLEANUP_NEEDED:
            logger.debug('Cleanup assets tables...')
            sql = f'''
                with dup as (    
                    select id, address, token_id, box_id, amount, height
                        , row_number() over(partition by address, token_id, box_id, amount, height order by id desc) as r
                    from assets 
                    -- where address != ''
                )
                delete from assets where id in (
                    select id
                    from dup 
                    where r > 1 
                )
            '''
            with eng.begin() as con:
                con.execute(sql) 

            sql = f'''
                with dup as (    
                    select id, address, box_id, amount, height
                        , row_number() over(partition by address, box_id, amount, height order by id desc) as r
                    from balances
                    -- where address != ''
                )
                delete from balances where id in (
                    select id
                    from dup 
                    where r > 1 
                )
            '''
            with eng.begin() as con:
                con.execute(sql) 

    except Exception as e:
        logger.debug(f'ERR: cleaning dups {e}')
        pass

    sec = t.stop()
    logger.debug(f'Processing complete: {sec:0.4f}s...                ')

    return {
        'box_count': box_count,
        'last_height': max_height,
    }

async def hibernate(new_height):
    current_height = new_height
    hibernate_timer = Timer()
    hibernate_timer.start()

    logger.info('Waiting for next block...')
    infinity_counter = 0
    while new_height == current_height:
        inf = get_node_info()
        current_height = inf['fullHeight']

        if PRETTYPRINT: 
            print(f'''\r{current_height} :: {hibernate_timer.split()} Waiting for next block{'.'*(infinity_counter%4)}    ''', end = "\r")
            infinity_counter += 1

        sleep(1)

    sec = hibernate_timer.stop()
    logger.debug(f'Next block {sec:0.4f}s...')     

    return current_height

#endregion FUNCTIONS

async def main(args):
    # args.juxtapose = 'jux'
    new_height = args.height

    while True:
        res = await process(new_height, boxes_tablename=args.juxtapose)
        new_height = res['last_height']+1
        await hibernate(new_height)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-J", "--juxtapose", help="Alternative table name", default='boxes')
    parser.add_argument("-T", "--truncate", help="Truncate boxes table", action='store_true')
    parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
    parser.add_argument("-E", "--endat", help="End at this height", type=int, default=10**10)
    parser.add_argument("-P", "--prettyprint", help="Begin at this height", action='store_true')
    args = parser.parse_args()

    PRETTYPRINT = args.prettyprint

    res = asyncio.run(main(args))
