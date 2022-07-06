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
CLEANUP_NEEDED = False

blips = []

async def checkpoint(addresses, keys_found, eng, vesting_tablename='vesting'):
    try:
        # addresses
        addrtokens = {'address': [], 'token_id': [], 'amount': [], 'box_id': [], 'height': []}
        addr_converter = {}
        for raw, tokens in addresses.items():
            if raw not in addr_converter:
                r2a = get(f'''{NODE_URL}/utils/rawToAddress/{raw}''', headers=headers, timeout=2)
                pubkey = ''
                if r2a.ok:
                    pubkey = r2a.json()['address']
                    addr_converter[raw] = pubkey

            for token in tokens:            
                addrtokens['address'].append(addr_converter[raw])
                addrtokens['token_id'].append(token['token_id'])
                addrtokens['amount'].append(token['amount'])
                addrtokens['box_id'].append(token['box_id'])
                addrtokens['height'].append(token['height'])
        df_addresses = pd.DataFrame().from_dict(addrtokens)
        df_addresses.to_sql(f'checkpoint_addresses_{vesting_tablename}', eng, if_exists='replace')

        # stake keys
        df_keys_vesting = pd.DataFrame().from_dict({
            'box_id': list(keys_found.keys()), 
            'token_id': [x['token_id'] for x in keys_found.values()],
            'amount': [x['amount'] for x in keys_found.values()],
            'penalty': [x['penalty'] for x in keys_found.values()],
            'address': [addr_converter[x['address']] for x in keys_found.values()],
            'stakekey_token_id': [x['stakekey_token_id'] for x in keys_found.values()],
            'height': [x['height'] for x in keys_found.values()],
        })
        df_keys_vesting.to_sql(f'checkpoint_keys_{vesting_tablename}', eng, if_exists='replace')

        # addresses
        # TODO: box example shows multiple tokens in same box, however token emission amount is 10 and each amount is 10
        # ... this may not be the proper way to handle this ??
        # box: e9ad4b744b96abc9244287b21c21720622f57b72d8fb2995c1fe4b4afe63f9d2
        # token: a342ae8776207b9a7529b93450187a33538ce86b68d11483758debffea667c25
        with eng.begin() as con:
            sql = f'''
                insert into addresses_vesting (address, token_id, amount, box_id, height)
                    select c.address, c.token_id, sum(c.amount) as amount, c.box_id, c.height
                    from checkpoint_addresses_{vesting_tablename} c
                        left join addresses_vesting a on a.address = c.address::text
                            and a.token_id = c.token_id::varchar(64)
                            and a.box_id = a.box_id::varchar(64)
                            and a.height = c.height
                    where c.address::text != '' -- unusable, don't store
                        and a.box_id::varchar(64) is null -- avoid duplicates
                    group by c.address, c.token_id, c.box_id, c.height
            '''
            con.execute(sql)

            # vesting (keys)
            sql = f'''
                insert into keys_vesting (box_id, token_id, amount, stakekey_token_id, penalty, address, height)
                    select c.box_id, c.token_id, c.amount, c.stakekey_token_id, c.penalty, c.address, c.height
                    from checkpoint_keys_{vesting_tablename} c
                        left join keys_vesting a on a.address = c.address::text
                            and a.token_id = c.token_id::varchar(64)
                            and a.box_id = a.box_id::varchar(64)
                            and a.height = c.height
                    where c.address::text != '' -- unusable, don't store
                        and a.box_id::varchar(64) is null -- avoid duplicates
            '''
            con.execute(sql)

    except Exception as e:
        logger.error(e)
        exit(1)

async def process(last_height, use_checkpoint = False, boxes_tablename:str = 'boxes', box_override=''):
    t = Timer()
    t.start()

    # manual boxes tablename
    # box_override = '331a963bbb33542f347aac7be1259980b08284e9a54dcf21e60342104820ba65'
    # box_override = 'ef7365a0d1817873e1f8e537ed0cc4dd32f80beb7f3f71799fb1a7da5f7d1802'
    boxes_tablename = ''.join([i for i in boxes_tablename if i.isalpha()]) # only-alpha tablename

    # find all stake keys
    sql = '''
        select stake_ergotree, vest_token_id, token_name, token_id, token_type, emission_amount, decimals 
        from tokens
    '''
    VESTING_KEYS = {
        '2k6J5ocjeESe4cuXP6rwwq55t6cUwiyqDzNdEFgnKhwnWhttnSShZb4LaMmqTndrog6MbdT8iJbnnwWEcNoeRfEqXBQW4ohBTgm8rDnu9WBBZSixjJoKPT4DStGSobBkoxS4HZMe4brCgujdnmnMBNf8s4cfGtJsxRqGwtLMvmP6Z6FAXw5pYveHRFDBZkhh6qbqoetEKX7ER2kJormhK266bPDQPmFCcsoYRdRiUJBtLoQ3fq4C6N2Mtb3Jab4yqjvjLB7JRTP82wzsXNNbjUsvgCc4wibpMc8MqJutkh7t6trkLmcaH12mAZBWiVhwHkCYCjPFcZZDbr7xeh29UDcwPQdApxHyrWTWHtNRvm9dpwMRjnG2niddbZU82Rpy33cMcN3cEYZajWgDnDKtrtpExC2MWSMCx5ky3t8C1CRtjQYX2yp3x6ZCRxG7vyV7UmfDHWgh9bvU': [],
        'HNLdwoHRsUSevguzRajzvy1DLAvUJ9YgQezQq6GGZiY4TmU9VDs2ae8mRpQkfEnLmuUKyJibZD2bXR2yoo1p8T5WCRKPn4rJVJ2VR2LvRBk8ViCmhcume5ubWaySXTUqpftEaaURTM6KSFxe4QbRFbToyPzZ3JJmjoDn4WzHh5ioXZMj7AX6xTwJvFmzPuko9BqDk5z1RJtD1wP4kd8sSsLN9P2YNQxmUGDEBYHaDCoAhY7Pg5oKit6ZyqMynoiycWqctfg1EHhMUKCTJsZNnidU961ri98RaYP4CfEwYQ3d9dRVuC6S1n7J1wPPHYqmUBgJCGWbTULayXUowSSmRuZUkQYGo9vvNaEpB7ManiLsX1n8cBYwN4XoVsY24mCfptBP86P4rZ5fgcr9mYtQ9nG934DMDZBbjs81VzCupB6KVrGCe1WtYSr6c1DwkNAinBMwqcqxTznXZUvfBsjDSCtJzCut44xcc7Zsy9mWz2B2pqhdKsX83BVzMDDM5hnjXTShYfauJGs81': [],
    }
    # res = eng.execute(sql).fetchall()
    # for key in res:
        # VESTING_KEYS[key['stake_ergotree']] = {
        #     'vest_token_id': key['vest_token_id'],
        #     'token_name': key['token_name'],
        #     'token_type': key['token_type'],
        #     'emission_amount': key['emission_amount'],
        #     'decimals': key['decimals'],
        # }

    # sql = f'select max(height) as height from {boxes_tablename}'
    # max_height = eng.execute(sql).fetchone()['height']

    ###
    ### 1. Remove spent boxes from vesting, keys
    ### 2. Find the new boxes to process
    ###

    # remove spent boxes from vesting tables
    logger.info('Remove spent boxes from vesting tables...')
    with eng.begin() as con:
        # addresses            
        sql = text(f'''
            with spent as (
                select a.box_id, a.height
                from addresses_vesting a
                    left join {boxes_tablename} b on a.box_id = b.box_id
                where b.box_id is null
            )
            delete from addresses_vesting t
            using spent s
            where s.box_id = t.box_id
                and s.height = t.height
        ''')
        if VERBOSE: logger.debug(sql)
        con.execute(sql)

        sql = text(f'''
            with spent as (
                select a.box_id, a.height
                from keys_vesting a
                    left join {boxes_tablename} b on a.box_id = b.box_id
                where b.box_id is null
            )
            delete from keys_vesting t
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
                where service = 'vesting'
                order by created_at desc 
                limit 1
            '''
            res = eng.execute(sql).fetchone()
            if res is not None:
                last_height = res['height']
            else:
                last_height = 0  

            # above args.height
            with eng.begin() as con:                
                sql = f'''
                    delete 
                    from vesting 
                    where last_height >= {last_height}
                '''
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

    stakekey_counter = 0    
    key_counter = 0
    address_counter = 0
    max_height = 0
    last_r = 1

    logger.info(f'Begin processing, {box_count} boxes total...')

    # process all new, unspent boxes
    vesting_tokens = {}
    for r in range(last_r-1, box_count, CHECKPOINT_INTERVAL):
        next_r = r+CHECKPOINT_INTERVAL
        if next_r > box_count:
            next_r = box_count

        suffix = f'''{t.split()} :: ({key_counter}/{address_counter}) Process ...'''+(' '*20)
        if PRETTYPRINT: printProgressBar(r, box_count, prefix='Progress:', suffix=suffix, length=50)
        else: logger.debug(suffix)

        try:
            keys_found = {}
            addresses = {}

            urls = [[box['height'], f'''{NODE_URL}/utxo/byId/{box['box_id']}'''] for box in boxes[r:next_r]]
            if VERBOSE: logger.debug(f'slice: {r}:{next_r} / up to height: {boxes[next_r-1]["height"]}')
            # ------------
            # primary loop
            # ------------
            # using get_json_ordered so that the height can be tagged to the box_id
            # .. although the height exists in the utxo, the height used is from what is stored in the database
            # .. these 2 heights should be the same, but not spent the time to validate.  May be able to simplify if these are always the same
            # from this loop, look for keys, addresses, assets, etc..
            # sometimes the node gets overwhelmed so using a retry counter (TODO: is there a better way?)
            utxo = await get_json_ordered(urls, headers)
            for address, box_id, assets, registers, height in [[u[2]['ergoTree'], u[2]['boxId'], u[2]['assets'], u[2]['additionalRegisters'], u[1]] for u in utxo if u[0] == 200]:
                # find height for audit (and checkpoint?)
                if height > max_height:
                    max_height = height
                
                # build keys and addresses objext
                retries = 0
                while retries < 5:
                    if retries > 0:
                        logger.warning(f'retry: {retries}')
                    if VERBOSE: logger.debug(address)

                    if address in VESTING_KEYS:
                        try: R5 = ErgoValue.fromHex(registers['R5']).getValue().apply(1)
                        except: logger.warning(f'R5 not found: {box_id}')
                        try: R4 = ErgoValue.fromHex(registers['R4']).getValue().apply(1)
                        except: logger.warning(f'R4 not found: {box_id}')
                        VESTING_KEYS[address].append({
                            'box_id': box_id,
                            'vesting_token_id': R5,
                            'amount': assets[0]['amount'],
                            'token_id': assets[0]['tokenId'],
                            'redeemPeriod': R4[0],
                            'numberOfPeriods': R4[1],
                            'vestingStart': R4[2],
                            'totalVested': R4[3],
                        })

                    # raw = address[6:]
                    # if VERBOSE: logger.warning(address)
                    # if address in VESTING_KEYS:
                    #     vest_token_id = VESTING_KEYS[address]['vest_token_id']
                    #     # found ergopad vesting key
                    #     if VERBOSE: logger.warning(assets[0]['tokenId'])
                    #     if assets[0]['tokenId'] == vest_token_id:
                    #         if VERBOSE: logger.debug(f'found ergopad vesting token in box: {box_id}')
                    #         stakekey_counter += 1
                    #         try: R4_1 = ErgoValue.fromHex(registers['R4']).getValue().apply(1)
                    #         except: logger.warning(f'R4 not found: {box_id}')
                    #         keys_found[box_id] = {
                    #             'stakekey_token_id': registers['R5'][4:], # TODO: validate that this starts with 0e20 ??
                    #             'amount': assets[1]['amount'],
                    #             'token_id': vest_token_id,
                    #             'penalty': int(R4_1),
                    #             'address': raw,
                    #             'height': height,
                    #         }                            

                    # # store assets by address
                    # if len(assets) > 0:
                    #     # init for address
                    #     if raw not in addresses:
                    #         addresses[raw] = []
                        
                    #     # save assets
                    #     for asset in assets:
                    #         addresses[raw].append({
                    #             'token_id': asset['tokenId'], 
                    #             'amount': asset['amount'],
                    #             'box_id': box_id,
                    #             'height': height,
                    #         })
                    
                    retries = 5

            key_counter += len(keys_found)
            address_counter += len(addresses)

            # save current unspent to sql
            suffix = f'{t.split()} :: ({key_counter}/{address_counter}) Checkpoint ({len(keys_found)} new keys, {len(addresses)} new adrs)...'+(' '*20)
            if PRETTYPRINT: printProgressBar(next_r, box_count, prefix='Progress:', suffix=suffix, length=50)
            else: logger.info(suffix)
            await checkpoint(addresses, keys_found, eng)

            # track vesting height here (since looping through boxes)
            notes = f'''{len(addresses)} addresses, {len(keys_found)} keys'''
            with eng.begin() as con:
                sql = f'''
                    insert into audit_log (height, service, notes)
                    values ({max_height}, 'vesting', '{notes}')
                '''
                con.execute(sql)

            # reset for outer loop: height range
            last_r = r
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

    # cleanup
    try:
        if CLEANUP_NEEDED:
            logger.debug('Cleanup vesting tables...')
            sql = f'''
                with dup as (    
                    select id, address, token_id, box_id, amount, height
                        , row_number() over(partition by address, token_id, box_id, amount, height order by id desc) as r
                    from addresses_vesting 
                    -- where address != ''
                )
                delete from addresses_vesting where id in (
                    select id
                    from dup 
                    where r > 1 
                )
            '''
            eng.execute(sql) 

            sql = f'''
                with dup as (    
                    select id, address, token_id, box_id, amount, height
                        , row_number() over(partition by address, token_id, box_id, amount, height order by id desc) as r
                    from keys_vesting 
                    -- where address != ''
                )
                delete from keys_vesting where id in (
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
    res = asyncio.run(main(args))
