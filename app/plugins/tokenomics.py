import asyncio
import argparse
import pandas as pd
import json

from time import sleep 
# from sqlalchemy import create_engine, text
from utils.logger import logger, Timer, printProgressBar, LEIF
from utils.db import eng, text, engErgopad
from utils.ergo import get_node_info, headers, NODE_APIKEY, NODE_URL, ERGOPAD_API
from utils.aioreq import get_json, get_json_ordered
from ergo_python_appkit.appkit import ErgoValue
from requests import get

# ready, go
ERGUSD_ORACLE_API = 'https://erg-oracle-ergusd.spirepools.com/frontendData'
PRETTYPRINT = False
VERBOSE = False
NERGS2ERGS = 10**9
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 5000 # save progress every X blocks
CLEANUP_NEEDED = False
TOKENS = {}
TOKEN_AGGS = {}

async def checkpoint(found, found_aggs):
    tokens = {'address': [], 'token_id': [], 'amount': [], 'box_id': [], 'height': []}
    for box_id, info in found.items():
        token_id = info['token_id']
        tokens['address'].append(info['address'])
        tokens['token_id'].append(token_id)
        tokens['amount'].append(info['amount'])
        tokens['box_id'].append(box_id)
        tokens['height'].append(info['height'])
    df_addresses = pd.DataFrame().from_dict(tokens)
    df_addresses.to_sql(f'checkpoint_found_tokens', eng, if_exists='replace')

    sql = text(f'''
        insert into tokens_tokenomics (address, token_id, amount, box_id, height)
            select address, token_id, amount, box_id, height
            from checkpoint_found_tokens
    ''')
    with eng.begin() as con:
        con.execute(sql)

    tokens = {'agg_id': [], 'token_id': [], 'amount': [], 'box_id': [], 'height': []}
    for box_id, info in found_aggs.items():
        token_id = info['token_id']
        # tokens['address'].append(info['address'])
        tokens['agg_id'].append(info['agg_id'])
        tokens['token_id'].append(info['token_id'])
        tokens['amount'].append(info['amount'])
        tokens['box_id'].append(box_id)
        tokens['height'].append(info['height'])
    df_addresses = pd.DataFrame().from_dict(tokens)
    df_addresses.to_sql(f'checkpoint_found_aggs', eng, if_exists='replace')

    sql = text(f'''
        insert into tokens_tokenomics_agg (agg_id, token_id, amount, box_id, height)
            select agg_id, token_id, amount, box_id, height
            from checkpoint_found_aggs
    ''')
    with eng.begin() as con:
        con.execute(sql)

async def get_ergo_price():    
    try:
        res = get(ERGUSD_ORACLE_API)
        ergo_price = -1.0
        if res.ok:
            try:
                if VERBOSE: logger.debug(res.text)
                if type(res.json()) == str:
                    ergo_price = json.loads(res.json())['latest_price']
                else:
                    ergo_price = res.json()['latest_price']
            except:
                pass

        if VERBOSE: logger.debug(f'ergo price: {ergo_price}')
        return ergo_price
    
    except Exception as e:
        logger.error(e)
        return 0

async def process(use_checkpoint=False, last_height=-1, juxtapose='boxes', box_override=''):
    try:
        box_id = -1
        # manual boxes tablename
        # box_override = '331a963bbb33542f347aac7be1259980b08284e9a54dcf21e60342104820ba65'
        # box_override = 'ef7365a0d1817873e1f8e537ed0cc4dd32f80beb7f3f71799fb1a7da5f7d1802'
        # last_height = args.height
        # box_override = args.override
        boxes_tablename = ''.join([i for i in juxtapose if i.isalpha()]) # only-alpha tablename

        logger.info(f'Setup...')
        t = Timer()
        t.start()

        # Find proper height
        if last_height == -1 or use_checkpoint:
            sql = text(f'''
                select coalesce(max(height), 0) as height
                from tokens_tokenomics
            ''')
            with eng.begin() as con:
                res = con.execute(sql).fetchone()
            # since height exists, start with height+1
            last_height = res['height'] + 1
            if use_checkpoint: 
                logger.debug(f'Unspent service using last height: {last_height}...')
            else:
                logger.debug(f'No starting height requestsed, using: {last_height}...')

        # BOXES
        sql = f'''
            select box_id, height
                , row_number() over(partition by is_unspent order by height) as r
            from {boxes_tablename}
        '''
        if box_override != '':
            logger.warning('Box override found...')
            sql += f'''where box_id in ('{box_override}')'''
        else:
            sql += f'''where height >= {last_height}'''
        # sql += 'order by height desc'
        if VERBOSE: logger.info(sql)

        # find boxes from checkpoint or standard sql query
        if VERBOSE: logger.debug(sql)
        with eng.begin() as con:
            boxes = con.execute(sql).fetchall()
        box_count = len(boxes)    

        # CLEANUP
        logger.debug('Cleanup tokens ...')
        # remove spent boxes
        logger.debug('  spent tokens_tokenomics')
        sql = text(f'''
            with spent as (
                select t.box_id
                from tokens_tokenomics t
                    left join boxes b on b.box_id = t.box_id
                where b.box_id is null
            )
            delete from tokens_tokenomics t
            using spent s
            where s.box_id = t.box_id
        ''')
        with eng.begin() as con:
            con.execute(sql)

        # remove spent boxes
        logger.debug('  spent tokens_tokenomics_agg')
        sql = text(f'''
            with spent as (
                select t.box_id
                from tokens_tokenomics_agg t
                    left join boxes b on b.box_id = t.box_id
                where b.box_id is null
            )
            delete from tokens_tokenomics_agg t
            using spent s
            where s.box_id = t.box_id
        ''')
        with eng.begin() as con:
            con.execute(sql)

        # if height specified, cleanup
        if last_height > -1:
            # reset to height
            logger.debug('  >= last_height')
            sql = text(f'''
                delete from tokens_tokenomics
                where height >= :last_height
            ''')
            with eng.begin() as con:
                con.execute(sql, {'last_height': last_height})

            # reset to height
            logger.debug('  >= last_height')
            sql = text(f'''
                delete from tokens_tokenomics_agg
                where height >= :last_height
            ''')
            with eng.begin() as con:
                con.execute(sql, {'last_height': last_height})

        # TOKEN Aggregations
        logger.debug('Token aggregations...')
        sql = f'''
            select id, ergo_tree, address, token_id, amount, notes
            from token_agg
        '''
        with eng.begin() as con:
            res = con.execute(sql).fetchall()
        for r in res:
            TOKEN_AGGS[r['ergo_tree']] = {
                'agg_id': r['id'],
                'address': r['address'],
                'token_id': r['token_id'],
                'amount': r['amount'],
                'notes': r['notes'],
            }
        
        # TOKENS
        logger.debug('Gather tokens...')
        sql = f'''
            select token_id, token_name, decimals, token_price
            from tokens
        '''
        with eng.begin() as con:
            res = con.execute(sql).fetchall()
        for r in res:
            token_name = r['token_name']
            TOKENS[r['token_id']] = {
                'token_name': token_name,
                'decimals': r['decimals'],
                'price': r['token_price'],
                'amount': 0
            }            
        if VERBOSE: logger.info(TOKENS)

        max_height = 0
        last_r = 1

        logger.info(f'Begin processing, {box_count} boxes total...')
        token_counter = ', '.join([f'''{t['token_name']}:-''' for token_id, t in TOKENS.items()])
        for r in range(last_r-1, box_count, CHECKPOINT_INTERVAL):
            next_r = r+CHECKPOINT_INTERVAL
            if next_r > box_count:
                next_r = box_count

            suffix = f'{t.split()} :: ErgoPad ({token_counter})...'+(' '*20)
            if PRETTYPRINT: printProgressBar(r, box_count, prefix='Progress:', suffix=suffix, length=50)
            else: logger.debug(suffix)

            try:
                ergopad = {}
                aggs = {}

                urls = [[box['height'], f'''{NODE_URL}/utxo/byId/{box['box_id']}'''] for box in boxes[r:next_r]]
                utxo = await get_json_ordered(urls, headers)   
                for address, box_id, assets, height in [[u[2]['ergoTree'], u[2]['boxId'], u[2]['assets'], u[1]] for u in utxo if u[0] == 200]:
                    # find height for audit (and checkpoint?)
                    if height > max_height:
                        max_height = height
                    
                    # build keys and addresses objext
                    retries = 0
                    while retries < 5:
                        if retries > 0: logger.warning(f'retry: {retries}')
                        
                        for asset in assets:
                            if asset['tokenId'] in TOKENS:
                                TOKENS[asset['tokenId']]['amount'] += asset['amount']
                                # logger.warning(f'''Found [token] in box {box_id}                    ''')
                                ergopad[box_id] = {
                                    'address': address,
                                    'amount': asset['amount'],
                                    'token_id': asset['tokenId'],
                                    'height': height,
                                }

                        # emitted, vested, staked, blah blah...
                        if address in TOKEN_AGGS:
                            # logger.warning(f'''Agg address found {box_id}                    ''')
                            for asset in assets: # TODO: potential to overwrite box values if multiple assets found
                                if TOKEN_AGGS[address]['token_id'] == asset['tokenId']:
                                    logger.warning(f'''Agg token in box {box_id}                    ''')
                                    aggs[box_id] = {
                                        'agg_id': TOKEN_AGGS[address]['agg_id'],
                                        'address': address,
                                        'amount': asset['amount'],
                                        'token_id': asset['tokenId'],
                                        'height': height,
                                    }

                        retries = 5

                # save current unspent to sql
                token_counter = ', '.join([f'''{t['token_name']}:{int(t['amount']/(10**t['decimals']))}''' for token_id, t in TOKENS.items()])
                suffix = f'{t.split()} :: ErgoPad ({token_counter})...'+(' '*20)
                if PRETTYPRINT: printProgressBar(next_r, box_count, prefix='Progress:', suffix=suffix, length=50)
                else: logger.info(suffix)

                if VERBOSE: logger.debug('Saving to sql...')
                await checkpoint(ergopad, aggs)

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

        logger.debug('Final tokenomics step...')
        with eng.begin() as con:
            # update ergopad
            sql = f'''
                update tokens set vested = (
                    select sum(a.amount)/power(10, max(k.decimals))
                    from tokens_tokenomics_agg a
                        join token_agg t on t.id  = a.agg_id
                        join tokens k on k.token_id = a.token_id
                    where t.notes = 'vested'
                        and t.token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
                )    
                where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
            '''
            res = con.execute(sql)

            sql = f'''
                update tokens set emitted = (
                    select sum(a.amount)/power(10, max(k.decimals))
                    from tokens_tokenomics_agg a
                        join token_agg t on t.id  = a.agg_id
                        join tokens k on k.token_id = a.token_id
                    where t.notes = 'emitted'
                        and t.token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
                )    
                where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
            '''
            res = con.execute(sql)

            sql = f'''
                update tokens set staked = (
                    select sum(a.amount)/power(10, max(k.decimals))
                    from tokens_tokenomics_agg a
                        join token_agg t on t.id  = a.agg_id
                        join tokens k on k.token_id = a.token_id
                    where t.notes = 'staked'
                        and t.token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
                )    
                where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
            '''
            res = con.execute(sql)

            sql = f'''
                update tokens set stake_pool = (
                    select sum(a.amount)/power(10, max(k.decimals))
                    from tokens_tokenomics_agg a
                        join token_agg t on t.id  = a.agg_id
                        join tokens k on k.token_id = a.token_id
                    where t.notes = 'stake_pool'
                        and t.token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
                )    
                where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
            '''
            res = con.execute(sql)

            # update ergopad in_circulation
            sql = f'''
                with u as (
                    select (each(assets)).key as token_id
                        , (each(assets)).value::bigint as token_amount
                    from utxos
                )
                , s as (
                    select sum(token_amount)/100 as current_total_supply
                        , token_id
                    from u
                    where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
                    group by token_id
                )
                update tokens 
                    set current_total_supply = s.current_total_supply
                    , in_circulation = s.current_total_supply - tokens.vested - tokens.emitted - tokens.stake_pool
                from s
				where s.token_id = tokens.token_id
            '''
            res = con.execute(sql)

        sec = t.stop()
        logger.debug(f'Tokenomics complete: {sec:0.4f}s...                ')

        return {
            'box_count': box_count,
            'last_height': max_height,
        }

    except Exception as e:
        logger.error(f'ERR: {e}; {box_id}')
        return {
            'box_count': -1,
            'last_height': -1,
            'status': 'error',
            'message': e
        }
        
#endregion FUNCTIONS

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-J", "--juxtapose", help="Alternative table name", default='boxes')
    parser.add_argument("-O", "--override", help="Troubleshoot single box", default='')
    parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
    parser.add_argument("-E", "--endat", help="End at this height", type=int, default=10**10)
    parser.add_argument("-V", "--verbose", help="Be wordy", action='store_true')
    parser.add_argument("-P", "--prettyprint", help="Begin at this height", action='store_true')
    args = parser.parse_args()

    PRETTYPRINT = args.prettyprint

    res = asyncio.run(process(use_checkpoint=False, last_height=args.height, juxtapose=args.juxtapose, box_override=args.override))
    print(res)

