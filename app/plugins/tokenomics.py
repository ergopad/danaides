import asyncio
import argparse
import pandas as pd
import json

from time import sleep 
# from sqlalchemy import create_engine, text
from utils.logger import logger, Timer, printProgressBar
from utils.db import eng, text
from utils.ergo import get_node_info, headers, NODE_APIKEY, NODE_URL
from utils.aioreq import get_json, get_json_ordered
from ergo_python_appkit.appkit import ErgoValue
from requests import get

parser = argparse.ArgumentParser()
parser.add_argument("-J", "--juxtapose", help="Alternative table name", default='boxes')
parser.add_argument("-O", "--override", help="Troubleshoot single box", default='')
parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
parser.add_argument("-E", "--endat", help="End at this height", type=int, default=10**10)
parser.add_argument("-V", "--verbose", help="Be wordy", action='store_true')
parser.add_argument("-P", "--prettyprint", help="Begin at this height", action='store_true')
args = parser.parse_args()

# ready, go
ERGUSD_ORACLE_API = 'https://erg-oracle-ergusd.spirepools.com/frontendData'
ERGOPAD_API = 'http://54.214.59.165:8000/api'
PRETTYPRINT = args.prettyprint
VERBOSE = False
NERGS2ERGS = 10**9
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 5000 # save progress every X blocks
CLEANUP_NEEDED = False
TOKENS = {}

async def checkpoint(found):
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

async def get_token_price(token_name):    
    try:
        res = get(f'{ERGOPAD_API}/asset/price/{token_name}', timeout=1)
        if res.ok:
            return res.json()['price']
        else:
            return -1

    except Exception as e:
        logger.error(e)
        return -1

async def get_in_circulation(token_name):    
    try:
        # api call should probably handle this better; just skip for now
        if token_name not in ('ergopad', 'paideia'):
            return -1

        # find in circulation
        if VERBOSE: logger.debug(f'{ERGOPAD_API}/blockchain/{token_name}InCirculation')
        res = get(f'{ERGOPAD_API}/blockchain/{token_name}InCirculation', timeout=1)
        if res.ok:
            if VERBOSE: logger.debug(f'{token_name}: {res.text}')
            return int(float(res.text))
        else:
            return -1

    except Exception as e:
        logger.error(e)
        return -1

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

async def process(use_checkpoint = False):
    try:
        box_id = -1
        # manual boxes tablename
        # box_override = '331a963bbb33542f347aac7be1259980b08284e9a54dcf21e60342104820ba65'
        # box_override = 'ef7365a0d1817873e1f8e537ed0cc4dd32f80beb7f3f71799fb1a7da5f7d1802'
        last_height = args.height
        box_override = args.override
        boxes_tablename = ''.join([i for i in args.juxtapose if i.isalpha()]) # only-alpha tablename

        logger.info(f'Setup...')
        t = Timer()
        t.start()

        # Find proper height
        if last_height == -1 or use_checkpoint:
            sql = text(f'''
                select max(height) as height
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
        # reset to height
        sql = text(f'''
            delete from tokens_tokenomics
            where height >= :last_height
        ''')
        with eng.begin() as con:
            con.execute(sql, {'last_height': last_height})

        # TOKENS
        logger.debug('Gather tokens...')
        sql = f'''
            select token_id, token_name, decimals
            from tokens
        '''
        with eng.begin() as con:
            res = con.execute(sql).fetchall()
        for r in res:
            token_name = r['token_name']
            price = await get_token_price(token_name)
            in_circulation = await get_in_circulation(token_name)
            TOKENS[r['token_id']] = {
                'token_name': token_name,
                'decimals': r['decimals'],
                'price': price,
                'in_circulation': in_circulation,
                'amount': 0
            }
            
            # don't update if API bonked; keep existing value
            if price != -1:
                sql = text(f'''
                    update tokens 
                    set token_price = :token_price
                        , in_circulation = :in_circulation
                    where token_name = :token_name
                ''')
                with eng.begin() as con:
                    con.execute(sql, {'token_price': price, 'token_name': token_name, 'in_circulation': in_circulation})

            # don't update if API bonked; keep existing value
            if in_circulation != -1:
                sql = text(f'''
                    update tokens 
                    set token_price = :token_price
                        , in_circulation = :in_circulation
                    where token_name = :token_name
                ''')
                with eng.begin() as con:
                    con.execute(sql, {'token_price': price, 'token_name': token_name, 'in_circulation': in_circulation})
        if VERBOSE: logger.info(TOKENS)

        max_height = 0
        last_r = 1

        logger.info(f'Begin processing, {box_count} boxes total...')
        token_counter = ', '.join([f'''{t['token_name']}:-''' for token_id, t in TOKENS.items()])
        for r in range(last_r-1, box_count, CHECKPOINT_INTERVAL):
            next_r = r+CHECKPOINT_INTERVAL-1
            if next_r > box_count:
                next_r = box_count

            suffix = f'{t.split()} :: ErgoPad ({token_counter})...'+(' '*20)
            if PRETTYPRINT: printProgressBar(r, box_count, prefix='Progress:', suffix=suffix, length=50)
            else: logger.debug(suffix)

            try:
                ergopad = {}

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
                                if VERBOSE: logger.warning(f'''Found [token] at height {height}; {int(token_counter/100)}/{int(asset['amount']/100)} total...                    ''')
                                ergopad[box_id] = {
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
                await checkpoint(ergopad)

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
        sql = text(f'''
            with s as (
                select
                    sum(k.amount) as current_total_supply
                    , t.token_price
                    , k.token_id::text
                -- select sum(amount)
                from tokens_tokenomics k
                    left join tokens t on t.token_id = k.token_id
                group by k.token_id
                    , t.token_price
            )
            update tokens set current_total_supply = s.current_total_supply
                , token_price = s.token_price
            from s
            where s.token_id = tokens.token_id
        ''')
        with eng.begin() as con:
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
    res = asyncio.run(process())
    print(res)

