import asyncio
import argparse

from utils.db import eng, text
from utils.logger import logger, Timer # , printProgressBar
from requests import get
from os import getenv
from base58 import b58encode
from ergo_python_appkit.appkit import ErgoValue

'''
address token page:
- /blockchain/totalSupply/${TOKEN_ID}
- /blockchain/ergopadInCirculation
- /asset/price/ergopad
'''

parser = argparse.ArgumentParser()
parser.add_argument("-P", "--prettyprint", help="Begin at this height", action='store_true')
parser.add_argument("-V", "--verbose", help="Begin at this height", action='store_true')
args = parser.parse_args()

PRETTYPRINT = args.prettyprint
VERBOSE = args.verbose
ERGUSD_ORACLE_API = 'https://erg-oracle-ergusd.spirepools.com/frontendData'
DB_DANAIDES = f"postgresql://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DBNM')}"
NODE_APIKEY = getenv('ERGOPAD_APIKEY')
NODE_URL = f'''http://{getenv('NODE_URL')}:{getenv('NODE_PORT')}'''
NERGS2ERGS = 10**9

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

async def get_token_price(token_id):    
    try:
        res = get(ERGUSD_ORACLE_API)
        token_price = 0.0
        if res.ok:
            token_price = res.json()['latest_price']
            is_price_stale = False
        else:
            sql = f'''
                select token_price
                from tokens
                where token_id = '{token_id}'
                -- order by created_at desc
                limit 1
            '''
            row = eng.execute(sql).fetchone()
            token_price = row['token_price']
            is_price_stale = True

        logger.debug(f'token price: {token_price}')
        return token_price # , is_price_stale
    
    except Exception as e:
        logger.error(e)
        return 0

async def get_current_total_supply(token_id):
    sql = f'''
        select sum(amount/power(10, decimals)) as amount
        from token_agg a
            join tokens t on t.token_id = a.token_id
        where a.token_id = '{token_id}'
            and ergo_tree is null
    '''
    res = eng.execute(sql).fetchone()    
    total_supply = res['amount']

    logger.debug(f'total supply: {total_supply}')
    return total_supply
    # return 398977967

async def get_in_circulation(token_id):
    total_supply = await get_current_total_supply(token_id)

    sql = f'''
        select sum(amount/power(10, decimals)) as amount
        from token_agg a
            join tokens t on t.token_id = a.token_id
        where a.token_id = '{token_id}'
            and ergo_tree is not null
    '''
    res = eng.execute(sql).fetchone()   
    in_circulation = total_supply - res['amount']
    
    logger.debug(f'in circ: {in_circulation}')
    return in_circulation

### MAIN
async def main(args, token_name = 'ergopad'):
    sql = f'''
        select token_id
            , emission_amount/power(10, decimals) as initial_total_supply
            -- , price_last_seen
        from tokens
        where token_name = '{token_name}'
    '''
    res = eng.execute(sql).fetchone()
    initial_total_supply = res['initial_total_supply']
    token_id = res['token_id']
    # price_last_seen = res['price_last_seen']

    # find
    current_total_supply = await get_current_total_supply(token_id)
    in_circulation = await get_in_circulation(token_id)
    token_price = await get_token_price(token_id)

    # calc
    burned = initial_total_supply - current_total_supply
    market_cap = token_price * in_circulation

    sql = f'''
        update tokens set
            current_total_supply = {current_total_supply}
            , in_circulation = {in_circulation}
            , token_price = {token_price}
        where token_id = '{token_id}'
    '''
    # eng.execute(sql)
    logger.debug(sql)

    if PRETTYPRINT:
        logger.info(sql)

    return {
        'token_name': token_name,
        'token_id': token_id,
        'market_cap': market_cap,
        'initial_total_supply': initial_total_supply,
        'current_total_supply': current_total_supply,
        'burned': burned,
        'in_circulation': in_circulation,
    }

if __name__ == '__main__':
    t = Timer()
    t.start()

    logger.debug(f'Begin...')
    res = asyncio.run(main(args))

    sec = t.stop()
    logger.debug(f'Fin {sec:0.4f}s...')
