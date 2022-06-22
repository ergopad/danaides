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

async def main(args):
    eng = create_engine(DB_DANAIDES)
    sql = '''
        select stake_ergotree, stake_token_id, token_name, token_id, token_type, emission_amount, decimals 
        from tokens
    '''

    # find all stake keys
    STAKE_KEYS = {}
    res = eng.execute(sql).fetchall()
    for key in res:
        STAKE_KEYS['stake_ergotree'] = {
            'stake_token_id': key['stake_token_id'],
            'token_name': key['token_name'],
            'token_type': key['token_type'],
            'emission_amount': key['emission_amount'],
            'decimals': key['decimals'],
        }
    found = {}
    addresses = {}

    # find newly unspent boxes
    sql = f'''
        select b.box_id, b.height
        from boxes b
            left join addresses_staking a on a.box_id = b.box_id
        where is_unspent = true
            and a.box_id is null
    '''
    
    # is this useful?
    if args.height > 0:
        sql = f'''
            select box_id, height
            from boxes
            where is_unspent = true
                and height >= {args.height}
        '''
    
    logger.info('Fetching all unspent boxes...')
    boxes = eng.execute(sql).fetchall()
    box_count = len(boxes)

    '''
    for all unspent boxes
    1. find all legit stake keys (using stake tree/id), and amount
    2. find all addresses with a token
    3. find ergs
    4. last updated
    '''
    logger.info(f'Find all staking keys from {box_count} boxes...')
    prg = 0
    blip_counter = 0
    stakekey_counter = 0
    for box in boxes:
        box_id = box['box_id']
        suffix = f'''stake keys: {stakekey_counter}/addresses: {len(addresses)}/blips: {blip_counter}/{box_id} {t.split()} {100*prg/box_count:0.2f}%'''
        if PRETTYPRINT: printProgressBar(prg, box_count, prefix='Progress:', suffix=suffix, length=50)
        else: logger.debug(suffix)
        prg += 1

        retries = 0
        while retries < 5:
            try:
                with get(f'''{NODE_URL}/utxo/byId/{box_id}''', headers=headers, timeout=2) as res:
                    if res.status_code == 404:
                        if VERBOSE: logger.warning(f'BLIP: {box_id}')
                        blip_counter += 1
                        pass
                        # logger.error(f'Only unspent boxes allowed on this endpoint; consider updating boxes table: {box}')

                    # logger.warning(res.text); sleep(5)
                    if res.ok:
                        utxo = res.json()
                        address = utxo['ergoTree']
                        raw = address[6:]
                        assets = utxo['assets']
                        if address in STAKE_KEYS:
                            decimals = STAKE_KEYS[address]['decimals']
                            stake_token_id = STAKE_KEYS[address]['stake_token_id']
                            # found ergopad staking key
                            if utxo['assets'][0]['tokenId'] == stake_token_id:
                                if VERBOSE: logger.debug(f'found ergopad staking token in box: {box}')
                                stakekey_counter += 1
                                R4_1 = ErgoValue.fromHex(utxo['additionalRegisters']['R4']).getValue().apply(1)
                                found[box_id] = {
                                    'stakekey_token_id': utxo['additionalRegisters']['R5'][4:], # TODO: validate that this starts with 0e20 ??
                                    'amount': utxo['assets'][1]['amount'],
                                    'token_id': stake_token_id,
                                    'penalty': R4_1,
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
    
    if PRETTYPRINT: printProgressBar(box_count, box_count, prefix='Progress:', suffix=f'Complete in {t.split()}'+(' '*100), length=50)
    else: logger.debug(suffix)

    # addresses
    addrtokens = {'address': [], 'token_id': [], 'amount': [], 'box_id': []}
    for raw, tokens in addresses.items():
        r2a = get(f'''{NODE_URL}/utils/rawToAddress/{raw}''', headers=headers, timeout=2)
        pubkey = ''
        if r2a.ok:
            pubkey = r2a.json()['address']
            # if pubkey == '9hix5hs1rCbdbd2tbfEqugrGTeq9oo18XDjHjrZn74WRcNNY6k3': logger.warning(tokens)

        for token in tokens:            
            addrtokens['address'].append(pubkey)
            addrtokens['token_id'].append(token['token_id'])
            addrtokens['amount'].append(token['amount'])
            addrtokens['box_id'].append(token['box_id'])
    df_addresses = pd.DataFrame().from_dict(addrtokens)
    df_addresses.to_sql('checkpoint_addresses_staking', eng, if_exists='replace')

    # stake keys
    df_keys_staking = pd.DataFrame().from_dict({
        'box_id': list(found.keys()), 
        'token_id': [x['token_id'] for x in found.values()],
        'amount': [x['amount'] for x in found.values()],
        'penalty': [x['penalty'] for x in found.values()],
        'stakekey_token_id': [x['stakekey_token_id'] for x in found.values()],
    })
    df_keys_staking.to_sql('checkpoint_keys_staking', eng, if_exists='replace')

    with eng.begin() as con:
        # addresses
        sql = f'''
            -- remove spent boxes from addresses
            delete from addresses_staking
            using addresses_staking a
                left join boxes b on b.box_id = a.box_id
            where b.is_unspent = true
                and b.box_id is null
        '''
        con.execute(sql)

        sql = f'''
            insert into addresses_staking (address, token_id, amount, box_id)
                select address, token_id, amount, box_id
                from checkpoint_addresses_staking
                except
                select address, token_id, amount, box_id
                from addresses_staking
        '''
        con.execute(sql)

        # staking
        sql = f'''
            -- remove spent boxes from keys_staking
            delete from keys_staking
            using keys_staking a
                left join boxes b on b.box_id = a.box_id
            where b.is_unspent = true
                and b.box_id is null
        '''
        con.execute(sql)

        sql = f'''
            insert into keys_staking (box_id, stakekey_id, stake_amount, stake_token_id)
                select box_id, stakekey_id, stake_amount, stake_token_id
                from checkpoint_keys_staking
                except
                select box_id, stakekey_id, stake_amount, stake_token_id
                from keys_staking
        '''
        con.execute(sql)

    eng.dispose()
    return {
        'box_count': box_count,
    }
#endregion FUNCTIONS

if __name__ == '__main__':
    t = Timer()
    t.start()

    res = asyncio.run(main(args))
    args.height = -1
    logger.info(f'''# boxes: {res['box_count']}''')

    t.stop()
