import asyncio
import argparse

from time import sleep 
# from sqlalchemy import create_engine, text
from utils.logger import logger, Timer, printProgressBar
from utils.db import eng, text
from utils.ergo import get_node_info, headers, NODE_APIKEY, NODE_URL
from utils.aioreq import get_json, get_json_ordered
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

async def process():
    last_height = args.height
    tabl

    t = Timer()
    t.start()

    # manual boxes tablename
    # box_override = '331a963bbb33542f347aac7be1259980b08284e9a54dcf21e60342104820ba65'
    # box_override = 'ef7365a0d1817873e1f8e537ed0cc4dd32f80beb7f3f71799fb1a7da5f7d1802'
    boxes_tablename = ''.join([i for i in boxes_tablename if i.isalpha()]) # only-alpha tablename

    # find all stake keys
    sql = '''
        select stake_ergotree, stake_token_id, token_name, token_id, token_type, emission_amount, decimals 
        from tokens
    '''
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
    for r in range(last_r-1, box_count, CHECKPOINT_INTERVAL):
        next_r = r+CHECKPOINT_INTERVAL-1
        if next_r > box_count:
            next_r = box_count

        suffix = f'''{t.split()} :: ({key_counter}/{address_counter}) Process ...'''+(' '*20)
        if PRETTYPRINT: printProgressBar(r, box_count, prefix='Progress:', suffix=suffix, length=50)
        else: logger.debug(suffix)

        try:
            keys_found = {}
            addresses = {}

            urls = [[box['height'], f'''{NODE_URL}/utxo/byId/{box['box_id']}'''] for box in boxes[r:next_r]]

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
                    raw = address[6:]
                    if VERBOSE: logger.warning(address)
                    if address in STAKE_KEYS:   
                        stake_token_id = STAKE_KEYS[address]['stake_token_id']
                        # found ergopad staking key
                        if VERBOSE: logger.warning(assets[0]['tokenId'])
                        if assets[0]['tokenId'] == stake_token_id:
                            if VERBOSE: logger.debug(f'found ergopad staking token in box: {box_id}')
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
            suffix = f'{t.split()} :: ({key_counter}/{address_counter}) Checkpoint ({len(keys_found)} new keys, {len(addresses)} new adrs)...'+(' '*20)
            if PRETTYPRINT: printProgressBar(next_r, box_count, prefix='Progress:', suffix=suffix, length=50)
            else: logger.info(suffix)

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

    sec = t.stop()
    logger.debug(f'Processing complete: {sec:0.4f}s...                ')

    eng.dispose()
    return {
        'box_count': box_count,
        'last_height': max_height,
    }
#endregion FUNCTIONS

if __name__ == '__main__':
    res = asyncio.run(main())
    print(res)