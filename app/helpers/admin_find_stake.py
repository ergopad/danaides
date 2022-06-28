import asyncio
import argparse

from sqlalchemy import create_engine, text
from utils.logger import logger, Timer, printProgressBar
from requests import get
from os import getenv
from base58 import b58encode
parser = argparse.ArgumentParser()
parser.add_argument("-A", "--address", help="Begin at this height", type=str, default='')
parser.add_argument("-P", "--prettyprint", help="Begin at this height", action='store_true')
args = parser.parse_args()

PRETTYPRINT = args.prettyprint
VERBOSE = False

DB_DANAIDES = f"postgresql://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DBNM')}"
NODE_APIKEY = getenv('ERGOPAD_APIKEY')
NODE_URL = f'''http://{getenv('NODE_URL')}:{getenv('NODE_PORT')}'''
NERGS2ERGS = 10**9

headers = {'Content-Type': 'application/json', 'api_key': NODE_APIKEY}
blips = []
eng = create_engine(DB_DANAIDES)

async def get_tokens():
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
    return STAKE_KEYS

async def main(raw):
    STAKE_KEYS = await get_tokens()
    
    # find newly unspent boxes
    sql = f'''
        select b.box_id
        from boxes b
        order by height desc
        -- limit 1000
    '''
    logger.info(f'Fetching all unspent boxes...')
    boxes = eng.execute(sql).fetchall()
    box_count = len(boxes)

    # ...deep breath
    t = Timer()
    t.start()
    STI = [] # stake token ids
    BXS = [] # boxes tied to address
    box_counter = 0
    raw_counter = 0
    stk_counter = 0

    # ready, go
    try:
        for box in boxes:
            box_id = box['box_id']
            suffix = f'{box_id} {t.split()}'+' '*10
            if PRETTYPRINT: printProgressBar(box_counter, box_count, prefix='Progress:', suffix=suffix, length=50)

            with get(f'''{NODE_URL}/utxo/byId/{box_id}''', headers=headers, timeout=2) as res:
                if res.ok:
                    utxo = res.json()
                    address = utxo['ergoTree']
                    
                    # not so useful; staked tokens, ergopad/paideia/egio
                    if False:
                        if address in STAKE_KEYS:   
                            assets = utxo['assets']
                            stake_token_id = STAKE_KEYS[address]['stake_token_id']
                            # box will contain:
                            # 2 assets:
                            # Staked Token Id, qty 1; match this with wallet address
                            # Token Id, i.e. Ergopad/Paideia; qty is amount staked
                            # R4 - penalty info
                            # R5/Stake Key, match with wallet address? 10993e8056067cc26d3c9d8b4647a283a1d10aa5d1d24f84ff43a9f4ede1f629
                            if stake_token_id in [tk['tokenId'] for tk in assets]:
                                logger.debug(f'stake found: {box_id}')
                                STI.append({
                                    'box_id': box_id,
                                    'stake_key': utxo['additionalRegisters']['R5'][4:], # i.e. 'Paideia Stake Key'/https://explorer.ergoplatform.com/en/token/{utxo['additionalRegisters']['R5'][4:]}
                                    'transaction_id': utxo['transactionId'],
                                })
                                stk_counter += 1

                    # boxes for given address
                    if address == f'0008cd{raw}':
                        logger.debug(f'box: [{box_id}]       ')
                        BXS.append(box_id)
                        raw_counter += 1

                box_counter += 1

        if BXS != []:
            logger.debug('boxes for address')
            with open('boxes.tsv', 'w') as f:
                try:
                    print('box_id', file=f)
                    for box in BXS: 
                        print(box, file=f)
                except:
                    print(BXS)
        else:
            print(f'no boxes found for raw address, {raw}')

        if STI != []:
            logger.debug('info for stakes')
            with open('stakes.tsv', 'w') as f:
                print('box_id\tstake_key\ttransaction_id', file=f)
                for box in STI:
                    print(f'''{box['box_id']}\t{box['stake_key']}\t{box['transaction_id']}''', file=f)
        else:
            print(f'no stake tokens found (...yeah, right; go recheck your work)')

        sec = t.stop()
        logger.debug(f'Timer {sec:0.4f}s...')

    except KeyboardInterrupt:
        logger.debug('boxes for address')
        with open('boxes.tsv', 'w') as f:
            try:
                print('box_id', file=f)
                for box in BXS: 
                    print(box, file=f)
            except:
                print(BXS)

if __name__ == '__main__':
    # 9go7mjFPcP4X37CD5tZwLA1fSuXzCoMD69X6ieLejiLdjh2EjGn
    r2a = get(f'''{NODE_URL}/utils/addressToRaw/{args.address}''', headers=headers, timeout=2)
    if r2a.ok:
        logger.debug(f'Searching for unspent boxes related to: {args.address}')
        raw = r2a.json()['raw']
        asyncio.run(main(raw))
