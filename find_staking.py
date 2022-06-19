import asyncio
from cmath import e
import pandas as pd
import argparse

from time import sleep 
from sqlalchemy import create_engine, text
from logger import logger, Timer, printProgressBar
from requests import get
from os import getenv
from base58 import b58encode
from pydantic import BaseModel

from dotenv import load_dotenv
load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument("-T", "--truncate", help="Truncate boxes table", action='store_true')
args = parser.parse_args()

VERBOSE = False

DB_DANAIDES = f"postgresql://{getenv('POSTGRES_USER')}:{getenv('POSTGRES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DBNM')}"
NODE_APIKEY = getenv('ERGOPAD_APIKEY')
NODE_URL = 'http://10.0.0.134:9053' # getenv('ERGONODE_HOST')
NERGS2ERGS = 10**9
# ready, go
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 1000 # save progress every X blocks

headers = {'Content-Type': 'application/json', 'api_key': NODE_APIKEY}
blips = []

DISPLAY = {
    'header': False,
    'transaction': False,
    'transaction_detail': False,
    'box': False,
    'input_detail': False,
    'output_detail': False,
    'register': False,
    'asset': False,
    'count': False,
    'summary': False,
}

def b58(n): 
    return b58encode(bytes.fromhex(n)).decode('utf-8')

async def main(args):
    STAKE_ADDRESS = '3eiC8caSy3jiCxCmdsiFNFJ1Ykppmsmff2TEpSsXY1Ha7xbpB923Uv2midKVVkxL3CzGbSS2QURhbHMzP9b9rQUKapP1wpUQYPpH8UebbqVFHJYrSwM3zaNEkBkM9RjjPxHCeHtTnmoun7wzjajrikVFZiWurGTPqNnd1prXnASYh7fd9E2Limc2Zeux4UxjPsLc1i3F9gSjMeSJGZv3SNxrtV14dgPGB9mY1YdziKaaqDVV2Lgq3BJC9eH8a3kqu7kmDygFomy3DiM2hYkippsoAW6bYXL73JMx1tgr462C4d2PE7t83QmNMPzQrD826NZWM2c1kehWB6Y1twd5F9JzEs4Lmd2qJhjQgGg4yyaEG9irTC79pBeGUj98frZv1Aaj6xDmZvM22RtGX5eDBBu2C8GgJw3pUYr3fQuGZj7HKPXFVuk3pSTQRqkWtJvnpc4rfiPYYNpM5wkx6CPenQ39vsdeEi36mDL8Eww6XvyN4cQxzJFcSymATDbQZ1z8yqYSQeeDKF6qCM7ddPr5g5fUzcApepqFrGNg7MqGAs1euvLGHhRk7UoeEpofFfwp3Km5FABdzAsdFR9'
    STAKE_ADDRESS_TREE = '1017040004000e200549ea3374a36b7a22a803766af732e61798463c3332c5f6d86c8ab9195eed59040204000400040204020400040005020402040204060400040204040e2005cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f312504020402010001010100d802d601b2a4730000d6028cb2db6308720173010001959372027302d80bd603b2a5dc0c1aa402a7730300d604e4c672030411d605e4c6a70411d606db63087203d607b27206730400d608db6308a7d609b27208730500d60ab27206730600d60bb27208730700d60c8c720b02d60de4c672010411d19683090193c17203c1a793c27203c2a793b272047308009ab27205730900730a93e4c67203050ee4c6a7050e93b27204730b00b27205730c00938c7207018c720901938c7207028c720902938c720a018c720b01938c720a029a720c9d9cb2720d730d00720cb2720d730e00d801d603b2a4730f009593c57203c5a7d801d604b2a5731000d1ed93720273119593c27204c2a7d801d605c67204050e95e67205ed93e47205e4c6a7050e938cb2db6308b2a573120073130001e4c67203050e73147315d17316'
    STAKE_KEY_ID = '1028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee'
    ERGOPAD_ID = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'

    eng = create_engine(DB_DANAIDES)
    found = {}
    addresses = {}

    sql = f'''
        select box_id, height
        from boxes
        where is_unspent = true
            and height > 774700
        -- limit 100
    '''
    logger.info('Fetching all unspent boxes...')
    boxes = eng.execute(sql).fetchall()
    box_count = len(boxes)

    '''
    for all unspent boxes
    1. find all legit stake keys (using stake tree/id), and amount
    2. find all addresses with a token
    
    '''
    logger.info(f'Find all staking keys from {box_count} boxes...')
    prg = 0
    blip_counter = 0
    stakekey_counter = 0
    for box in boxes:
        box_id = box['box_id']
        suffix = f'''found: {stakekey_counter}/blips: {blip_counter}/{box_id} {t.split()}'''
        printProgressBar(prg, box_count, prefix='Progress:', suffix=suffix, length=50)
        prg += 1

        res = get(f'''{NODE_URL}/utxo/byId/{box_id}''', headers=headers, timeout=2)
        if res.status_code == 404:
            blip_counter += 1
            pass
            # logger.error(f'Only unspent boxes allowed on this endpoint; consider updating boxes table: {box}')

        # logger.warning(res.text); sleep(5)
        if res.ok:
            utxo = res.json()
            address = utxo['ergoTree']
            assets = utxo['assets']
            if address == STAKE_ADDRESS_TREE:
                # found ergopad staking key
                if utxo['assets'][0]['tokenId'] == STAKE_KEY_ID:
                    if VERBOSE: logger.debug(f'found ergopad staking token in box: {box}')
                    stakekey_counter += 1
                    found[box_id] = {
                        'stakeKeyId': utxo['additionalRegisters']['R5'][4:], # TODO: validate that this starts with 0e20 ??
                        'stakeAmount': utxo['assets'][1]['amount'],
                        # 'penaltyPct': utxo['additionalRegisters']['R4'][1], # TODO: how to render this- box["assets"][1]["amount"]/10**stakedTokenInfo["decimals"],
                        # 'penaltyEndTime': utxo['additionalRegisters']['R4'][1], # TODO: how to render this- validPenalty(stakeBoxR4[1]),
                    }

            if len(assets) > 0:
                if address not in addresses:
                    raw = address[6:]
                    addresses[raw] = []
                
                for asset in assets:
                    addresses[raw].append({
                        'token_id': asset['tokenId'], 
                        'amount': asset['amount'],
                    })

                if VERBOSE: logger.debug(addresses[raw])
        else:
            if VERBOSE: logger.error('bonk')    

    # addresses
    addrtokens = {'address': [], 'token_id': [], 'amount': []}
    for raw, tokens in addresses.items():
        r2a = get(f'''{NODE_URL}/utils/rawToAddress/{raw}''', headers=headers, timeout=2)
        pubkey = ''
        if r2a.ok:
            pubkey = r2a.json()['address']
        for token in tokens:            
            addrtokens['address'].append(pubkey)
            addrtokens['token_id'].append(token['token_id'])
            addrtokens['amount'].append(token['amount'])
    df_addresses = pd.DataFrame().from_dict(addrtokens)
    df_addresses.to_sql('addresses', eng, if_exists='replace')

    # stake keys
    df_stakekeys = pd.DataFrame().from_dict({
        'box_id': list(found.keys()), 
        'stakekey_id': [x['stakeKeyId'] for x in found.values()],
        'stake_amount': [x['stakeAmount'] for x in found.values()],
    })
    df_stakekeys.to_sql('stakekeys', eng, if_exists='replace')

    return {
        'box_count': box_count,
    }

            # if DISPLAY['summary']: print('='*80+f'block {i}')
            # if DISPLAY['count']: print(f'''headers: {len(res.json())}''')
            # iHeaders += len(res.json())
            # for hdr in res.json():
            #     if DISPLAY['header']: print(f'''header: {hdr}''')
            #     txs = get(f'{NODE_URL}/blocks/{hdr}/transactions', headers=headers, timeout=2)
            #     if DISPLAY['count']: print(f'''{len(txs.json()['transactions'])} transactions''')
            #     iTransactions += len(txs.json()['transactions'])
            #     for tx in txs.json()['transactions']:
            #         if DISPLAY['transaction']: print(f'''  txid: {tx['id']}''')
            #         if DISPLAY['transaction_detail']: print(tx)
            # 
            #         if DISPLAY['count']: print(f'''  outputs: {len(tx['outputs'])}''')
            #         iOutputs += len(tx['outputs'])
            #         for io in tx['outputs']:
            #             if DISPLAY['box']: print(f'''    boxid: {io['boxId']}/{io['value']}''')
            #             if DISPLAY['output_detail']: print(io)
            #             if io['ergoTree'] == STAKE_ADDRESS_TREE:
            #                 print(':: FOUND ADDRESS ::')
            #                 if io['assets'][0]['tokenId'] == STAKE_KEY_ID:
            #                     if io['assets'][1]['tokenId'] == ERGOPAD_ID:
            #                         found[io['boxId']] = {
            #                             'stakeKeyId': io['additionalRegisters']['R5'][4:], # TODO: validate that this starts with 0e20 ??
            #                             'stakeAmount': io['assets'][1]['amount'],
            #                             # 'penaltyPct': io['additionalRegisters']['R4'][1], # TODO: how to render this- box["assets"][1]["amount"]/10**stakedTokenInfo["decimals"],
            #                             # 'penaltyEndTime': io['additionalRegisters']['R4'][1], # TODO: how to render this- validPenalty(stakeBoxR4[1]),
            #                             # 'additionalRegisters': io['additionalRegisters'], # don't need
            #                         }
            #                         print(f''':: FOUND Address/TOKEN in transaction: {tx['id']} ::''')
            #                 # for a in io['assets']:
            #                 #     if DISPLAY['asset']: print(f'''    asset: {a}''')
            #                 #     if a['tokenId'] == STAKE_KEY_ID: 
            #                 #         found[io['boxId']] = a['amount']
            #                 #         print(f''':: FOUND Address/TOKEN in transaction: {tx['id']} ::''')
            #             for r in io['additionalRegisters']:
            #                 if DISPLAY['register']: print(f'''    registers: {r}''')
            #             if io['boxId'] not in spent:
            #                 pass
            #                 # spent[io['boxId']] = False
            #             else:
            #                 if False: print('?? multiple outs')
            # 
            #         if DISPLAY['count']: print(f'''  inputs: {len(tx['inputs'])}''')
            #         iInputs += len(tx['inputs'])
            #         for io in tx['inputs']:
            #             if DISPLAY['box']: print(f'''    boxid: {io['boxId']}''')
            #             if DISPLAY['input_detail']: print(io)
            #             if io['boxId'] in spent:
            #                 spent[io['boxId']] = True
            #             else:
            #                 if False: print('?? input to untracked box')
            # 
            # if DISPLAY['summary']: print(f'''{iHeaders} headers, {iTransactions} transactions, {iInputs} inputs, {iOutputs} outputs''')

#endregion FUNCTIONS

if __name__ == '__main__':
    t = Timer()
    t.start()

    res = asyncio.run(main(args))
    logger.info(f'''
        # boxes: {res['box_count']}
    ''')

    t.stop()
