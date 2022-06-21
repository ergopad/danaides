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

from dotenv import load_dotenv
load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument("-T", "--truncate", help="Truncate boxes table", action='store_true')
parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
args = parser.parse_args()

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
    # ergopad 
    ERGOPAD_STAKE_ADDRESS_TREE = '1017040004000e200549ea3374a36b7a22a803766af732e61798463c3332c5f6d86c8ab9195eed59040204000400040204020400040005020402040204060400040204040e2005cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f312504020402010001010100d802d601b2a4730000d6028cb2db6308720173010001959372027302d80bd603b2a5dc0c1aa402a7730300d604e4c672030411d605e4c6a70411d606db63087203d607b27206730400d608db6308a7d609b27208730500d60ab27206730600d60bb27208730700d60c8c720b02d60de4c672010411d19683090193c17203c1a793c27203c2a793b272047308009ab27205730900730a93e4c67203050ee4c6a7050e93b27204730b00b27205730c00938c7207018c720901938c7207028c720902938c720a018c720b01938c720a029a720c9d9cb2720d730d00720cb2720d730e00d801d603b2a4730f009593c57203c5a7d801d604b2a5731000d1ed93720273119593c27204c2a7d801d605c67204050e95e67205ed93e47205e4c6a7050e938cb2db6308b2a573120073130001e4c67203050e73147315d17316'
    ERGOPAD_STAKE_KEY_ID = '1028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee'
    # STAKE_ADDRESS = '3eiC8caSy3jiCxCmdsiFNFJ1Ykppmsmff2TEpSsXY1Ha7xbpB923Uv2midKVVkxL3CzGbSS2QURhbHMzP9b9rQUKapP1wpUQYPpH8UebbqVFHJYrSwM3zaNEkBkM9RjjPxHCeHtTnmoun7wzjajrikVFZiWurGTPqNnd1prXnASYh7fd9E2Limc2Zeux4UxjPsLc1i3F9gSjMeSJGZv3SNxrtV14dgPGB9mY1YdziKaaqDVV2Lgq3BJC9eH8a3kqu7kmDygFomy3DiM2hYkippsoAW6bYXL73JMx1tgr462C4d2PE7t83QmNMPzQrD826NZWM2c1kehWB6Y1twd5F9JzEs4Lmd2qJhjQgGg4yyaEG9irTC79pBeGUj98frZv1Aaj6xDmZvM22RtGX5eDBBu2C8GgJw3pUYr3fQuGZj7HKPXFVuk3pSTQRqkWtJvnpc4rfiPYYNpM5wkx6CPenQ39vsdeEi36mDL8Eww6XvyN4cQxzJFcSymATDbQZ1z8yqYSQeeDKF6qCM7ddPr5g5fUzcApepqFrGNg7MqGAs1euvLGHhRk7UoeEpofFfwp3Km5FABdzAsdFR9'
    # ERGOPAD_TOKEN_ID = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'

    # ergopad 
    PAIDEIA_STAKE_ADDRESS_TREE = '101f040004000e2012bbef36eaa5e61b64d519196a1e8ebea360f18aba9b02d2a21b16f26208960f040204000400040001000e20b682ad9e8c56c5a0ba7fe2d3d9b2fbd40af989e8870628f4a03ae1022d36f0910402040004000402040204000400050204020402040604000100040404020402010001010100040201000100d807d601b2a4730000d6028cb2db6308720173010001d6039372027302d604e4c6a70411d605e4c6a7050ed60695ef7203ed93c5b2a4730300c5a78fb2e4c6b2a57304000411730500b2e4c6720104117306007307d6079372027308d1ecec957203d80ad608b2a5dc0c1aa402a7730900d609e4c672080411d60adb63087208d60bb2720a730a00d60cdb6308a7d60db2720c730b00d60eb2720a730c00d60fb2720c730d00d6107e8c720f0206d611e4c6720104119683090193c17208c1a793c27208c2a793b27209730e009ab27204730f00731093e4c67208050e720593b27209731100b27204731200938c720b018c720d01938c720b028c720d02938c720e018c720f01937e8c720e02069a72109d9c7eb272117313000672107eb27211731400067315957206d801d608b2a5731600ed72079593c27208c2a7d801d609c67208050e95e67209ed93e472097205938cb2db6308b2a57317007318000172057319731a731b9595efec7206720393c5b2a4731c00c5a7731d7207731e'
    PAIDEIA_STAKE_KEY_ID = '245957934c20285ada547aa8f2c8e6f7637be86a1985b3e4c36e4e1ad8ce97ab'
    # PAIDEIA_STAKE_ADDRESS = ''
    # PAIDEIA_TOKEN_ID = '1fd6e032e8476c4aa54c18c1a308dce83940e8f4a28f576440513ed7326ad489'

    STAKE_KEYS = {
        ERGOPAD_STAKE_ADDRESS_TREE: ERGOPAD_STAKE_KEY_ID,
        PAIDEIA_STAKE_ADDRESS_TREE: PAIDEIA_STAKE_KEY_ID,
    }

    eng = create_engine(DB_DANAIDES)
    found = {}
    addresses = {}

    # find newly unspent boxes
    sql = f'''
        select box_id, height
        from boxes
        where is_unspent = true
            and box_id not in (
                select box_id
                from addresses_staking
            )
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
    
    '''
    logger.info(f'Find all staking keys from {box_count} boxes...')
    prg = 0
    blip_counter = 0
    stakekey_counter = 0
    for box in boxes:
        box_id = box['box_id']
        suffix = f'''stake keys: {stakekey_counter}/addresses: {len(addresses)}/blips: {blip_counter}/{box_id} {t.split()} {prg/box_count:0.2f}%'''
        # printProgressBar(prg, box_count, prefix='Progress:', suffix=suffix, length=50)
        logger.debug(suffix)
        prg += 1

        res = get(f'''{NODE_URL}/utxo/byId/{box_id}''', headers=headers, timeout=2)
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
                stake_token_id = STAKE_KEYS[address]
                # found ergopad staking key
                if utxo['assets'][0]['tokenId'] == stake_token_id:
                    if VERBOSE: logger.debug(f'found ergopad staking token in box: {box}')
                    stakekey_counter += 1
                    found[box_id] = {
                        'stakeKeyId': utxo['additionalRegisters']['R5'][4:], # TODO: validate that this starts with 0e20 ??
                        'stakeAmount': utxo['assets'][1]['amount'],
                        'stake_token_id': stake_token_id,
                        # 'penaltyPct': utxo['additionalRegisters']['R4'][1], # TODO: how to render this- box["assets"][1]["amount"]/10**stakedTokenInfo["decimals"],
                        # 'penaltyEndTime': utxo['additionalRegisters']['R4'][1], # TODO: how to render this- validPenalty(stakeBoxR4[1]),
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
    
    # printProgressBar(box_count, box_count, prefix='Progress:', suffix=f'Complete in {t.split()}'+(' '*100), length=50)
    logger.debug(suffix)

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
    df_addresses.to_sql('checkpoint_addresses', eng, if_exists='replace')

    # stake keys
    df_keys_staking = pd.DataFrame().from_dict({
        'box_id': list(found.keys()), 
        'stakekey_id': [x['stakeKeyId'] for x in found.values()],
        'stake_amount': [x['stakeAmount'] for x in found.values()],
        'stake_token_id': [x['stake_token_id'] for x in found.values()],
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
                from checkpoint_addresses
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
                from keys_staking
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
