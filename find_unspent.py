import asyncio
from cmath import e
import pandas as pd
import argparse
 
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

''' 
- Find all current unspent boxes

- From current unspent boxes, find stake keys (R5.renderedValue where address=STAKE_ADDRESS and asset[0]=STAKE_KEY_ID)
.. this also provides the amount from assets[1], which is the tokenId (i.e. ERGOPAD_ID) and value
  > assets:
  >   1028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee: 1
  >   d71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413: 232858

- From found stake keys find where asset[0] is R5.renderedValue (above), this is the address
  > ergoTree: 0008cd03fb431c71705e2e3b19d9f4b6ae405fe57bfaf09888f1cc45d40d67423fb88f07
  > address: 9iNTr1roReH8M38wVsazuCkWbpi5Y3UN9F7iScx1BhzS2TgYpCy
  > assets:
  >   6fa205cd20a20a34ee888cd283e67c55296ddd2666ef67452d2b12da5e09ee62: 1

# begin update with boxes to update is_spent
# partition table (list) on is_spent
# update starting from max(height)+1
# if unspent and last_seen height < current_height, invalid?
boxes
. id
. box_id
. height
. is_spent
. last_seen (height?)

# using f'{NODE}/blocks/at/{i}'
# .. only need to check heights from unspent (boxes table)
stake_keys
. id
. name (i.e. Ergopad/Paideia)
. token_id (ERGOPAD_ID)
. address (STAKE_ADDRESS)
. tree (STAKE_ADDRESS_TREE)
. key_id (STAKE_KEY_ID)

# only need to check heights from unspent (boxes table)
stake_holders
. id
. address
. config_id (-> stake_config)
. box_id
. amount
. penalty
. last_seen (height?)


-- drop table audit_log
create table audit_log (
    id serial not null primary key,
    height int not null,
    created_at timestamp default now()
)

-- drop table boxes
create table boxes (
    id serial not null,
    box_id varchar(64) not null,
    height int not null,
    is_unspent boolean default true,
    primary key (id, is_unspent),
    unique (box_id, is_unspent)
)
partition by list(is_unspent);

create table boxes_spent partition of boxes for values in (false);
create table boxes_unspent partition of boxes for values in (true);
'''

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

#region CLASSES
class UTXO():
    
    height = -1

    def __init__(self) -> None:
        res = get(f'{NODE_URL}/utxo/genesis', headers=headers, timeout=2)
        genesis_blocks = res.json()
        for gen in genesis_blocks:
            self.unspent[gen['boxId']] = gen['value']

    def restore(self) -> None:
        # restore from previous state (sql)
        # set self.height
        pass

    def process(self) -> None:
        # update unspent with next block
        # check if current state is max (current height of node)
        pass

#region FUNCTIONS
def b58(n): 
    return b58encode(bytes.fromhex(n)).decode('utf-8')

def get_node_info():
    res = get(f'{NODE_URL}/info', headers=headers, timeout=2)
    node_info = None
    current_height = 0
    if not res.ok:
        logger.error(f'unable to retrieve node info: {res.text}')
        exit()
    else:
        node_info = res.json()
        if VERBOSE: logger.debug(node_info)
    
    # return 10000 # testing
    return node_info

# remove all inputs from current block
async def del_inputs(inputs: dict, unspent: dict, height: int = -1) -> dict:
    new = unspent
    for i in inputs:
        box_id = i['boxId']
        try:
            new[box_id] = height
        except Exception as e:
            blips.append({'box_id': box_id, 'height': height, 'msg': f'cant remove'})
            if VERBOSE: logger.warning(f'cant find {box_id} at height {height} while removing from unspent {e}')
    return new

# add all outputs from current block
async def add_outputs(outputs: dict, unspent: dict, height: int = -1) -> dict:
    new = unspent
    for o in outputs:
        box_id = o['boxId']
        # amount = o['value']
        try:
            new[box_id] = height
        except Exception as e:
            blips.append({'box_id': box_id, 'height': height, 'msg': f'cant add'})
            if VERBOSE: logger.warning(f'{box_id} exists at height {height} while adding to unspent {e}')
    return new

async def checkpoint(blk, current_height, unspent, eng):
    suffix = f'Checkpoint at {blk}...'
    printProgressBar(blk, current_height, prefix='Progress:', suffix=suffix, length=50)
    # logger.info('checkpoint')
    df = pd.DataFrame.from_dict({
        'box_id': list(unspent.keys()), 
        'height': list(map(int, unspent.values())), 
        'is_unspent': [b!=-1 for b in list(unspent.values())]
    })
    # logger.info(df)
    df.to_sql('checkpoint', eng, if_exists='replace')

    # execute as transaction
    with eng.begin() as con:
        # remove spent
        sql = f'''
            delete from boxes
            where box_id in (
                select box_id
                from checkpoint 
                where is_unspent = false
            );
        '''
        con.execute(sql)

        # add unspent
        sql = f'''
            insert into boxes (box_id, height, is_unspent)
                select box_id, height, is_unspent
                from checkpoint 
                where is_unspent = true;
        '''
        con.execute(sql)

        sql = f'''
            insert into audit_log (height)
            values ({int(blk)})
        '''
        con.execute(sql)

### MAIN
async def main(args):
    # find unspent boxes at current height
    node_info = get_node_info()
    current_height = node_info['fullHeight']
    node_version = node_info['appVersion']
    node_network = node_info['network']
    unspent = {}

    # init or pull boxes from sql into unspent
    eng = create_engine(DB_DANAIDES)
    if args.truncate:
        logger.warning('Truncate requested...')
        sql = text(f'''truncate table boxes''')
        eng.execute(sql)
        sql = text(f'''insert into audit_log (height) values (0)''')
        eng.execute(sql)
    sql = text(f'''select height from audit_log order by created_at desc limit 1''')
    ht = eng.execute(sql).fetchone()
    
    # start from saved, if exists
    starting_height = 0
    if ht is not None:
        if ht['height'] > 0:
            logger.info(f'Existing boxes found...')
            starting_height = ht['height']+1
    
    # create from scratch
    else:
        # in the beginning...
        res = get(f'{NODE_URL}/utxo/genesis', headers=headers, timeout=2)
        if not res.ok:
            logger.error(f'unable to determine genesis blocks {res.text}')

        # init unspent blocks
        genesis_blocks = res.json()
        for gen in genesis_blocks:
            box_id = gen['boxId']
            unspent[box_id] = True # height 0

    # lets gooooo...
    logger.info(f'''
    Find Unspent Boxes...
        current: {current_height}
          start: {starting_height}
           node: {node_network}/{node_version}
    ''')
    printProgressBar(starting_height, current_height, prefix = 'Progress:', length = 50)
    for blk in range(starting_height, current_height):
        if VERBOSE: logger.debug(f'{blk}: {len(unspent.keys())}')
        res = get(f'{NODE_URL}/blocks/at/{blk}', headers=headers, timeout=2)
        if not res.ok:
            logger.warning(f'block header request failed {res.text}')
        else:
            block_headers = res.json()
            for hdr in block_headers:
                res = get(f'{NODE_URL}/blocks/{hdr}/transactions', headers=headers, timeout=2)
                if not res.ok:
                    logger.warning(f'block transaction request failed {res.text}')
                else:
                    block_transactions = res.json()['transactions']
                    for tx in block_transactions:
                        if VERBOSE: logger.debug(f'  removing inputs')
                        unspent = await del_inputs(tx['inputs'], unspent)
                        if VERBOSE: logger.debug(f'  adding outputs')
                        unspent = await add_outputs(tx['outputs'], unspent, blk)
        
        # don't checkpoint at height 0
        if blk > 0:

            # update progress bar on screen
            if blk%UPDATE_INTERVAL == 0:
                suffix = f'''{blk}/{len(unspent.keys())}/{t.split()} ({len(blips)} blips)'''
                printProgressBar(blk, current_height, prefix='Progress:', suffix=suffix, length=50)

            # save current unspent to sql
            if (blk%CHECKPOINT_INTERVAL == 0) or (blk == current_height):
                await checkpoint(blk, current_height, unspent, eng)
                unspent = {}

    # keep track of the stragglers
    if unspent == {}:
        logger.info('No new boxes...')
    else:
        await checkpoint(blk, current_height, unspent, eng)

    total_value = 0
    for val in unspent.values():
        total_value += val
    total_value /= NERGS2ERGS

    return {
        'current_height' : current_height,
        'num_unspent_boxes': len(unspent.keys()),
        'total_value'  : total_value,
        'blips': blips
    }

if __name__ == '__main__':
    t = Timer()
    t.start()

    res = asyncio.run(main(args))
    logger.info(f'''
        SUMMARY:
        ========
        current height : {res['current_height']}
        # unspent boxes: {res['num_unspent_boxes']}
            total value: {res['num_unspent_boxes']:,.2f}
    ''')

    t.stop()
