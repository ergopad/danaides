import asyncio
import pandas as pd
import argparse
import json

from time import sleep 
from utils.logger import logger, Timer, printProgressBar
from utils.db import eng, text
from utils.ergo import headers, NODE_API
from utils.aioreq import get_json_ordered
from requests import get
from ergo_python_appkit.appkit import ErgoValue

#region INIT
PRETTYPRINT = False
LINELEN = 100
VERBOSE = False
NERGS2ERGS = 10**9
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 5000 # save progress every X blocks
CLEANUP_NEEDED = False
#endregion INIT

#region FUNCTIONS

async def checkpoint(utxos):
    try:
        addr_converter = {}

        # utxos
        contents = {'box_id': [], 'ergo_tree': [], 'address': [], 'nergs': [], 'registers': [], 'assets': [], 'transaction_id': [], 'creation_height': [], 'height': []}
        for box_id, content in utxos.items():
            contents['box_id'].append(box_id)
            contents['ergo_tree'].append(content['ergo_tree'])
            if content['address'] not in addr_converter:
                r2a = get(f'''{NODE_API}/utils/rawToAddress/{content['address']}''', headers=headers, timeout=2)
                if r2a.ok:
                    pubkey = r2a.json()['address']
                    addr_converter[content['address']] = pubkey
                else: 
                    addr_converter[content['address']] = ''
            contents['address'].append(addr_converter[content['address']])
            contents['nergs'].append(content['nergs'])
            contents['registers'].append(json.dumps(content['registers']))
            contents['assets'].append(json.dumps(content['assets']))
            contents['transaction_id'].append(json.dumps(content['transaction_id']))
            contents['creation_height'].append(json.dumps(content['creation_height']))
            contents['height'].append(content['height'])
        df_box_contents = pd.DataFrame().from_dict(contents)
        df_box_contents.to_sql(f'checkpoint_utxos', eng, if_exists='replace') # , dtype={"new_json_col": sqlalchemy.types.JSON},

        # utxos
        with eng.begin() as con:
            sql = f'''
                insert into utxos (box_id, ergo_tree, address, nergs, registers, assets, transaction_id, creation_height, height)
                    select 
                        box_id::varchar(64)
                        , ergo_tree::text
                        , address::varchar(64)
                        , nergs::bigint
                        , trim(both '"' from registers::text)::hstore as registers
                        , trim(both '"' from assets::text)::hstore as assets
                        , trim(both '"' from transaction_id::text)::varchar(64) as transaction_id
                        , creation_height::int
                        , height::int
                    from checkpoint_utxos
            '''
            con.execute(sql)

    except Exception as e:
        logger.debug(f'ERR: checkpoint {e}')
        pass

async def prepare_destination(boxes_tablename:str):
    logger.info('Remove spent boxes from utxos tables...')

    try:    
        with eng.begin() as con:
            # remove unspent boxes from utxos
            sql = text(f'''
                with spent as (
                    select a.box_id, a.height
                    from utxos a
                        left join {boxes_tablename} b on a.box_id = b.box_id
                    where b.box_id is null
                )
                delete from utxos t
                using spent s
                where s.box_id = t.box_id
                    and s.height = t.height
            ''')
            if VERBOSE: logger.debug(sql)
            con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: Preparing utxo table {e}')
        pass
    
async def get_all_unspent_boxes(boxes_tablename:str, box_override:str):
    # determine next height to start at
    try:
        logger.info('Finding boxes...')
        sql = text(f'''
            select b.box_id, b.height
            from {boxes_tablename} b
                left join utxos u on u.box_id = b.box_id
            where u.box_id is null
        ''')
        # find boxes from checkpoint or standard sql query
        if VERBOSE: logger.debug(sql)
        with eng.begin() as con:
            # TODO: implement box_override
            boxes = con.execute(sql, {'box_override': box_override}).fetchall()

        return boxes

    except Exception as e:
        logger.error(f'ERR: Fetching all unspent boxes {e}')
        pass

async def process(is_plugin:bool=False, args=None):# boxes_tablename:str='boxes', box_override:str='') -> int:
    try:
        t = Timer()
        t.start()

        # handle globals when process called from as plugin
        PRETTYPRINT = False
        if is_plugin:
            if args.prettyprint:
                PRETTYPRINT = True

        # cleanup tablename
        boxes_tablename = ''.join([i for i in args.juxtapose if i.isalpha()]) # only-alpha tablename

        # refresh 
        await prepare_destination(boxes_tablename)
        boxes = await get_all_unspent_boxes(boxes_tablename, args.override)
        box_count = len(boxes)

        if not is_plugin:
            # no special processing for checkpoint call
            logger.info('Sleeping to make sure boxes are processed...')
            sleep(2)    

        max_height = 0 # track max height
        last_r = 1

        # process all new, unspent boxes
        logger.info(f'BOXES: {box_count} boxes found...')
        for r in range(last_r-1, box_count, CHECKPOINT_INTERVAL):
            # the node can sometimes get overwhelmed; in the event, retry
            range_retries = 0
            while range_retries < 3:
                try:
                    if range_retries > 0:
                        logger.warning(f'Retry attempt {range_retries}...')
                        sleep(1) # take a deep breath before trying again...

                    # determine proper slices
                    next_r = r+CHECKPOINT_INTERVAL
                    if next_r > box_count:
                        next_r = box_count

                    suffix = f'''{t.split()} :: Process ({next_r:,}/{box_count:,}) ...'''                    
                    if PRETTYPRINT: printProgressBar(r, box_count, prefix=t.split(), suffix=f'{suffix}{" "*(LINELEN-len(suffix))}', length=50)
                    else: logger.info(suffix)

                    utxos = {}

                    # find all the calls to build boxes
                    if VERBOSE: logger.warning(f'{r}::{next_r}::{boxes[r:next_r]}, len={box_count}')
                    urls = [[box['height'], f'''{NODE_API}/utxo/byId/{box['box_id']}'''] for box in boxes[r:next_r]]
                    if VERBOSE: logger.debug(f'slice: {r}:{next_r} / up to height: {boxes[next_r-1]["height"]}')
                    retries = 0
                    while retries < 5:
                        try:
                            utxo = await get_json_ordered(urls, headers)
                            retries = 5
                        except Exception as e:
                            retries += 1
                            logger.warning(f'get ordered json retry: {retries}; {e}')
                            pass
                    logger.debug(f'Boxes: {box_count}; UTXOs: {len(utxo)}')

                    # fetch box info
                    for ergo_tree, box_id, box_assets, registers, nergs, creation_height, transaction_id, height in [[u[2]['ergoTree'], u[2]['boxId'], u[2]['assets'], u[2]['additionalRegisters'], u[2]['value'], u[2]['creationHeight'], u[2]['transactionId'], u[1]] for u in utxo if u[0] == 200]:
                        if VERBOSE: logger.warning(f'box_id: {box_id}')
                        try:
                            # track largest height processed
                            if max_height < height:
                                max_height = height

                            # only worry about addresses for wallets
                            if ergo_tree[:6] == '0008cd':
                                address = ergo_tree[6:]
                            else:
                                address = ''

                            # utxo row
                            utxos[box_id] = {
                                'box_id': box_id,
                                'ergo_tree': ergo_tree,
                                'address': address,
                                'nergs': nergs,
                                'registers': ','.join([f'''{i}=>{j}''' for i, j in registers.items()]),
                                'assets': ','.join([f'''{a['tokenId']}=>{a['amount']}''' for a in box_assets]),
                                'transaction_id': transaction_id,
                                'creation_height': creation_height,
                                'height': height,
                            }          
                        
                        except Exception as e:
                            logger.error(f'ERR: {e}; box_id: {box_id}')
                            pass

                    # save current unspent to sql
                    suffix = f'Checkpoint...'
                    if PRETTYPRINT: printProgressBar(next_r, box_count, prefix=t.split(), suffix=f'{suffix}{" "*(LINELEN-len(suffix))}', length=50)
                    else:
                        try: percent_complete = f'{100*next_r/box_count:0.2f}%' 
                        except: percent_complete= 0
                        logger.warning(f'{percent_complete}/{t.split()} {suffix}')
                    await checkpoint(utxos)

                    # track utxos height here (since looping through boxes)
                    notes = f'''{len(utxos)} utxos'''
                    sql = text(f'''
                        insert into audit_log (height, service, notes)
                        values ({max_height}, 'utxo', '{notes}')
                    ''')
                    with eng.begin() as con:
                        con.execute(sql)

                    # reset for outer loop: height range
                    last_r = r
                    if args.override != '':
                        exit(1)

                    range_retries = 3

                except KeyError as e:
                    logger.error(f'ERR (KeyError): {e}; {box_id}')
                    pass
                
                except Exception as e:
                    logger.error(f'ERR: {e}; range: {r}')
                    range_retries += 1
                    sleep(1) # give node a break
                    pass

        sec = t.stop()
        logger.debug(f'Processing complete: {sec:0.4f}s...{" "*50}')

        return max_height+1

    except Exception as e:
        logger.error(f'ERR: Process {e}')
        pass

async def hibernate(new_height):
    hibernate_timer = Timer()
    hibernate_timer.start()

    logger.info('Waiting for next block...')

    try:
        infinity_counter = 0
        current_height = new_height
        while new_height >= current_height:
            sql = f'''select max(height) as height from boxes'''
            with eng.begin() as con: res = con.execute(sql).fetchone()
            current_height = res['height']

            if PRETTYPRINT: 
                print(f'''\r{current_height} :: {hibernate_timer.split()} Waiting for next block{'.'*(infinity_counter%4)}    ''', end = "\r")
                infinity_counter += 1

            sleep(1)

        sec = hibernate_timer.stop()
        logger.debug(f'Next block {sec:0.4f}s...')     

        return current_height

    except Exception as e:
        logger.error(f'ERR: Hibernating {e}')
        pass

#endregion FUNCTIONS

#region MAIN

async def main(args):
    # height not super useful, but ok for waiting ... TODO: make better
    new_height = args.height

    while True:
        new_height = await process(new_height, boxes_tablename=args.juxtapose)
        if args.once:
            exit(1)
        else:
            await hibernate(new_height)

# Refresh UTXOs table
# 1. remove all spent boxes
# 2. add utxos where box_id is in boxes/juxtapose
#
# Perform steps irrelevant of height (may add something for this later)
#
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-J", "--juxtapose", help="Alternative table name", default='boxes')
    parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
    # parser.add_argument("-E", "--endat", help="End at this height", type=int, default=10**10)
    parser.add_argument("-P", "--prettyprint", help="Progress bar vs scrolling", action='store_true')
    parser.add_argument("-O", "--once", help="One and done", action='store_true')
    args = parser.parse_args()

    PRETTYPRINT = args.prettyprint

    res = asyncio.run(main(args))

#endregion MAIN