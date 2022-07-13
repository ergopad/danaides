import asyncio
import pandas as pd
import argparse
import json

from time import sleep 
from utils.logger import logger, Timer, printProgressBar
from utils.db import eng, text
from utils.ergo import headers, NODE_URL
from utils.aioreq import get_json_ordered
from requests import get
from ergo_python_appkit.appkit import ErgoValue

#region INIT
PRETTYPRINT = False
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
                r2a = get(f'''{NODE_URL}/utils/rawToAddress/{content['address']}''', headers=headers, timeout=2)
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
                        , trim(both '"' from registers)::hstore as registers
                        , trim(both '"' from assets)::hstore as assets
                        , trim(both '"' from transaction_id)::varchar(64) as transaction_id
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
            boxes = con.execute(sql, {'box_override': box_override}).fetchall()

        return boxes

    except Exception as e:
        logger.error(f'ERR: Fetching all unspent boxes {e}')
        pass

async def build_balances():
    try:
        tbl = 'balances'
        with eng.begin() as con:
            # drop
            sql = text(f'''truncate table {tbl}''')
            con.execute(sql)

            # ...and pop
            sql = text(f'''
                insert into {tbl} (address, nergs)
                    select address, sum(nergs) as nergs 
                    from utxos
                    where address != '' -- only wallets; no smart contracts
                    group by address;
            ''')
            con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: building balances {e}')

async def build_assets():
    try:
        tbl = 'assets'
        with eng.begin() as con:
            # drop
            sql = text(f'''truncate table {tbl}''')
            con.execute(sql)

            # ...and pop
            sql = text(f'''
                with a as (
                    select 
                        address
                        , (each(assets)).key::varchar(64) as token_id
                        , (each(assets)).value::bigint as amount
                    from utxos
                    where address != '' -- only wallets; no smart contracts
                )
                -- truncate table assets
                insert into {tbl} (address, token_id, amount)
                    select 
                        address
                        , token_id
                        , sum(amount) as amount
                    from a 
                    group by address, token_id;
            ''')
            con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: building balances {e}')

async def build_vesting():
    try:
        tbl = 'vesting'
        with eng.begin() as con:
            # drop
            sql = text(f'''truncate table {tbl}''')
            con.execute(sql)

            # ...and pop
            sql = text(f'''
                with v as (
                    select id 
                        , ergo_tree
                        , box_id
                        , registers->'R4' as parameters
                        , right(registers->'R5', length(registers->'R5')-4) vesting_key_id
                        , (each(assets)).key::varchar(64) as token_id
                        , (each(assets)).value as remaining
                    from utxos
                    where ergo_tree in (
                            '100e04020400040404000402040604000402040204000400040404000400d810d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605e4c6a70411d6069d99db6903db6503feb27205730200b27205730300d607b27205730400d608b27205730500d6099972087204d60a9592720672079972087209999d9c7206720872077209d60b937204720ad60c95720bb2a5730600b2a5730700d60ddb6308720cd60eb2720d730800d60f8c720301d610b2a5730900d1eded96830201aedb63087201d901114d0e938c721101720293c5b2a4730a00c5a79683050193c2720cc2720193b1720d730b938cb2720d730c00017202938c720e01720f938c720e02720aec720bd801d611b2db63087210730d009683060193c17210c1a793c27210c2a7938c721101720f938c721102997204720a93e4c67210050e720293e4c6721004117205',
                            '1012040204000404040004020406040c0408040a050004000402040204000400040404000400d812d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605db6903db6503fed606e4c6a70411d6079d997205b27206730200b27206730300d608b27206730400d609b27206730500d60a9972097204d60b95917205b272067306009d9c7209b27206730700b272067308007309d60c959272077208997209720a999a9d9c7207997209720b7208720b720ad60d937204720cd60e95720db2a5730a00b2a5730b00d60fdb6308720ed610b2720f730c00d6118c720301d612b2a5730d00d1eded96830201aedb63087201d901134d0e938c721301720293c5b2a4730e00c5a79683050193c2720ec2720193b1720f730f938cb2720f731000017202938c7210017211938c721002720cec720dd801d613b2db630872127311009683060193c17212c1a793c27212c2a7938c7213017211938c721302997204720c93e4c67212050e720293e4c6721204117206'
                        )
                        -- and box_id = '9643bcdff57059490fdd2af3382248ffb9ce3739aaad032e335a0842d0081c07'
                )
                insert into {tbl} (box_id, vesting_key_id, parameters, token_id, remaining, address, ergo_tree)
                    select v.box_id
                        , v.vesting_key_id::varchar(64)
                        , v.parameters::varchar(1024)
                        , v.token_id::varchar(64)
                        , v.remaining::bigint -- need to divide by decimals
                        , a.address::varchar(64)
                        , v.ergo_tree::text
                    from v
                        -- filter to only vesting keys
                        join assets a on a.token_id = v.vesting_key_id            
            ''')
            con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: building balances {e}')

async def process(use_checkpoint:bool=False, boxes_tablename:str='boxes', box_override:str='') -> int:
    try:

        t = Timer()
        t.start()

        # cleanup tablename
        boxes_tablename = ''.join([i for i in boxes_tablename if i.isalpha()]) # only-alpha tablename

        # refresh 
        await prepare_destination(boxes_tablename)
        boxes = await get_all_unspent_boxes(boxes_tablename, box_override)
        box_count = len(boxes)
        logger.debug(f'Found {box_count} boxes to process...')

        if not use_checkpoint:
            # no special processing for checkpoint call
            logger.info('Sleeping to make sure boxes are processed...')
            sleep(2)    

        max_height = 0 # track max height
        utxo_counter = 0
        last_r = 1

        # process all new, unspent boxes
        logger.info(f'Begin processing, {box_count} boxes total...')
        for r in range(last_r-1, box_count, CHECKPOINT_INTERVAL):
            try:
                # the node can sometimes get overwhelmed; in the event, retry
                retries = 0
                while retries < 3:
                    if retries > 0:
                        logger.warning(f'Retry attempt {retries}...')
                        sleep(1) # take a deep breath before trying again...

                    # determine proper slices
                    next_r = r+CHECKPOINT_INTERVAL
                    if next_r > box_count:
                        next_r = box_count

                    suffix = f'''{t.split()} :: ({utxo_counter}) Process ...'''+(' '*20)
                    if PRETTYPRINT: printProgressBar(r, box_count, prefix='Progress:', suffix=suffix, length=50)
                    else: logger.debug(suffix)

                    utxos = {}

                    # find all the calls to build boxes
                    logger.warning(f'{r}::{next_r}::{boxes[r:next_r]}, len={box_count}')
                    urls = [[box['height'], f'''{NODE_URL}/utxo/byId/{box['box_id']}'''] for box in boxes[r:next_r]]
                    if VERBOSE: logger.debug(f'slice: {r}:{next_r} / up to height: {boxes[next_r-1]["height"]}')
                    while retries < 5:
                        try:
                            utxo = await get_json_ordered(urls, headers)
                            retries = 5
                        except:
                            retries += 1
                            logger.warning(f'get ordered json retry: {retries}')
                            pass

                    # fetch box info
                    for ergo_tree, box_id, box_assets, registers, nergs, creation_height, transaction_id, height in [[u[2]['ergoTree'], u[2]['boxId'], u[2]['assets'], u[2]['additionalRegisters'], u[2]['value'], u[2]['creationHeight'], u[2]['transactionId'], u[1]] for u in utxo if u[0] == 200]:
                        logger.warning(box_id)
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
                    suffix = f'{t.split()} :: ({utxo_counter}) Checkpoint...'+(' '*20)
                    if PRETTYPRINT: printProgressBar(next_r, box_count, prefix='Progress:', suffix=suffix, length=50)
                    else: logger.info(suffix)
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
                    if box_override != '':
                        exit(1)

                    retries = 3

            except KeyError as e:
                logger.error(f'ERR (KeyError): {e}; {box_id}')
                pass
            
            except Exception as e:
                logger.error(f'ERR: {e}; range: {r}')
                retries += 1
                sleep(1) # give node a break
                pass

        logger.debug('Rebuild balances table...')
        await build_balances()
        
        logger.debug('Rebuild assets table...')
        await build_assets()

        logger.debug('Rebuild vesting table...')
        await build_vesting()

        # await build_staking()
        # await build_tokenomics()

        sec = t.stop()
        logger.debug(f'Processing complete: {sec:0.4f}s...                ')

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