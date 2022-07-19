import asyncio
import argparse
import pandas as pd
import json

from time import sleep 
# from sqlalchemy import create_engine, text
from utils.logger import logger, Timer, printProgressBar, LEIF
from utils.db import eng, text, engErgopad
from utils.ergo import get_node_info, headers, NODE_APIKEY, NODE_URL, ERGOPAD_API
from utils.aioreq import get_json, get_json_ordered
from ergo_python_appkit.appkit import ErgoValue
from requests import get

# ready, go
ERGUSD_ORACLE_API = 'https://erg-oracle-ergusd.spirepools.com/frontendData'
PRETTYPRINT = False
VERBOSE = False
NERGS2ERGS = 10**9
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 5000 # save progress every X blocks
CLEANUP_NEEDED = False
TOKENS = {}
TOKEN_AGGS = {}

async def checkpoint(found, found_aggs):
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

    tokens = {'agg_id': [], 'token_id': [], 'amount': [], 'box_id': [], 'height': []}
    for box_id, info in found_aggs.items():
        token_id = info['token_id']
        # tokens['address'].append(info['address'])
        tokens['agg_id'].append(info['agg_id'])
        tokens['token_id'].append(info['token_id'])
        tokens['amount'].append(info['amount'])
        tokens['box_id'].append(box_id)
        tokens['height'].append(info['height'])
    df_addresses = pd.DataFrame().from_dict(tokens)
    df_addresses.to_sql(f'checkpoint_found_aggs', eng, if_exists='replace')

    sql = text(f'''
        insert into tokens_tokenomics_agg (agg_id, token_id, amount, box_id, height)
            select agg_id, token_id, amount, box_id, height
            from checkpoint_found_aggs
    ''')
    with eng.begin() as con:
        con.execute(sql)

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

async def process(use_checkpoint=False, last_height=-1, juxtapose='boxes', box_override=''):
    try:
        box_id = -1
        # manual boxes tablename
        # box_override = '331a963bbb33542f347aac7be1259980b08284e9a54dcf21e60342104820ba65'
        # box_override = 'ef7365a0d1817873e1f8e537ed0cc4dd32f80beb7f3f71799fb1a7da5f7d1802'
        # last_height = args.height
        # box_override = args.override
        boxes_tablename = ''.join([i for i in juxtapose if i.isalpha()]) # only-alpha tablename

        logger.info(f'Setup...')
        t = Timer()
        t.start()

        # Find proper height
        if last_height == -1 or use_checkpoint:
            sql = text(f'''
                select coalesce(max(height), 0) as height
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
            logger.warning('Box override found...')
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
        logger.debug('  spent tokens_tokenomics')
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
        with eng.begin() as con:
            con.execute(sql)

        # remove spent boxes
        logger.debug('  spent tokens_tokenomics_agg')
        sql = text(f'''
            with spent as (
                select t.box_id
                from tokens_tokenomics_agg t
                    left join boxes b on b.box_id = t.box_id
                where b.box_id is null
            )
            delete from tokens_tokenomics_agg t
            using spent s
            where s.box_id = t.box_id
        ''')
        with eng.begin() as con:
            con.execute(sql)

        # if height specified, cleanup
        if last_height > -1:
            # reset to height
            logger.debug('  >= last_height')
            sql = text(f'''
                delete from tokens_tokenomics
                where height >= :last_height
            ''')
            with eng.begin() as con:
                con.execute(sql, {'last_height': last_height})

            # reset to height
            logger.debug('  >= last_height')
            sql = text(f'''
                delete from tokens_tokenomics_agg
                where height >= :last_height
            ''')
            with eng.begin() as con:
                con.execute(sql, {'last_height': last_height})

        # TOKEN Aggregations
        logger.debug('Token aggregations...')
        sql = f'''
            select id, ergo_tree, address, token_id, amount, notes
            from token_agg
        '''
        with eng.begin() as con:
            res = con.execute(sql).fetchall()
        for r in res:
            TOKEN_AGGS[r['ergo_tree']] = {
                'agg_id': r['id'],
                'address': r['address'],
                'token_id': r['token_id'],
                'amount': r['amount'],
                'notes': r['notes'],
            }
        
        # TOKENS
        logger.debug('Gather tokens...')
        sql = f'''
            select token_id, token_name, decimals, token_price
            from tokens
        '''
        with eng.begin() as con:
            res = con.execute(sql).fetchall()
        for r in res:
            token_name = r['token_name']
            TOKENS[r['token_id']] = {
                'token_name': token_name,
                'decimals': r['decimals'],
                'price': r['token_price'],
                'amount': 0
            }            
        if VERBOSE: logger.info(TOKENS)

        max_height = 0
        last_r = 1

        logger.info(f'Begin processing, {box_count} boxes total...')
        token_counter = ', '.join([f'''{t['token_name']}:-''' for token_id, t in TOKENS.items()])
        for r in range(last_r-1, box_count, CHECKPOINT_INTERVAL):
            next_r = r+CHECKPOINT_INTERVAL
            if next_r > box_count:
                next_r = box_count

            suffix = f'{t.split()} :: ErgoPad ({token_counter})...'+(' '*20)
            if PRETTYPRINT: printProgressBar(r, box_count, prefix='Progress:', suffix=suffix, length=50)
            else: logger.debug(suffix)

            try:
                ergopad = {}
                aggs = {}

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
                                # logger.warning(f'''Found [token] in box {box_id}                    ''')
                                ergopad[box_id] = {
                                    'address': address,
                                    'amount': asset['amount'],
                                    'token_id': asset['tokenId'],
                                    'height': height,
                                }

                        # emitted, vested, staked, blah blah...
                        if address in TOKEN_AGGS:
                            # logger.warning(f'''Agg address found {box_id}                    ''')
                            for asset in assets: # TODO: potential to overwrite box values if multiple assets found
                                if TOKEN_AGGS[address]['token_id'] == asset['tokenId']:
                                    logger.warning(f'''Agg token in box {box_id}                    ''')
                                    aggs[box_id] = {
                                        'agg_id': TOKEN_AGGS[address]['agg_id'],
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
                await checkpoint(ergopad, aggs)

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
        with eng.begin() as con:
            # vested
            sql = f'''
				with u as (
                    select (each(assets)).value::bigint as token_amount
                    from utxos
                    where ergo_tree = '10070400040204000500040004000400d80bd601e4c6a7040ed602b2db6308a7730000d6038c720201d604e4c6a70805d605e4c6a70705d606e4c6a70505d607e4c6a70605d6089c9d99db6903db6503fe720572067207d6098c720202d60a9972047209d60b958f99720472087207997204720a997208720ad1ed93b0b5a5d9010c63ededed93c2720c720193e4c6720c040ee4c6a7090e93b1db6308720c7301938cb2db6308720c7302000172037303d9010c41639a8c720c018cb2db63088c720c0273040002720bec937209720baea5d9010c63ededededededededed93c1720cc1a793c2720cc2a7938cb2db6308720c730500017203938cb2db6308720c73060002997209720b93e4c6720c040e720193e4c6720c0505720693e4c6720c0605720793e4c6720c0705720593e4c6720c0805720493e4c6720c090ee4c6a7090e'
                )
				, q as (
					select sum(token_amount) as amount
					from u
				)
                update tokens set vested = coalesce(amount, 0)
                from q
				where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
            '''
            res = con.execute(sql)

            # emitted
            sql = f'''
				with u as (
                    select (each(assets)).value::bigint as token_amount
                    from utxos
                    where ergo_tree = '102d04000e20d71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de41304000e2005cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f312504040404040604020400040204040402050004020400040004020402040404040400040004000402040004000e201028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee040004020100040604000500040204000400040204020400040204020404040404060100d802d601b2a4730000d602730195ed938cb2db6308720173020001730393c5b2a4730400c5a7d80ad603b2a5730500d604e4c672030411d605e4c672010411d606b27204730600d607b2a4730700d608b2e4c672070411730800d609db6308a7d60a9a8cb2db63087207730900029592b17209730a8cb27209730b0002730cd60bdb63087203d60cb2720b730d00d19683080193c27203c2a793b27204730e00b27205730f0093b27204731000b2720573110093b27204731200b27205731300937206958f7208720a7208720a938cb2720b731400018cb2720973150001938c720c017202938c720c0272069593c57201c5a7d80ad603b2a5731600d604db63087203d605db6308a7d6068cb2720573170002d607b5a4d9010763d801d609db630872079591b172097318ed938cb2720973190001731a93b2e4c672070411731b00b2e4c6a70411731c00731dd608e4c6a70411d609b27208731e00d60ab27208731f00d60bb072077320d9010b41639a8c720b019d9c8cb2db63088c720b02732100027209720ad60ce4c672030411d19683070193c27203c2a7938cb27204732200018cb272057323000195907206720b93b172047324d801d60db27204732500ed938c720d017202928c720d02997206720b93b2720c732600720a93b2720c732700b2720873280093b2720c73290099b27208732a007eb172070593b2720c732b007209d1732c'
                )
				, q as (
					select sum(token_amount) as amount
					from u
				)
                update tokens set emitted = coalesce(amount, 0)
                from q
				where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
            '''
            res = con.execute(sql)

            # staked
            sql = f'''
				with u as (
                    select (each(assets)).value::bigint as token_amount
                    from utxos
                    where ergo_tree = '1017040004000e200549ea3374a36b7a22a803766af732e61798463c3332c5f6d86c8ab9195eed59040204000400040204020400040005020402040204060400040204040e2005cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f312504020402010001010100d802d601b2a4730000d6028cb2db6308720173010001959372027302d80bd603b2a5dc0c1aa402a7730300d604e4c672030411d605e4c6a70411d606db63087203d607b27206730400d608db6308a7d609b27208730500d60ab27206730600d60bb27208730700d60c8c720b02d60de4c672010411d19683090193c17203c1a793c27203c2a793b272047308009ab27205730900730a93e4c67203050ee4c6a7050e93b27204730b00b27205730c00938c7207018c720901938c7207028c720902938c720a018c720b01938c720a029a720c9d9cb2720d730d00720cb2720d730e00d801d603b2a4730f009593c57203c5a7d801d604b2a5731000d1ed93720273119593c27204c2a7d801d605c67204050e95e67205ed93e47205e4c6a7050e938cb2db6308b2a573120073130001e4c67203050e73147315d17316'
                )
				, q as (
					select sum(token_amount) as amount
					from u
                    where token_amount != 1
				)
                update tokens set staked = coalesce(amount, 0)
                from q
				where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
            '''
            res = con.execute(sql)

            # stake_pool
            sql = f'''
				with u as (
                    select (each(assets)).value::bigint as token_amount
                    from utxos
                    where ergo_tree = '1014040004000e2005cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f312504020402040204040404040205000400040004000402040204000400040004000100d801d601b2a473000095ed938cb2db6308720173010001730293c5b2a4730300c5a7d808d602b2a5730400d603db63087202d604db6308a7d605b27204730500d606db6308b2a4730600d6079592b1720673078cb27206730800027309d6089a8c7205027207d609b2e4c6a70411730a00d19683050193c27202c2a7938cb27203730b00018cb27204730c0001959172087209d801d60ab27203730d00ed938c720a018c720501938c720a02997208720993b17203730e93b2e4c672020411730f00720993b2e4c6b2a57310000411731100999ab2e4c67201041173120072097207d17313'
                )
				, q as (
					select sum(token_amount) as amount
					from u
                    where token_amount != 1
				)
                update tokens set stake_pool = coalesce(amount, 0)
                from q
				where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
            '''
            res = con.execute(sql)

            # current total supply and in circulation
            sql = f'''
                with u as (
                    select (each(assets)).key as token_id
                        , (each(assets)).value::bigint as token_amount
                    from utxos
                )
                , s as (
                    -- leave as int (do not /decimals)
                    select sum(token_amount) as current_total_supply
                        , token_id
                    from u
                    where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
                    group by token_id
                )
                update tokens 
                    set current_total_supply = s.current_total_supply
                    , in_circulation = s.current_total_supply - tokens.vested - tokens.emitted - coalesce(tokens.stake_pool, 0)
                from s
				where s.token_id = tokens.token_id
            '''
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
    parser = argparse.ArgumentParser()
    parser.add_argument("-J", "--juxtapose", help="Alternative table name", default='boxes')
    parser.add_argument("-O", "--override", help="Troubleshoot single box", default='')
    parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
    parser.add_argument("-E", "--endat", help="End at this height", type=int, default=10**10)
    parser.add_argument("-V", "--verbose", help="Be wordy", action='store_true')
    parser.add_argument("-P", "--prettyprint", help="Begin at this height", action='store_true')
    args = parser.parse_args()

    PRETTYPRINT = args.prettyprint

    res = asyncio.run(process(use_checkpoint=False, last_height=args.height, juxtapose=args.juxtapose, box_override=args.override))
    print(res)

