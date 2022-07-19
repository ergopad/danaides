import asyncio
import os, sys, time, signal
import pandas as pd
import argparse

from utils.db import eng, text
from utils.logger import logger, myself, Timer, printProgressBar, LEIF
from utils.ergo import get_node_info, get_genesis_block, headers, NODE_URL, NODE_APIKEY
from utils.aioreq import get_json_ordered
from plugins import staking, prices, utxo
from ergo_python_appkit.appkit import ErgoAppKit, ErgoValue

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

PRETTYPRINT = False
VERBOSE = False
FETCH_INTERVAL = 1500
PLUGINS = dotdict({
    'staking': True, 
    'utxo': True,
    'prices': True,
})
headers = {'Content-Type': 'application/json', 'api_key': NODE_APIKEY}
blips = []

#region FUNCTIONS
# remove all inputs from current block
async def del_inputs(inputs: dict, unspent: dict, height: int = -1) -> dict:
    try:
        new = unspent
        for i in inputs:
            box_id = i['boxId']
            try:
                # new[box_id] = height
                new[box_id] = {
                    'height': height,
                    'nergs': 0
                }
            except Exception as e:
                blips.append({'box_id': box_id, 'height': height, 'msg': f'cant remove'})
                if VERBOSE: logger.warning(f'cant find {box_id} at height {height} while removing from unspent {e}')
        return new

    except Exception as e:
        logger.error(f'ERR: find tokens {e}')
        return {}

# add all outputs from current block
async def add_outputs(outputs: dict, unspent: dict, height: int = -1) -> dict:
    try:
        new = unspent
        for o in outputs:
            box_id = o['boxId']
            nergs = o['value']
            # amount = o['value']
            try:
                # new[box_id] = height
                new[box_id] = {
                    'height': height,
                    'nergs': nergs
                }
            except Exception as e:
                blips.append({'box_id': box_id, 'height': height, 'msg': f'cant add'})
                if VERBOSE: logger.warning(f'{box_id} exists at height {height} while adding to unspent {e}')
        return new

    except Exception as e:
        logger.error(f'ERR: add outputs {e}')
        return {}

async def find_tokens(transactions: dict, tokens: dict, height: int = -1) -> dict:
    try:
        # find all input box ids
        new = tokens
        for tx in transactions:
            input_boxes = [i['boxId'] for i in tx['inputs']]
            for o in tx['outputs']:
                for a in o['assets']:
                    try:
                        if a['tokenId'] in input_boxes:
                            token_id = a['tokenId'] # this is also the box_id
                            
                            # if already exists, don't redo work
                            if token_id not in new:
                                try: token_name = ''.join([chr(r) for r in ErgoAppKit.deserializeLongArray(o['additionalRegisters']['R4'])])
                                except: token_name = ''
                                if 'R6' in o['additionalRegisters']:
                                    try: decimals = ''.join([chr(r) for r in ErgoAppKit.deserializeLongArray(o['additionalRegisters']['R6'])])
                                    except: decimals = ErgoValue.fromHex(o['additionalRegisters']['R6']).getValue()
                                else:
                                    decimals = 0
                                try: decimals = int(decimals)
                                except: decimals = 0
                                try: amount = int(a['amount'])
                                except: pass
                                # some funky deserialization issues
                                if type(amount) == int:
                                    if VERBOSE: logger.debug(f'''token found: {token_name}/{decimals}/{token_id}/{amount}''')
                                    new[token_id] = {
                                        'height': height,
                                        'token_name': token_name,
                                        'decimals': decimals,
                                        'amount': amount
                                    }
                    except Exception as e:
                        blips.append({'asset': a, 'height': height, 'msg': f'invalid asset while looking for tokens'})
                        logger.warning(f'invalid asset, {a} at height {height} while fetching tokens {e}')
                        pass

        return new

    except Exception as e:
        logger.error(f'ERR: find tokens {e}')
        return {}

# upsert current chunk
async def checkpoint(height, unspent, tokens, boxes_tablename='boxes', tokens_tablename='tokens_alt'):
    try:
        # unspent
        df = pd.DataFrame.from_dict({
            'box_id': list(unspent.keys()), 
            'height': [n['height'] for n in unspent.values()], # list(map(int, unspent.values())), # 'nergs': list(map(int, [v[0] for v in unspent.values()])), 
            'nerg': [n['nergs'] for n in unspent.values()],
            'is_unspent': [n['height']!=-1 for n in unspent.values()], # [b!=-1 for b in list(unspent.values())]
        })
        # logger.info(df)
        df.to_sql(f'checkpoint_{boxes_tablename}', eng, if_exists='replace')

        # execute as transaction
        with eng.begin() as con:
            # remove spent
            sql = f'''
                with spent as (
                    select box_id
                    from checkpoint_{boxes_tablename}
                    where is_unspent::boolean = false
                )
                delete from {boxes_tablename} t
                using spent s
                where s.box_id = t.box_id
            '''
            if VERBOSE: logger.debug(sql)
            con.execute(sql)

            # add unspent
            sql = f'''
                insert into {boxes_tablename} (box_id, height, is_unspent, nerg)
                    select c.box_id, c.height, c.is_unspent, c.nerg
                    from checkpoint_{boxes_tablename} c
                        left join {boxes_tablename} b on b.box_id = c.box_id
                    where c.is_unspent::boolean = true
                        and b.box_id is null
                    ;
            '''
            if VERBOSE: logger.debug(sql)
            con.execute(sql)

            sql = f'''
                insert into audit_log (height, service)
                values ({int(height)-1}, '{boxes_tablename}')
            '''
            if VERBOSE: logger.debug(sql)
            con.execute(sql)

        # tokens
        if tokens == {}:
            if VERBOSE: logger.debug('No tokens this block...')
        else:
            suffix = f'''TOKENS: Found {len(tokens)} tokens this block...                      '''
            # if PRETTYPRINT: printProgressBar(last_height, height, prefix='[TOKENS]', suffix=suffix, length = 50)
            # else: logger.info(suffix)
            df = pd.DataFrame.from_dict({
                'token_id': list(tokens.keys()), 
                'height': [n['height'] for n in tokens.values()], 
                'amount': [n['amount'] for n in tokens.values()],
                'token_name': [n['token_name'] for n in tokens.values()],
                'decimals': [n['decimals'] for n in tokens.values()], 
            })
            # logger.info(df)
            df.to_sql(f'checkpoint_{tokens_tablename}', eng, if_exists='replace')

            # execute as transaction
            with eng.begin() as con:
                # add unspent
                sql = f'''
                    insert into {tokens_tablename} (token_id, height, amount, token_name, decimals)
                        select c.token_id, c.height, c.amount, c.token_name, c.decimals
                        from checkpoint_{tokens_tablename} c
                            left join {tokens_tablename} t on t.token_id = c.token_id
                        -- unique constraint; but want all others
                        where t.token_id is null
                        ;
                '''
                if VERBOSE: logger.debug(sql)
                con.execute(sql)

                sql = f'''
                    insert into audit_log (height, service, notes)
                    values ({int(height)-1}, '{tokens_tablename}', '{len(tokens)} found')
                '''
                if VERBOSE: logger.debug(sql)
                con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: checkpointing {e}')
        pass        

async def get_all(urls, headers):
    retries = 0
    res = {}
    while retries < 5:
        try:
            res = await get_json_ordered(urls, headers)
            retries = 5
        except:
            retries += 1
            logger.warning(f'retry: {retries}')
            pass

    return res

async def update_tokenomics():
    logger.debug('Tokenomics step...')
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
            with a as (
				select distinct token_id
				from addresses_staking
			)
			, q as (
				select sum(k.amount/power(10, t.decimals)) as amount
				from keys_staking k
					join a on a.token_id = k.stakekey_token_id
					join tokens t on t.stake_token_id = k.token_id
				where t.token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'				
			)
			update tokens set staked = coalesce(amount, 0)
			from q
			where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413';
        '''
        res = con.execute(sql)

        # stake_pool
        sql = f'''
            with u as (
                select (each(assets)).value::bigint as token_amount
                    , (each(assets)).key::varchar(64) as token_id
                from utxos 
                where ergo_tree = '1014040004000e2005cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f312504020402040204040404040205000400040004000402040204000400040004000100d801d601b2a473000095ed938cb2db6308720173010001730293c5b2a4730300c5a7d808d602b2a5730400d603db63087202d604db6308a7d605b27204730500d606db6308b2a4730600d6079592b1720673078cb27206730800027309d6089a8c7205027207d609b2e4c6a70411730a00d19683050193c27202c2a7938cb27203730b00018cb27204730c0001959172087209d801d60ab27203730d00ed938c720a018c720501938c720a02997208720993b17203730e93b2e4c672020411730f00720993b2e4c6b2a57310000411731100999ab2e4c67201041173120072097207d17313'
            )
            , q as (
                select sum(token_amount) as amount
                from u
                where token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
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

### MAIN
async def process(args, t):
    # find unspent boxes at current height
    node_info = get_node_info()
    current_height = node_info['fullHeight']
    unspent = {}
    tokens = {}
    last_height = -1
    tokens_tablename = 'tokens_alt'
    boxes_tablename = ''.join([i for i in args.juxtapose if i.isalpha()]) # only-alpha tablename

    # init or pull boxes from sql into unspent    
    if args.truncate:
        logger.warning('Truncate requested...')
        sql = text(f'''truncate table {boxes_tablename}''')
        eng.execute(sql)
        sql = text(f'''insert into audit_log (height, service) values (0, '{boxes_tablename}')''')
        eng.execute(sql)
    
    # begin at blockchain height from cli
    if args.height >= 0:
        # start from argparse
        logger.info(f'Starting at block; resetting tables {boxes_tablename}, {tokens_tablename}: {args.height}...')
        last_height = args.height
        with eng.begin() as con:
            sql = text(f'''delete from {tokens_tablename} where height >= {args.height}''')
            con.execute(sql)
            sql = text(f'''delete from {boxes_tablename} where height >= {args.height}''')
            con.execute(sql)

    # where to begin reading blockchain
    else:
        # check existing height in audit_log
        sql = f'''
            select height 
            from audit_log 
            where service in ('{boxes_tablename}')
            order by created_at desc 
            limit 1
        '''
        ht = eng.execute(sql).fetchone()
        if ht is not None:
            if ht['height'] > 0:
                last_height = ht['height']+1
                logger.info(f'Existing boxes found, starting at {last_height}...')
    
        # nothing found, start from beginning
        else:
            res = get_genesis_block()
            genesis_blocks = res.json()
            for gen in genesis_blocks:
                box_id = gen['boxId']
                unspent[box_id] = {} # height 0

    # lets gooooo...
    try:
        if last_height >= current_height:
            logger.warning('Already caught up...')
        while last_height < current_height:
            next_height = last_height+FETCH_INTERVAL
            if next_height > current_height:
                next_height = current_height
            batch_order = range(last_height, next_height)

            # find block headers
            suffix = f'''BLOCKS: last: {last_height}/{next_height}, max: {current_height}             '''
            if PRETTYPRINT: printProgressBar(last_height, current_height, prefix=f'{t.split()}s', suffix=suffix, length = 50)
            else: logger.info(suffix)
            urls = [[blk, f'{NODE_URL}/blocks/at/{blk}'] for blk in batch_order]
            block_headers = await get_all(urls, headers)

            # find transactions
            suffix = f'''Transactions: {last_height}/{next_height}..{current_height}             '''
            if PRETTYPRINT: printProgressBar(int(last_height+(FETCH_INTERVAL/3)), current_height, prefix=f'{t.split()}s', suffix=suffix, length = 50)
            else: logger.info(suffix)
            urls = [[hdr[1], f'''{NODE_URL}/blocks/{hdr[2][0]}/transactions'''] for hdr in block_headers if hdr[1] != 0]
            blocks = await get_all(urls, headers)

            # recreate blockchain (must put together in order)
            suffix = f'''Filter Unspent: {last_height}/{next_height}..{current_height}              '''
            if PRETTYPRINT: printProgressBar(int(last_height+(2*FETCH_INTERVAL/3)), current_height, prefix=f'{t.split()}s', suffix=suffix, length = 50)
            else: logger.info(suffix)
            for blk, transactions in sorted([[b[1], b[2]] for b in blocks]):
                for tx in transactions['transactions']:
                    unspent = await del_inputs(tx['inputs'], unspent)
                    unspent = await add_outputs(tx['outputs'], unspent, blk)
                    if args.ignoreplugins:
                        tokens = {}
                    else:
                        tokens = await find_tokens(transactions['transactions'], tokens, blk)

            # checkpoint
            if VERBOSE: logger.debug('Checkpointing...')
            last_height += FETCH_INTERVAL
            suffix = f'Checkpoint at {next_height}...                  '
            if PRETTYPRINT: printProgressBar(next_height, current_height, prefix=f'{t.split()}s', suffix=suffix, length=50)
            else: logger.info(suffix)
            await checkpoint(next_height, unspent, tokens, boxes_tablename, tokens_tablename)
                
            unspent = {}
            tokens = {}

            # tokenomics
            await update_tokenomics()

    except KeyboardInterrupt:
        logger.error('Interrupted.')
        try: sys.exit(0)
        except SystemExit: os._exit(0)         

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')

    return {
        'current_height' : current_height,
        'blips': blips
    }

class App:
    def __init__(self):
        self.shutdown = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        print('Received:', signum)
        self.shutdown = True

    def start(self):
        logger.info("Begin...")

    def stop(self):
        logger.info("Fin.")

    # find all new currnet blocks
    async def process_unspent(self, args):
        # init timer
        t = Timer()
        t.start()

        # process
        res = await process(args, t)
        
        # timer
        sec = t.stop()
        logger.debug(f'Danaides update took {sec:0.4f}s...')
        
        return res['current_height']

    # wait for new height
    async def hibernate(self, last_height):
        current_height = last_height
        infinity_counter = 0
        t = Timer()
        t.start()

        # admin overview
        sql = f'''
                  select count(*) as i, 0 as j, 'assets' as tbl from assets
            union select count(*), max(height), 'staking' from addresses_staking
            union select count(*), 0, 'vesting' from vesting
			union select count(*), max(height), 'utxos' from utxos
            union select count(*), max(height), 'boxes' from boxes
            union select count(*), max(height), 'tokens' from tokens_alt
        '''
        with eng.begin() as con:
            res = con.execute(sql) 
        logger.warning('\n'.join([f'''{r['tbl']}: {r['i']} rows, {r['j']} max height''' for r in res]))

        # chill for next block
        logger.info('Waiting for next block...')
        while last_height == current_height:
            inf = get_node_info()
            current_height = inf['fullHeight']

            if PRETTYPRINT: 
                print(f'''\r({current_height}) {t.split()} Waiting for next block{'.'*(infinity_counter%4)}    ''', end = "\r")
                infinity_counter += 1

            time.sleep(1)

        # cleanup audit log
        logger.debug('Cleanup audit log...')
        sql = f'''
            delete 
            -- select *
            from audit_log 
            where created_at < now() - interval '3 days';            
        '''
        with eng.begin() as con:
            con.execute(sql)

        sec = t.stop()
        logger.log(LEIF, f'Block took {sec:0.4f}s...')        

### MAIN
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # parser.add_argument("-V", "--verbose", help="Wordy", action='store_true')
    parser.add_argument("-J", "--juxtapose", help="Alternative table name", default='boxes')
    parser.add_argument("-B", "--override", help="Process this box", default='')
    parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
    parser.add_argument("-F", "--fetchinterval", help="Begin at this height", type=int, default=1500)
    parser.add_argument("-T", "--truncate", help="Truncate boxes table", action='store_true')
    parser.add_argument("-P", "--prettyprint", help="Progress bar vs. scrolling", action='store_true')
    parser.add_argument("-O", "--once", help="When complete, finish", action='store_true')
    parser.add_argument("-X", "--ignoreplugins", help="Only process boxes", action='store_true')
    parser.add_argument("-V", "--verbose", help="Be wordy", action='store_true')
    args = parser.parse_args()

    if args.ignoreplugins:
        logger.warning('Ignoring plugins...')
        for p in PLUGINS: 
            p=False

    if args.juxtapose != 'boxes': logger.warning(f'Using alt boxes table: {args.juxtapose}...')
    if args.height != -1: logger.warning(f'Starting at height: {args.height}...')
    if args.prettyprint: logger.warning(f'Pretty print...')
    if args.once: logger.warning(f'Processing once, then exit...')
    if args.truncate: logger.warning(f'Truncating {args.juxtapose}...')
    if args.verbose: logger.warning(f'Verbose...')
    if args.fetchinterval != 1500: logger.warning(f'Fetch interval: {args.fetchinterval}...')

    PRETTYPRINT = args.prettyprint
    VERBOSE = args.verbose
    FETCH_INTERVAL = args.fetchinterval
    
    app = App()
    app.start()
    
    while not app.shutdown:
        try:
            # process unspent
            last_height = asyncio.run(app.process_unspent(args))
            args.height = -1 # tell process to use audit_log

            ##
            ## PLUGINS
            ##            

            # UTXOS - process first
            if PLUGINS.utxo:
                logger.warning('PLUGIN: Utxo...')
                # call last_height-1 as node info returns currently in progress block height, and plugin needs to process the previous block
                asyncio.run(utxo.process(use_checkpoint=True, boxes_tablename=args.juxtapose, box_override=args.override))

            # PRICES
            if PLUGINS.prices:
                logger.warning('PLUGIN: Prices...')
                asyncio.run(prices.process())

            # STAKING
            if PLUGINS.staking:
                logger.warning('PLUGIN: Staking...')
                sql = f'''
                    select height 
                    from audit_log 
                    where service = 'staking'
                    order by created_at desc 
                    limit 1
                '''
                res = eng.execute(sql).fetchone()
                last_staking_block = 0  
                if res is not None: 
                    logger.info('Existing staking info found...')
                    last_staking_block = res['height']
                else:
                    logger.info('No staking info found; rebuiding from height 0...')
                use_checkpoint = last_staking_block == last_height
                logger.debug('Sync main and staking plugin...')
                asyncio.run(staking.process(last_staking_block, use_checkpoint=use_checkpoint, boxes_tablename=args.juxtapose))

            # quit or wait for next block
            if args.once:
                app.stop()
                try: sys.exit(0)
                except SystemExit: os._exit(0)            
            else:
                asyncio.run(app.hibernate(last_height))

        except KeyboardInterrupt:
            logger.debug('Interrupted.')
            app.stop()
            try: sys.exit(0)
            except SystemExit: os._exit(0)            

        except Exception as e:
            logger.error(f'ERR: {myself()}, {e}')

    # fin
    app.stop()
    try: sys.exit(0)
    except SystemExit: os._exit(0)            

