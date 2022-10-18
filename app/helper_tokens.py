import asyncio
import os, sys
import pandas as pd
import argparse

from requests import get
from time import sleep
from utils.db import eng
from utils.logger import logger, myself, Timer, printProgressBar, LEIF
from utils.ergo import get_node_info, NODE_API
from utils.aioreq import get_json_ordered
from ergo_python_appkit.appkit import ErgoAppKit, ErgoValue

"""
tokens.py
---------

- current ability to refresh token table to validate that all tokens have been caught in current process
- working towards replacing current token.py plugin with this

"""

#region INIT
FETCH_INTERVAL = 500
LINELEN = 100
PRETTYPRINT = False
VERBOSE = False
NERGS2ERGS = 10**9
TOKENS = 'tokens'

async def refresh(height, tokens):
    try:
        # init
        stats = {}

        # dataframe - create dataset to push to sql table
        if VERBOSE: logger.debug(tokens)
        df = pd.DataFrame.from_dict({
            'token_id': list(tokens.keys()), 
            'height': [n['height'] for n in tokens.values()], 
            'amount': [n['amount'] for n in tokens.values()],
            'token_name': [n['token_name'] for n in tokens.values()],
            'decimals': [n['decimals'] for n in tokens.values()], 
        })
        if VERBOSE: logger.warning(df)
        df.to_sql(f'{TOKENS}_refresh', eng, schema='checkpoint', if_exists='replace')
        if VERBOSE: logger.debug(f'saved to checkpoint.{TOKENS}_refresh')

        # stats - find new/existing tokens
        sql = f'''
            with 
            new as (
                select count(*) as i
                from checkpoint.{TOKENS}_refresh c
                    left join {TOKENS} t on t.token_id = c.token_id
                where t.token_id is null
            )
            , xst as (
                select count(*) as i
                from checkpoint.{TOKENS}_refresh c
                    join {TOKENS} t on t.token_id = c.token_id
            )
            select new.i as new
                , xst.i as xst
            from new
            cross join xst
        '''
        with eng.begin() as con:
            if VERBOSE: logger.debug(sql)
            i = con.execute(sql).fetchone()

        stats['new'] = i['new']
        stats['existing'] = i['xst']

        # execute as transaction
        if i['new'] > 0:
            with eng.begin() as con:
                sql = f'''
                    select c.token_id
                    from checkpoint.{TOKENS}_refresh c
                        left join {TOKENS} t on t.token_id = c.token_id
                    where t.token_id is null
                '''
                tok = con.execute(sql).fetchall()
                for t in tok:
                    logger.warning(t['token_id'])

                # add new tokens
                sql = f'''
                    insert into {TOKENS} (token_id, height, amount, token_name, decimals)
                        select c.token_id, c.height, c.amount, c.token_name, c.decimals
                        from checkpoint.{TOKENS}_refresh c
                            left join {TOKENS} t on t.token_id = c.token_id
                        where t.token_id is null
                        ;
                '''
                if VERBOSE: logger.debug(sql)
                con.execute(sql)

                sql = f'''
                    insert into audit_log (height, service, notes)
                    values ({int(height)-1}, '{TOKENS}', 'refresh: {i['new']} found, {i['xst']} existing')
                '''
                if VERBOSE: logger.debug(sql)
                con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: checkpointing {e}')
        pass  

    return stats

# extract tokens from the bolckchain
async def process(transactions: dict, tokens: dict, height: int, is_plugin: bool=False, args=None) -> dict:
    try:
        # handle globals when process called from as plugin
        if is_plugin and (args != None):
            if args.prettyprint: PRETTYPRINT = True

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
                        # BLIPS.append({'asset': a, 'height': height, 'msg': f'invalid asset while looking for tokens'})
                        logger.warning(f'invalid asset, {a} at height {height} while fetching tokens {e}')
                        pass

        return new

    except Exception as e:
        logger.error(f'ERR: find tokens {e}')
        return {}

def cli():
    global PRETTYPRINT
    global VERBOSE
    global FETCH_INTERVAL

    parser = argparse.ArgumentParser()
    
    parser.add_argument("-B", "--override", help="Process this box", default='')
    parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
    parser.add_argument("-Z", "--sleep", help="Begin at this height", type=int, default=0)
    parser.add_argument("-F", "--fetchinterval", help="Begin at this height", type=int, default=FETCH_INTERVAL)
    parser.add_argument("-P", "--prettyprint", help="Progress bar vs. scrolling", action='store_true')
    parser.add_argument("-O", "--once", help="When complete, finish", action='store_true')
    parser.add_argument("-V", "--verbose", help="Be wordy", action='store_true')
    
    args = parser.parse_args()

    if args.juxtapose != 'boxes': logger.warning(f'Using alt boxes table: {args.juxtapose}...')        
    if args.height != -1: logger.warning(f'Starting at height: {args.height}...')
    if args.prettyprint: logger.warning(f'Pretty print...')
    if args.once: logger.warning(f'Processing once, then exit...')
    if args.verbose: logger.warning(f'Verbose...')
    if args.fetchinterval != 1500: logger.warning(f'Fetch interval: {args.fetchinterval}...')

    PRETTYPRINT = args.prettyprint
    VERBOSE = args.verbose
    FETCH_INTERVAL = args.fetchinterval    

    return args

async def get_all(urls) -> dict:
    retries = 1
    res = {}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko', "Content-Type": "application/json"} # {'Content-Type': 'application/json'}

    # try until successful; missing a valid response will invalidate the database
    while retries > 0:
        # loop 3 times, then pause
        while res == {} and (retries%3 > 0):
            try:
                res = await get_json_ordered(urls, headers)
                retries = 0

            # try, try again
            except Exception as e:
                retries += 1
                res = {}
                logger.warning(f'retry: {retries} ({e})')

        # take a beat
        if retries > 0:
            logger.warning(f'WARN: {retries} retries; sleeping for 20s.')
            sleep(20)
            retries = 1
            res = {}
    
    return res

#region MAIN
# goal of main is to refresh all tokens
# this is useful outside of being called as a plugin to re-scan the entier blockchain and not just the current blocks
async def main(args):
    # init
    tokens = {}
    new_tokens = 0
    old_tokens = 0
    last_height = 1
    current_height = 1
    t = Timer()
    t.start()

    # find current height
    node_info = get_node_info()
    current_height = node_info['fullHeight']

    # lets gooooo...
    try:
        # process blocks until caught up with current height
        while last_height < current_height:
            next_height = last_height+FETCH_INTERVAL
            if next_height > current_height:
                next_height = current_height
            batch_order = range(last_height, next_height)

            # find block headers
            suffix = f'''{last_height}-{next_height} / {current_height} BLOCKS'''
            prefix=f'''({new_tokens}/{old_tokens}) {t.split()}'''
            if PRETTYPRINT: printProgressBar(last_height, current_height, prefix=prefix, suffix=f'{suffix}{" "*(LINELEN-len(suffix))}', length=50)
            else: 
                try: percent_complete = f'{100*last_height/current_height:0.2f}%'
                except: percent_complete= 0
                logger.info(f'{percent_complete}/{t.split()} {suffix}')
            urls = [[blk, f'{NODE_API}/blocks/at/{blk}'] for blk in batch_order]
            block_headers = await get_all(urls)

            # find transactions
            suffix = f'''{last_height}-{next_height} / {current_height} TRANSACTIONS'''
            # prefix=f'''({new_tokens}/{old_tokens}) {t.split()}'''
            if PRETTYPRINT: printProgressBar(int(last_height+(FETCH_INTERVAL/3)), current_height, prefix=prefix, suffix=f'{suffix}{" "*(LINELEN-len(suffix))}', length=50)
            else: 
                try: percent_complete = f'{100*int(last_height+(2*FETCH_INTERVAL/3))/current_height:0.2f}%'
                except: percent_complete= 0
                logger.info(f'{percent_complete}/{t.split()} {suffix}')
            urls = [[hdr[1], f'''{NODE_API}/blocks/{hdr[2][0]}/transactions'''] for hdr in block_headers if hdr[1] != 0]
            blocks = await get_all(urls)

            # recreate blockchain (must put together in order)
            suffix = f'''{last_height}-{next_height} / {current_height} UNSPENT'''
            # prefix=f'''({new_tokens}/{old_tokens}) {t.split()}'''
            if PRETTYPRINT: printProgressBar(int(last_height+(2*FETCH_INTERVAL/3)), current_height, prefix=prefix, suffix=f'{suffix}{" "*(LINELEN-len(suffix))}', length=50)
            else: 
                try: percent_complete = f'{100*int(last_height+(2*FETCH_INTERVAL/3))/current_height:0.2f}%'
                except: percent_complete= 0
                logger.info(f'{percent_complete}/{t.split()} {suffix}')
            for blk, transactions in sorted([[b[1], b[2]] for b in blocks]):
                tokens = await process(transactions['transactions'], tokens, blk, is_plugin=True, args=args)

            # checkpoint
            if VERBOSE: logger.debug('Checkpointing...')
            last_height += FETCH_INTERVAL
            suffix = f'''{last_height}-{next_height} / {current_height} CHECKPOINT'''            
            # prefix=f'''({new_tokens}/{old_tokens}) {t.split()}'''
            if PRETTYPRINT: printProgressBar(next_height, current_height, prefix=prefix, suffix=f'{suffix}{" "*(LINELEN-len(suffix))}', length=50)
            else: 
                try: percent_complete = f'{100*next_height/current_height:0.2f}%'
                except: percent_complete= 0
                logger.warning(f'{percent_complete}/{t.split()} {suffix}')
            
            if len(tokens) > 0: 
                stats = await refresh(next_height, tokens)
                new_tokens += stats['new']
                old_tokens += stats['existing']

            if ((next_height-1) % 100000) == 0:
                logger.info(f'[[ height: {next_height-1} | new: {new_tokens} | old: {old_tokens} ]]')

            tokens = {}

        sec = t.stop()
        logger.debug(f'main.app:: Token refresh took {sec:0.4f}s...')

        fin = {
            'current_height' : current_height,
            'new_tokens' : new_tokens,
            'old_tokens' : old_tokens,
            'total_tokens' : new_tokens+old_tokens,
            'status': 'success',
            'message': f'update took {sec:0.4f}s...',
        }

        logger.info(fin)

    except KeyboardInterrupt:
        logger.error('Interrupted.')
        try: sys.exit(0)
        except SystemExit: os._exit(0)

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')
        fin = {
            'current_height' : current_height,
            'new_tokens' : new_tokens,
            'old_tokens' : old_tokens,
            'total_tokens' : new_tokens+old_tokens,
            'status': 'error',
            'message': e
        }

    return fin

if __name__ == '__main__':    
    args = cli()
    res = asyncio.run(main(args))
#endregion MAIN