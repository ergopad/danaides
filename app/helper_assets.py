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
FETCH_INTERVAL = 5
LINELEN = 100
PRETTYPRINT = False
VERBOSE = False
NERGS2ERGS = 10**9
TOKENS = 'tokens'
#endregion

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

# remove all inputs from current block
async def del_inputs(inputs: dict, unspent: dict, height: int=-1) -> dict:
    try:
        new = unspent
        for i in inputs:
            box_id = i['boxId']
            try:
                # new[box_id] = height
                new[box_id] = {
                    'height': height, # -1 indicates spent; see checkpoint (is_unspent in checkpoint.boxes table)
                    'nergs': 0
                }
            except Exception as e:
                #BLIPS.append({'box_id': box_id, 'height': height, 'msg': f'cant remove'})
                if VERBOSE: logger.warning(f'cant find {box_id} at height {height} while removing from unspent {e}')
        return new

    except Exception as e:
        logger.error(f'ERR: find tokens {e}')
        return {}

# add all outputs from current block
async def add_outputs(outputs: dict, unspent: dict, height: int) -> dict:
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
                #BLIPS.append({'box_id': box_id, 'height': height, 'msg': f'cant add'})
                if VERBOSE: logger.warning(f'{box_id} exists at height {height} while adding to unspent {e}')
        return new

    except Exception as e:
        logger.error(f'ERR: add outputs {e}')
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
    unspent = {}
    utxos = {}
    new_tokens = 0
    old_tokens = 0
    last_height = 776135
    current_height = 776135
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

            # find register value
            for b in blocks:
                if b[0] == 200:
                    for tx in b[2]['transactions']:
                        for o in tx['outputs']:
                            if 'R5' in o['additionalRegisters']:
                                if '0e2030360b441b33136330f7bec2b06126c5a8fdc5f389e8ed53345f9ecd5ae10cdf' in o['additionalRegisters']['R5']:
                                    logger.error(f'=========================================\n{o}=========================================\n{b}=========================================\n')
                                # if o['additionalRegisters']['R5'][:4] == '0e20': logger.warning(o)

            # checkpoint
            if VERBOSE: logger.debug('Checkpointing...')
            last_height += FETCH_INTERVAL
            suffix = f'''{last_height}-{next_height} / {current_height} CHECKPOINT'''            
            if PRETTYPRINT: printProgressBar(next_height, current_height, prefix=prefix, suffix=f'{suffix}{" "*(LINELEN-len(suffix))}', length=50)
            else: 
                try: percent_complete = f'{100*next_height/current_height:0.2f}%'
                except: percent_complete= 0
                logger.warning(f'{percent_complete}/{t.split()} {suffix}')

        sec = t.stop()
        logger.debug(f'main.app:: Token refresh took {sec:0.4f}s...')

    except KeyboardInterrupt:
        logger.error('Interrupted.')
        try: sys.exit(0)
        except SystemExit: os._exit(0)

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')

    return None

if __name__ == '__main__':    
    args = cli()
    res = asyncio.run(main(args))
#endregion MAIN