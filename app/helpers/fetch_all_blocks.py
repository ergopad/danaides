import asyncio
import os, sys, time, signal
import pandas as pd
import argparse
import asyncio
import json
import time
from typing import Dict, Any, List, Tuple
import requests
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from itertools import repeat
from aiohttp import ClientSession

from utils.db import eng, text
from utils.logger import logger, myself, Timer, printProgressBar
from plugins import staking
from requests import get
from os import getenv
from base58 import b58encode

parser = argparse.ArgumentParser()
parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
parser.add_argument("-P", "--prettyprint", help="Progress bar vs. scrolling", action='store_true')
args = parser.parse_args()

PRETTYPRINT = args.prettyprint
VERBOSE = False

NODE_APIKEY = getenv('ERGOPAD_APIKEY')
NODE_URL = f'''http://{getenv('NODE_URL')}:{getenv('NODE_PORT')}'''
NERGS2ERGS = 10**9
UPDATE_INTERVAL = 100 # update progress display every X blocks
CHECKPOINT_INTERVAL = 1000 # save progress every X blocks
PROGRESS_COUNTER = 0
PROGRESS_INTERVAL = 10
TOTAL_BOXES = 0

headers = {'Content-Type': 'application/json', 'api_key': NODE_APIKEY}
blips = []

async def http_get_with_aiohttp(session: ClientSession, block: str, headers: Dict = {}, proxy: str = None, timeout: int = 10) -> (int, Dict[str, Any], bytes):
    url = f'{NODE_URL}/blocks/at/{block}'
    try:
        res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
        block_headers = await res.json(content_type=None)
        inputs = []
        outputs = []
        for hdr in block_headers:
            url = f'{NODE_URL}/blocks/{hdr}/transactions'
            res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
            block_transactions = await res.json(content_type=None)
            for tx in block_transactions['transactions']:
                inputs.append(tx['inputs'])
                outputs.append(tx['outputs'])
        return block, inputs, outputs      

    except Exception as e:
        logger.error(f'ERR: fetching block, {block}; {e}')
        pass
    block_transactions = res.json()['transactions']
    unspent = {}
    for tx in block_transactions:
        unspent.appent(tx['inputs'], tx['outputs'])
    return unspent

async def http_get_with_aiohttp_parallel(session: ClientSession, list_of_urls: List[str], headers: Dict = {}, proxy: str = None, timeout: int = 10) -> (List[Tuple[int, Dict[str, Any], bytes]], float):
    res = await asyncio.gather(*[http_get_with_aiohttp(session, url, headers, proxy, timeout) for url in list_of_urls])
    return res

async def aioreq(urls, headers):
    session = ClientSession()
    res = await http_get_with_aiohttp_parallel(session, urls, headers)
    await session.close()
    return res

# remove all inputs from current block
async def del_inputs(inputs: dict, unspent: dict, height: int = -1) -> dict:
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

# add all outputs from current block
async def add_outputs(outputs: dict, unspent: dict, height: int = -1) -> dict:
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

# if PRETTYPRINT: printProgressBar(spent_counter, len(boxes), prefix='Progress:', suffix=box_id, length=50)

# if res.status_code == 404: spent.append(box_id)
async def main():
    t = Timer()
    t.start()

    # batch node requests
    current_height = 779622
    height_counter = 0
    fetch_interval = 5000
    transactions = {}
    while height_counter < current_height:
        printProgressBar(height_counter, current_height, prefix='Progress:', suffix=f'{height_counter}+ {t.split()}', length=50)
        end_counter = height_counter+fetch_interval
        if end_counter >= current_height:
            end_counter = current_height+1
        blocks = range(height_counter, end_counter)
        results = await aioreq(blocks, headers)
        for r in results:            
            transactions[r[0]] = {'inputs': r[1], 'outputs': r[2]}
        height_counter += fetch_interval

    sec = t.stop()
    logger.debug(f'Took {sec:0.4f}s; saving to file...')

    printProgressBar(height_counter, current_height, prefix='Progress:', suffix=f'Sorting...     ', length=50)    
    unspent = {}
    # sort in order
    res = get(f'{NODE_URL}/utxo/genesis', headers=headers, timeout=2)
    if not res.ok:
        logger.error(f'unable to determine genesis blocks {res.text}')

    # init unspent blocks
    genesis_blocks = res.json()
    for gen in genesis_blocks:
        box_id = gen['boxId']
        # unspent[box_id] = True # height 0

    for height in range(1, current_height+1):
        tx = transactions[height]
        unspent = await del_inputs(tx['inputs'][0], unspent)
        unspent = await add_outputs(tx['outputs'][0], unspent, height)

    df = pd.DataFrame.from_dict({
        'box_id': list(unspent.keys()), 
        'height': [n['height'] for n in unspent.values()], # list(map(int, unspent.values())), # 'nergs': list(map(int, [v[0] for v in unspent.values()])), 
        'nerg': [n['nergs'] for n in unspent.values()],
        'is_unspent': [n['height']!=-1 for n in unspent.values()], # [b!=-1 for b in list(unspent.values())]
    })
    # logger.info(df)
    df.to_sql(f'checkpoint_testing', eng, if_exists='replace')


    logger.debug(f'unspent: {len(unspent)}')

if __name__ == '__main__':
    asyncio.run(main())