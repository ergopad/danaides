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

async def http_get_with_aiohttp(session: ClientSession, block: str, headers: Dict = {}, proxy: str = None, timeout: int = 10) -> (int, Dict[str, Any], bytes):
    url = f'{NODE_URL}/blocks/at/{blk}'
    try:
        res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
        block_headers = res.json(content_type=None)
        inputs = []
        outputs = []
        for hdr in block_headers:
            url = f'{NODE_URL}/blocks/{hdr}/transactions'
            res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
            block_transactions = res.json(content_type=None)['transactions']
            for tx in block_transactions:
                inputs.append(tx['inputs'])
                outputs.append(tx['outputs'])
        return block, inputs, outputs      

    except Exception as e:
        logger.error(f'ERR: fetching box, {box}; {e}')
        pass
    block_transactions = res.json()['transactions']
    unspent = {}
    for tx in block_transactions:
        unspent.appent(tx['inputs'], tx['outputs'])
    return unspent

async def http_get_with_aiohttp_parallel(session: ClientSession, list_of_urls: List[str], headers: Dict = {}, proxy: str = None, timeout: int = 10) -> (List[Tuple[int, Dict[str, Any], bytes]], float):
    res = await asyncio.gather(*[http_get_with_aiohttp(session, url, headers, proxy, timeout) for url in list_of_urls])
    return res

async def main(urls, headers):
    session = ClientSession()
    res = await http_get_with_aiohttp_parallel(session, urls, headers)
    await session.close()
    return res

# if PRETTYPRINT: printProgressBar(spent_counter, len(boxes), prefix='Progress:', suffix=box_id, length=50)
# if res.status_code == 404: spent.append(box_id)
if __name__ == '__main__':
    fetch_interval = 10000
    t = Timer()
    t.start()

    # batch node requests
    current_height = 100000
    height_counter = 0
    fetch_interval = 1000
    for height in range(current_height):
        printProgressBar(height_counter, current_height, prefix='Progress:', suffix=f'{box_counter}+ {t.split()}', length=50)
        urls = [box['box_id'] for box in boxes[height_counter:height_counter+fetch_interval]]
        results = asyncio.run(main(urls, headers))
        for r in results:            
            unspent[r[0]] = {'inputs': r[1], 'outputs': r[2]}
        height_counter += fetch_interval

    sec = t.stop()
    logger.debug(f'Took {sec:0.4f}s; saving to file...')

    logger.info(f'found {len(inputs)} inputs, {len(outputs)} outputs')
