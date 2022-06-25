import asyncio
import os, sys, time, signal
import pandas as pd
import argparse
import asyncio
import json
import time
import requests

from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from itertools import repeat
from aiohttp import ClientSession
from utils.db import eng, text
from utils.logger import logger, myself, Timer, printProgressBar
from plugins import staking
from requests import get
from os import getenv
from base58 import b58encode
from ergo_python_appkit.appkit import ErgoValue

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

async def http_get_with_aiohttp(session: ClientSession, box_id: str, headers: Dict = {}, proxy: str = None, timeout: int = 10) -> (int, Dict[str, Any], bytes):
    url = f'''{NODE_URL}/utxo/byId/{box_id}'''
    try:
        res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
    except Exception as e:
        logger.error(f'ERR: fetching box, {box}; {e}')
        pass
    response_json = None
    try: response_json = await res.json(content_type=None)
    except json.decoder.JSONDecodeError as e: pass

    try:
        keys_found = {}
        utxo = response_json
        address = utxo['ergoTree']
        assets = utxo['assets']
        nergs = utxo['value']
        raw = address[6:]
        if address in STAKE_KEYS:   
            stake_token_id = STAKE_KEYS[address]['stake_token_id']
            # found ergopad staking key
            if assets[0]['tokenId'] == stake_token_id:
                # stakekey_counter += 1
                R4_1 = ErgoValue.fromHex(utxo['additionalRegisters']['R4']).getValue().apply(1)
                keys_found[box_id] = {
                    'stakekey_token_id': utxo['additionalRegisters']['R5'][4:], # TODO: validate that this starts with 0e20 ??
                    'amount': assets[1]['amount'],
                    'token_id': stake_token_id,
                    'penalty': int(R4_1),
                    'address': raw
                }                
    except Exception as e: 
        logger.error(e)
        pass

    return keys_found

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
    sql = f'''
        select box_id 
        from boxes
        order by height desc
        limit 1000
    '''
    logger.debug('Find boxes...')
    boxes = eng.execute(sql).fetchall()

    sql = '''
        select stake_ergotree, stake_token_id, token_name, token_id, token_type, emission_amount, decimals 
        from tokens
    '''
    STAKE_KEYS = {}
    res = eng.execute(sql).fetchall()
    for key in res:
        STAKE_KEYS[key['stake_ergotree']] = {
            'stake_token_id': key['stake_token_id'],
            'token_name': key['token_name'],
            'token_type': key['token_type'],
            'emission_amount': key['emission_amount'],
            'decimals': key['decimals'],
        }
    
    keys_found = {}
    addresses = {}

    logger.debug('Find spent...')
    box_counter = 0
    total_boxes = len(boxes)
    fetch_interval = 1000
    t = Timer()
    t.start()

    # batch node requests
    utxos = {} 
    while box_counter < total_boxes:
        printProgressBar(box_counter, total_boxes, prefix='Progress:', suffix=f'{box_counter}+ {t.split()}', length=50)
        urls = [box['box_id'] for box in boxes[box_counter:box_counter+fetch_interval]]
        results = asyncio.run(main(urls, headers))
        for r in results:
            if len(r) > 0:
                # print(r)
                for utxo, stake_keys in r.items():
                    utxos[utxo] = stake_keys
                box_counter += fetch_interval

    sec = t.stop()
    logger.debug(f'Took {sec:0.4f}s; found {len(utxos)} keys...')

    print(utxos)

