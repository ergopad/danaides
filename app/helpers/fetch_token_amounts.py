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
    found = {}
    url = f'''{NODE_URL}/utxo/byId/{box_id}'''
    try:
        res = await session.get(url=url, headers=headers, proxy=proxy, timeout=timeout)
    except Exception as e:
        logger.error(f'ERR: fetching box, {box_id}; {e}')
        pass
    if res.status == 200:
        try: 
            utxo = await res.json(content_type=None)
            
            # found for in circulation calc
            if utxo['ergoTree'] in TOKEN_AMOUNTS.keys():
                for asset in utxo['assets']:
                    if asset['tokenId'] == TOKEN_AMOUNTS[utxo['ergoTree']]:
                        found[utxo['ergoTree']] = asset['amount']
            
            # found ergopad
            if TOKEN_AMOUNTS['_ergopad'] in [a['tokenId'] for a in utxo['assets']]:
                for asset in utxo['assets']:
                    if asset['tokenId'] == TOKEN_AMOUNTS['_ergopad']:
                        found['_ergopad'] = asset['amount']

        except json.decoder.JSONDecodeError as e: 
            pass

        except Exception as e: 
            logger.error(f'{e}')
            pass

    return found

async def http_get_with_aiohttp_parallel(session: ClientSession, list_of_urls: List[str], headers: Dict = {}, proxy: str = None, timeout: int = 10) -> (List[Tuple[int, Dict[str, Any], bytes]], float):
    res = await asyncio.gather(*[http_get_with_aiohttp(session, url, headers, proxy, timeout) for url in list_of_urls])
    return res

async def main(urls, headers):
    session = ClientSession()
    res = await http_get_with_aiohttp_parallel(session, urls, headers)
    await session.close()
    return res

if __name__ == '__main__':
    # find max height of current unspent boxes
    sql = f'select max(height) as max_height from boxes'
    logger.debug('Find max height...')
    max_height = eng.execute(sql).fetchone()['max_height']

    # find the address/token combos to evaluate
    # TODO: need a config table and a table to store values by box
    sql = f'''
        select ergo_tree, token_id
        from token_agg
    '''
    logger.debug('Find config...')
    config = eng.execute(sql).fetchall()
    TOKEN_AMOUNTS = {}
    for cfg in config:
        TOKEN_AMOUNTS[cfg['ergo_tree']] = cfg['token_id']

    pred = f'where height > 700000'
    sql = f'''
        select box_id
        from boxes
        {pred}
        order by height
        -- limit 100000
    '''
    logger.debug('Find boxes...')
    boxes = eng.execute(sql).fetchall()

    logger.info('Begin')
    box_counter = 0
    total_boxes = len(boxes)
    fetch_interval = 1000
    t = Timer()
    t.start()

    # batch node requests
    token_amounts = {}
    for ergo_tree in TOKEN_AMOUNTS.keys():
        token_amounts[ergo_tree] = 0
    while box_counter < total_boxes:
        printProgressBar(box_counter, total_boxes, prefix='Progress:', suffix=f'{box_counter}+ {t.split()}', length=50)
        urls = [box['box_id'] for box in boxes[box_counter:box_counter+fetch_interval]]
        results = asyncio.run(main(urls, headers))
        for r in results:
            if len(r) > 0:
                # logger.debug(r)
                for ergo_tree, amount in r.items():
                    token_amounts[ergo_tree] += amount
                    
        box_counter += fetch_interval

    logger.debug('Save to SQL...')
    # logger.debug(token_amounts['_ergopad']); exit(1)    

    # cleanup over 100k blocks old
    # TODO: need to save by box_id/height
    #try:
    #    sql = f'delete from token_agg where height between 1 and {max_height}-100000'
    #    eng.execute(sql)
    #except Exception as e:
    #    logger.error(e)
    #    pass

    # store current values
    for ergo_tree, amount in token_amounts.items():
        if amount > 0:
            sql = f'''
                update token_agg set amount = {amount}
                where ergo_tree = '{ergo_tree}'
            '''
            eng.execute(sql)

    sec = t.stop()
    logger.debug(f'Took {sec:0.4f}s...')
