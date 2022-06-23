import asyncio
import os, sys, time, signal
import pandas as pd
import argparse
 
from utils.db import eng, text
from utils.logger import logger, myself, Timer, printProgressBar
from plugins import staking
from requests import get
from os import getenv
from base58 import b58encode

# from pydantic import BaseModel

parser = argparse.ArgumentParser()
parser.add_argument("-T", "--truncate", help="Truncate boxes table", action='store_true')
parser.add_argument("-H", "--height", help="Begin at this height", type=int, default=-1)
parser.add_argument("-P", "--prettyprint", help="Begin at this height", action='store_true')
args = parser.parse_args()

PRETTYPRINT = args.prettyprint
VERBOSE = False

NODE_APIKEY = getenv('ERGOPAD_APIKEY')
NODE_URL = f'''http://{getenv('NODE_URL')}:{getenv('NODE_PORT')}'''
NERGS2ERGS = 10**9
UPDATE_INTERVAL = 1000 # update progress display every X blocks
CHECKPOINT_INTERVAL = 10000 # save progress every X blocks

headers = {'Content-Type': 'application/json', 'api_key': NODE_APIKEY}
blips = []

def b58(n): 
    return b58encode(bytes.fromhex(n)).decode('utf-8')

def get_node_info():
    res = get(f'{NODE_URL}/info', headers=headers, timeout=2)
    node_info = None
    if not res.ok:
        logger.error(f'unable to retrieve node info: {res.text}')
        exit()
    else:
        node_info = res.json()
        if VERBOSE: logger.debug(node_info)
    
    # return 10000 # testing
    return node_info

if __name__ == '__main__':
    node_info = get_node_info()

    # find newly unspent boxes
    sql = f'''
        select distinct b.box_id
        from boxes b
        where is_unspent = true
            and nerg is null
    '''
    msg = ''

    logger.info(f'Fetching all unspent boxes{msg}...')
    boxes = eng.execute(sql).fetchall()
    box_count = len(boxes)

    nergs = {}
    box_counter = 0
    for box in boxes:
        box_id = box['box_id']
        box_counter += 1
        suffix = f'''{box_counter}/{box_count} {box_id}'''
        if PRETTYPRINT: printProgressBar(box_counter, box_count, prefix='Progress:', suffix=suffix, length=50)

        with get(f'''{NODE_URL}/utxo/byId/{box_id}''', headers=headers, timeout=2) as res:
            if res.ok:
                nerg = res.json()['value']
                nergs[box_id] = nerg
                if VERBOSE: logger.debug(nerg)

        # update progress bar on screen
        if box_counter%UPDATE_INTERVAL == 0:
            suffix = f'''{box_counter}/{box_count} {box_id}'''
            if PRETTYPRINT: printProgressBar(box_counter, box_count, prefix='Progress:', suffix=suffix, length=50)
            else: logger.info(suffix)                

        # save current unspent to sql
        if (box_counter%CHECKPOINT_INTERVAL == 0) or (box_counter == box_count):
            suffix = f'''Checkpoint at {box_counter}'''+(' '*80)
            if PRETTYPRINT: printProgressBar(box_counter, box_count, prefix='Progress:', suffix=suffix, length=50)
            with eng.begin() as con:
                for box_id, nerg in nergs.items():
                    sql = text(f'''
                        update boxes set nerg = :nerg 
                        where box_id = :box_id
                    ''')
                    con.execute(sql, {'nerg': nerg, 'box_id': box_id})
            nergs = {}
