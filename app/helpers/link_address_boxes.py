import asyncio
import os, sys, time, signal
import pandas as pd
import argparse
 
from utils.db import eng, text
from utils.logger import logger, myself, Timer, printProgressBar
from plugins import staking
from requests import get
from os import getenv
from base58 import b58encode, b58decode_check

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

headers = {'Content-Type': 'application/json', 'api_key': NODE_APIKEY}
blips = []

if __name__ == '__main__':
    # mainnet = '9iD7JfYYemJgVz7nTGg9gaHuWg7hBbHo2kxrrJawyz4BD1r9fLS'
    # testnet = '3iD7JfYYemJgVz7nTGg9gaHuWg7hBbHo2kxrrJawyz4BD1r9fLS'
    # ergotree = '0008cd0208e5fcdd215384545908eac6601d89658998c0a1a2baefe33fde5de3ada59703'

    prefix         = ergotree[:6] == '0008cd'
    is_mainnet     = True
    checksum_valid = True
    r2a            = get(f'''{NODE_URL}/utils/rawToAddress/{ergotree[6:]}''', headers=headers, timeout=2)
    valid_size     = len(r2a.json()['address']) == 51
    is_base58      = 

    is_valid = prefix & is_mainnet & checksum_valid & r2a.ok & valid_size

