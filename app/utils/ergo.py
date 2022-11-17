from requests import get
from base58 import b58encode
from os import getenv
from utils.logger import logger
from time import sleep

NODE_API = f'''http://{getenv('NODE_URL')}:{getenv('NODE_PORT')}'''
NERGS2ERGS = 10**9
headers = {'Content-Type': 'application/json'}

class Network():
  Mainnet = 0 << 4
  Testnet = 1 << 4

class AddressKind():
  P2PK = 1
  P2SH = 2
  P2S = 3

def b58(n): 
    return b58encode(bytes.fromhex(n)).decode('utf-8')

# Attempt to get basic node info.
# Wait until node responds reasonably, since nothing will work if the connection is gone
def get_node_info():
    i:int = 0
    while i >= 0:
        try:
            res = get(f'{NODE_API}/info', headers=headers, timeout=2)
            if res.ok:
                node_info = res.json()
                return node_info
        
        # Check every second for node; update every minute, if issues
        except Exception as e:
            pass

        if i%60 == 0:
            logger.warning(f'Waiting on node connection [{NODE_API}/info]...')
        i += 1
        sleep(1)

def get_genesis_block():
    try:
        genesis_block = get(f'{NODE_API}/utxo/genesis', headers=headers, timeout=2)
        if not genesis_block.ok:
            raise ValueError(f'ergo.get_genesis_block; unable to determine genesis blocks {genesis_block.text}')

        return genesis_block
    
    except:
        return {}
