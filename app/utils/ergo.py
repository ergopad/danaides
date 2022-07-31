from requests import get
from base58 import b58encode
from os import getenv

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

def get_node_info():
    res = get(f'{NODE_API}/info', headers=headers, timeout=2)
    node_info = None
    if not res.ok:
        raise ValueError(f'ergo.get_node_info; unable to retrieve node info: {res.text}')
        exit()
    else:
        node_info = res.json()
    
    # return 10000 # testing
    return node_info

def get_genesis_block():
    genesis_block = get(f'{NODE_API}/utxo/genesis', headers=headers, timeout=2)
    if not genesis_block.ok:
        raise ValueError(f'ergo.get_genesis_block; unable to determine genesis blocks {genesis_block.text}')

    return genesis_block