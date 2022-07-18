import asyncio
import argparse

from utils.logger import logger, Timer, printProgressBar
from utils.db import eng
from utils.ergodex import getErgodexPoolBox, parseValidPools

#region INIT
PRETTYPRINT = False
VERBOSE = False # args.verbose
NERGS2ERGS = 10**9
POOL_SAMPLE = "1999030f0400040204020404040405feffffffffffffffff0105feffffffffffffffff01050004d00f040004000406050005000580dac409d819d601b2a5730000d602e4c6a70404d603db63087201d604db6308a7d605b27203730100d606b27204730200d607b27203730300d608b27204730400d6099973058c720602d60a999973068c7205027209d60bc17201d60cc1a7d60d99720b720cd60e91720d7307d60f8c720802d6107e720f06d6117e720d06d612998c720702720fd6137e720c06d6147308d6157e721206d6167e720a06d6177e720906d6189c72117217d6199c72157217d1ededededededed93c27201c2a793e4c672010404720293b27203730900b27204730a00938c7205018c720601938c7207018c72080193b17203730b9593720a730c95720e929c9c721072117e7202069c7ef07212069a9c72137e7214067e9c720d7e72020506929c9c721372157e7202069c7ef0720d069a9c72107e7214067e9c72127e7202050695ed720e917212730d907216a19d721872139d72197210ed9272189c721672139272199c7216721091720b730e"
#endregion INIT

#region FUNCTIONS 

async def process() -> int:
    try:
        t = Timer()
        t.start()

        # ergodex tokens
        boxes = getErgodexPoolBox() # boxes = list(map(explorerToErgoBox, res["items"]))        
        if VERBOSE: logger.debug(f'{len(boxes)} boxes')
        pools = parseValidPools(boxes) # print('\n'.join([f'''{b}, {pools[b].y.asset.name}''' for b in range(12)]))        
        if VERBOSE: logger.debug(f'{len(pools)} pools')
        prices = [pool.getCalculatedPrice() for pool in pools]
        if VERBOSE: logger.debug(f'{len(prices)} prices')
        SigUSD_ERG = [(p['amountY']*(10**p['decimalX']))/(p['amountX']*(10**p['decimalY'])) for p in prices if p['assetY'].lower() == 'sigusd']
        if len(SigUSD_ERG) > 0:
            SigUSD_ERG = SigUSD_ERG[0]
        else:
            logger.warning(f'''Unable to find SigUSD_ERG in prices list {[p['assetY'] for p in prices]}''')
        if VERBOSE: logger.debug(f'''sigusd: {SigUSD_ERG}''')

        # for token in tokens:
        for p in prices:
            if VERBOSE: logger.debug(p)
            token_id = p['assetYId']
            token_name = p['assetY']
            decimals = p['decimalY']
            token_ERG = (p['amountY']*(10**p['decimalX']))/(p['amountX']*(10**p['decimalY']))
            price = SigUSD_ERG / token_ERG
            
            try:
                logger.debug(f'Token price {price} for {token_name} ({token_id})...')
                
                # update tokens table with price
                if price > 0:
                    with eng.begin() as con:
                        sql = f'''
                            select id 
                            from tokens
                            where token_id = '{token_id}'
                        '''
                        if VERBOSE: logger.warning(sql)
                        res = con.execute(sql).fetchone()
                        id = -1
                        if res is not None:
                            id = res['id']
                            logger.warning(f'Found id {id} for token {token_id}...')
                        else:
                            logger.warning(f'Token does not exist {token_id}...')
                        
                        # upsert
                        if id > 0:
                            sql = f'''
                                update tokens set token_price = {price:0.10f}
                                where id = {id}
                            '''                            
                        else:
                            sql = f'''
                                insert into tokens (token_id, token_name, decimals, token_price)
                                values ('{token_id}', '{token_name}', {decimals}, {price:0.10f})
                            '''
                        if VERBOSE: logger.warning(sql)
                        con.execute(sql)                        

            except Exception as e:
                logger.error(f'ERR: {e}')
                pass

        sec = t.stop()
        logger.debug(f'Processing complete: {sec:0.4f}s...')

    except Exception as e:
        logger.error(f'ERR: Process {e}')
        pass

#endregion FUNCTIONS

#region MAIN

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-P", "--prettyprint", help="Begin at this height", action='store_true')
    args = parser.parse_args()
    
    PRETTYPRINT = args.prettyprint
    
    res = asyncio.run(process())

#endregion MAIN
