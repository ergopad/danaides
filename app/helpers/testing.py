import asyncio
import pandas as pd
import argparse

from time import sleep, time
from datetime import datetime
# from sqlalchemy import create_engine, text
from utils.logger import logger, Timer, printProgressBar
from utils.db import eng, text
from utils.ergo import get_node_info, headers, NODE_APIKEY, NODE_URL
from utils.aioreq import get_json, get_json_ordered
from requests import get
from os import getenv
from base58 import b58encode
from pydantic import BaseModel
from ergo_python_appkit.appkit import ErgoValue, ErgoAppKit

sql = f'''
    -- vesting
    with v as (
        select id 
            , ergo_tree
            , box_id
            , (each(registers)).key::varchar(64) as register
            , right((each(registers)).value::text, length((each(registers)).value::text)-4) as token_id
            , (each(registers)).value::text as parameter
        from utxos
        where ergo_tree in (
                '100e04020400040404000402040604000402040204000400040404000400d810d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605e4c6a70411d6069d99db6903db6503feb27205730200b27205730300d607b27205730400d608b27205730500d6099972087204d60a9592720672079972087209999d9c7206720872077209d60b937204720ad60c95720bb2a5730600b2a5730700d60ddb6308720cd60eb2720d730800d60f8c720301d610b2a5730900d1eded96830201aedb63087201d901114d0e938c721101720293c5b2a4730a00c5a79683050193c2720cc2720193b1720d730b938cb2720d730c00017202938c720e01720f938c720e02720aec720bd801d611b2db63087210730d009683060193c17210c1a793c27210c2a7938c721101720f938c721102997204720a93e4c67210050e720293e4c6721004117205',
                '1012040204000404040004020406040c0408040a050004000402040204000400040404000400d812d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605db6903db6503fed606e4c6a70411d6079d997205b27206730200b27206730300d608b27206730400d609b27206730500d60a9972097204d60b95917205b272067306009d9c7209b27206730700b272067308007309d60c959272077208997209720a999a9d9c7207997209720b7208720b720ad60d937204720cd60e95720db2a5730a00b2a5730b00d60fdb6308720ed610b2720f730c00d6118c720301d612b2a5730d00d1eded96830201aedb63087201d901134d0e938c721301720293c5b2a4730e00c5a79683050193c2720ec2720193b1720f730f938cb2720f731000017202938c7210017211938c721002720cec720dd801d613b2db630872127311009683060193c17212c1a793c27212c2a7938c7213017211938c721302997204720c93e4c67212050e720293e4c6721204117206'
            )
    )
    , amounts as (
        select id, box_id, parameter, ergo_tree
        from v
        where register = 'R4'
    )
    , tokens as (
        select id, box_id, token_id
        from v
        where register = 'R5'
    )
    select a.address, t.token_id, q.parameter, q.ergo_tree, a.amount, q.box_id
    from tokens t
        join amounts q on q.id = t.id
        join assets a on a.token_id = t.token_id
	where address in ('9eXGhU2T4SatNVHrPNt5KRExeQ9Jz89aamvx2Q7CHjHbnY1sRzG')
'''

with eng.begin() as con:
    res = con.execute(sql).fetchall()

vested = {}
vestedTokenInfo = {}
for r in res:
    parameters = ErgoAppKit.deserializeLongArray(r['parameter'])
    blockTime = int(time()*1000)
    
    redeemPeriod        = parameters[0]
    numberOfPeriods     = parameters[1]
    vestingStart        = parameters[2]
    totalVested         = parameters[3]

    timeVested          = blockTime - vestingStart
    periods             = max(0,int(timeVested/redeemPeriod))
    redeemed            = totalVested - int(r['amount'])
    
    if r['ergo_tree'] == "1012040204000404040004020406040c0408040a050004000402040204000400040404000400d812d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605db6903db6503fed606e4c6a70411d6079d997205b27206730200b27206730300d608b27206730400d609b27206730500d60a9972097204d60b95917205b272067306009d9c7209b27206730700b272067308007309d60c959272077208997209720a999a9d9c7207997209720b7208720b720ad60d937204720cd60e95720db2a5730a00b2a5730b00d60fdb6308720ed610b2720f730c00d6118c720301d612b2a5730d00d1eded96830201aedb63087201d901134d0e938c721301720293c5b2a4730e00c5a79683050193c2720ec2720193b1720f730f938cb2720f731000017202938c7210017211938c721002720cec720dd801d613b2db630872127311009683060193c17212c1a793c27212c2a7938c7213017211938c721302997204720c93e4c67212050e720293e4c6721204117206":
        tgeNum              = parameters[4]
        tgeDenom            = parameters[5]
        tgeTime             = parameters[6]
        tgeAmount           = int(totalVested * tgeNum / tgeDenom) if (blockTime > tgeTime) else 0
        totalRedeemable     = int(periods * (totalVested-tgeAmount) / numberOfPeriods) + tgeAmount
    else:
        totalRedeemable     = int(periods * totalVested / numberOfPeriods)

    redeemableTokens    = totalVested - redeemed if (periods >= numberOfPeriods) else totalRedeemable - redeemed
    
    vestedTokenInfo = {'name': 'hello', 'decimals': 2} # get_token_info(r['token_id'])
    if vestedTokenInfo['name'] not in vested:
        vested[vestedTokenInfo['name']] = []

    vested[vestedTokenInfo['name']].append({
        'boxId': r['box_id'],
        'Remaining': round(r['amount']*10**(-1*vestedTokenInfo["decimals"]),vestedTokenInfo["decimals"]),
        'Redeemable': round(redeemableTokens*10**(-1*vestedTokenInfo["decimals"]),vestedTokenInfo["decimals"]),
        'Vesting Key Id': r['token_id'],
        'Next unlock': datetime.fromtimestamp((vestingStart+((periods+1)*redeemPeriod))/1000)
    })

logger.debug(vested)
