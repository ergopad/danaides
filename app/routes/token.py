from fastapi import APIRouter, Depends, HTTPException, status
from utils.logger import logger, myself, Timer, LEIF
from utils.db import eng
from pydantic import BaseModel
from typing import List
from sqlalchemy import text
from decimal import Decimal

# mint
# burn
# get by id

token_router = r = APIRouter()

class Token(BaseModel):
    id: str
    name: str = ''
    decimals: int = 0
    amount: int = 0

# comments show paideia values for reference
class TokenInventoryDAO(BaseModel):
    addresses: List[str]
    token_id: str = '1fd6e032e8476c4aa54c18c1a308dce83940e8f4a28f576440513ed7326ad489'
    stake_tree: str = '101f040004000e2012bbef36eaa5e61b64d519196a1e8ebea360f18aba9b02d2a21b16f26208960f040204000400040001000e20b682ad9e8c56c5a0ba7fe2d3d9b2fbd40af989e8870628f4a03ae1022d36f0910402040004000402040204000400050204020402040604000100040404020402010001010100040201000100d807d601b2a4730000d6028cb2db6308720173010001d6039372027302d604e4c6a70411d605e4c6a7050ed60695ef7203ed93c5b2a4730300c5a78fb2e4c6b2a57304000411730500b2e4c6720104117306007307d6079372027308d1ecec957203d80ad608b2a5dc0c1aa402a7730900d609e4c672080411d60adb63087208d60bb2720a730a00d60cdb6308a7d60db2720c730b00d60eb2720a730c00d60fb2720c730d00d6107e8c720f0206d611e4c6720104119683090193c17208c1a793c27208c2a793b27209730e009ab27204730f00731093e4c67208050e720593b27209731100b27204731200938c720b018c720d01938c720b028c720d02938c720e018c720f01937e8c720e02069a72109d9c7eb272117313000672107eb27211731400067315957206d801d608b2a5731600ed72079593c27208c2a7d801d609c67208050e95e67209ed93e472097205938cb2db6308b2a57317007318000172057319731a731b9595efec7206720393c5b2a4731c00c5a7731d7207731e'
    vest_tree: str # 
    proxy_address: str = '245957934c20285ada547aa8f2c8e6f7637be86a1985b3e4c36e4e1ad8ce97ab'

@r.post("/locked/")
async def locked(tid: TokenInventoryDAO):
    
    # TODO: validate address
    addresses = "'"+("','".join(tid.addresses))+"'"

    # find free/staked tokens
    sql = text(f'''
		with fre as (
			select sum(amount) as amount, address 
			from token_free 
			group by address
		)
		select max(coalesce(fre.amount, 0)) as individual_free
            , sum(coalesce(stk.amount, 0)) as individual_staked
            , 0 as individual_vested -- hack for now
            , coalesce(fre.address, stk.address) as address
        from fre
        full outer join token_locked stk on stk.address = fre.address
		where coalesce(fre.address, stk.address) in ({addresses})
		group by coalesce(fre.address, stk.address)
    ''')
    with eng.begin() as con:
        res = con.execute(sql).fetchall()

    # make sure all addresses exist in final set
    individual_free = {}
    individual_staked = {}
    individual_vested = {}
    for adr in tid.addresses:
        individual_free[adr] = Decimal('0')
        individual_staked[adr] = Decimal('0')
        individual_vested[adr] = Decimal('0')

    # add any that have qty
    totals = {
        'grand': Decimal('0'),
        'free': Decimal('0'),
        'staked': Decimal('0'),
        'vested': Decimal('0'),
    }
    for row in res:
        individual_free[row['address']] = row['individual_free']
        individual_staked[row['address']] = row['individual_staked']
        individual_vested[row['address']] = row['individual_vested']
        totals['free'] += Decimal(row['individual_free'])
        totals['staked'] += Decimal(row['individual_staked'])
        totals['vested'] += Decimal(row['individual_vested'])
        totals['grand'] += Decimal(row['individual_free']) + Decimal(row['individual_staked']) + Decimal(row['individual_vested'])
    
    return {
        'totalTokens': totals['grand'],
        'totalFree': totals['free'],
        'totalStaked': totals['staked'],
        'totalVested': totals['vested'],

        'free': individual_free,        
        'staked': individual_staked,        
        'vested': individual_vested,
    }

@r.post("/burn/")
async def burn(token: Token):
    # check is valid token
    logger.debug(f'burning token: {token.id}')

    token_id = token.id
    token_name = 'xyzpad'
    burn_address = ''
    wallet_address = ''
    value = 1
    fee = .01
    decimals = 2
    burn_amount = 100
    emission = 1000

    tx = {
        "inputs": [
            {
                "boxId":"f74ce7a954d63e4ca4089db2b21377f91a5206567fa482acf58c1600550bd7af",
                "transactionId":"4af200f4da9eab7b33beca14dd8e4d4f21fd06b57f182ccdc5f7d499271fc327",
                "index":2,
                "ergoTree":"0008cd029bb1317f5fa5678961b88f7a12c87ffd10b9a132d962c697acd0698d0b0c75fc",
                "creationHeight":765238,
                "value":"4640094783",
                "assets":[
                    {"tokenId":"8b70d5e59232d6437a74afec8d0eca20c00abaccd2518dbccf0ecde918b8831a","amount":"999000000"},
                    {"tokenId":"42ad11164cb217a7ed5327c4c235a49e801655486220c33de5ae7460f49dbfd1","amount":"1"},
                    {"tokenId":"3e6df7dbf3c48748ae74b94cbee4b2e3ec5c4522138e0a1898e8edbe0bfb5ddb","amount":"1"},
                    {"tokenId":"03faf2cb329f2e90d6d23b58d91bbb6c046aa143261cc21f52fbe2824bfcbf04","amount":"2500"},
                    {"tokenId":"001475b06ed4d2a2fe1e244c951b4c70d924b933b9ee05227f2f2da7d6f46fd3","amount":"489900000"},
                    {"tokenId":"0f034551879db5880d227c855fc533d6fc8740dcc9670846fa1818bd80c8c727","amount":"208219"}
                ],
                "additionalRegisters":{},
                "confirmed":True,
                "extension":{}
            }
        ],
        "dataInputs":[],
        "outputs": [
            {
                "value":"2000000",
                "ergoTree":"0008cd029bb1317f5fa5678961b88f7a12c87ffd10b9a132d962c697acd0698d0b0c75fc",
                "assets":[],
                "additionalRegisters":{},
                "creationHeight":805392
            },
            {
                "value":"1000000",
                "ergoTree":"0008cd0362f2d59008815649038ea9f2bc0550150177eb88bcae8be5d95592bec2d8ce99",
                "assets":[],
                "additionalRegisters":{},
                "creationHeight":805392
            },
            {
                "value":"4635994783",
                "ergoTree":"0008cd029bb1317f5fa5678961b88f7a12c87ffd10b9a132d962c697acd0698d0b0c75fc",
                "assets":[
                    {"tokenId":"0f034551879db5880d227c855fc533d6fc8740dcc9670846fa1818bd80c8c727","amount":"208219"},
                    {"tokenId":"3e6df7dbf3c48748ae74b94cbee4b2e3ec5c4522138e0a1898e8edbe0bfb5ddb","amount":"1"},
                    {"tokenId":"42ad11164cb217a7ed5327c4c235a49e801655486220c33de5ae7460f49dbfd1","amount":"1"},
                    {"tokenId":"001475b06ed4d2a2fe1e244c951b4c70d924b933b9ee05227f2f2da7d6f46fd3","amount":"489800000"},
                    {"tokenId":"8b70d5e59232d6437a74afec8d0eca20c00abaccd2518dbccf0ecde918b8831a","amount":"999000000"},
                    {"tokenId":"03faf2cb329f2e90d6d23b58d91bbb6c046aa143261cc21f52fbe2824bfcbf04","amount":"2500"}
                ],
                "additionalRegisters":{},
                "creationHeight":805392
            },
            {
                "value":"1100000",
                "ergoTree":"1005040004000e36100204a00b08cd0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798ea02d192a39a8cc7a701730073011001020402d19683030193a38cc7b2a57300000193c2b2a57301007473027303830108cdeeac93b1a57304",
                "assets":[],
                "additionalRegisters":{},
                "creationHeight":805392
            }
        ]
    }

    # build tx
    tx = {
        'hello': 'world'
    }
    logger.debug(f'transaction: {tx}')

    # try to sign/submit
    return {"tx": tx}

@r.post("/mint/")
async def mint(token: Token):
    return {}

@r.get("/info/")
async def mint(token: Token):
    return {}
