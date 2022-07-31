from fastapi import APIRouter, Depends, HTTPException, status
from utils.logger import logger, myself, Timer, LEIF
from utils.db import eng
from pydantic import BaseModel

# mint
# burn
# get by id

token_router = r = APIRouter()

class Token(BaseModel):
    id: str
    name: str = ''
    decimals: int = 0
    amount: int = 0

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
