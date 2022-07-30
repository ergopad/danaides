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
