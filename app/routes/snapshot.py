from fastapi import APIRouter, Depends, HTTPException, status
from utils.logger import logger, myself, Timer, LEIF
from utils.db import eng
from pydantic import BaseModel
from sqlalchemy import text

# get by token id

snapshot_router = r = APIRouter()

class Token(BaseModel):
    id: str
    name: str = ''
    decimals: int = 0
    amount: int = 0

@r.post("/byTokenId/")
async def snapshot(token: Token):
    try:
        token_id = token.id
        logger.debug(f'token id: {token_id}')
        if token_id is None:
            token_id = 'd71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413'
        sql = text(f'''
            select token_id, address, sum(staking.amount) as amount
            from staking
            where token_id = :token_id
            group by token_id, address;
        ''')
        # logger.debug(sql)
        with eng.begin() as con:
            res = con.execute(sql, {'token_id': token_id}).fetchall()

        snp = {}
        for r in res:
            snp[r['address']] = r['amount']

        return {
            'token_id': token_id,
            'snapshot': snp
        }

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')

    return {}