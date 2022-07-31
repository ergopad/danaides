from fastapi import APIRouter, Depends, HTTPException, status
from utils.logger import logger, myself, Timer, LEIF
from utils.db import eng
from pydantic import BaseModel

# get by token id

dashboard_router = r = APIRouter()

class Token(BaseModel):
    id: str
    name: str = ''
    decimals: int = 0
    amount: int = 0

# vesting by address(s)
# staking by address(s)
# assets by address(s)
# balances by address(s)
# get token infor (price) ??
# prices by token id ??
# tokenomics ??

@r.post("/balances/")
async def assets(addresses):
    sql = f'''
        select address, sum(nergs)/power(10, 9) as ergs
        from balances 
        where address in ({','.join([a for a in addresses])})
        group by address
    '''
    with eng.begin() as con:
        res = con.execute(sql).fetchall
    return res

