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

class dotdict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__

class Token(BaseModel):
    token_id: str = '1fd6e032e8476c4aa54c18c1a308dce83940e8f4a28f576440513ed7326ad489'
    stake_tree: str = '101f040004000e2012bbef36eaa5e61b64d519196a1e8ebea360f18aba9b02d2a21b16f26208960f040204000400040001000e20b682ad9e8c56c5a0ba7fe2d3d9b2fbd40af989e8870628f4a03ae1022d36f0910402040004000402040204000400050204020402040604000100040404020402010001010100040201000100d807d601b2a4730000d6028cb2db6308720173010001d6039372027302d604e4c6a70411d605e4c6a7050ed60695ef7203ed93c5b2a4730300c5a78fb2e4c6b2a57304000411730500b2e4c6720104117306007307d6079372027308d1ecec957203d80ad608b2a5dc0c1aa402a7730900d609e4c672080411d60adb63087208d60bb2720a730a00d60cdb6308a7d60db2720c730b00d60eb2720a730c00d60fb2720c730d00d6107e8c720f0206d611e4c6720104119683090193c17208c1a793c27208c2a793b27209730e009ab27204730f00731093e4c67208050e720593b27209731100b27204731200938c720b018c720d01938c720b028c720d02938c720e018c720f01937e8c720e02069a72109d9c7eb272117313000672107eb27211731400067315957206d801d608b2a5731600ed72079593c27208c2a7d801d609c67208050e95e67209ed93e472097205938cb2db6308b2a57317007318000172057319731a731b9595efec7206720393c5b2a4731c00c5a7731d7207731e'
    vest_tree: str # 
    proxy_address: str = '245957934c20285ada547aa8f2c8e6f7637be86a1985b3e4c36e4e1ad8ce97ab'

# comments show paideia values for reference
class TokenInventoryDAO(BaseModel):
    addresses: List[str]
    tokens: List[Token]

class Address(BaseModel):
    addresses: List[str]

@r.post("/daoMembership")
# async def daoMembership(addresses: Address):
async def daoMembership(token_inv: TokenInventoryDAO):
    try:
        membership = {}
        for tkn in token_inv.tokens:
            logger.debug(f'checking token id: {tkn.token_id}')
            tokens = Token(token_id=tkn.token_id, stake_tree=tkn.stake_tree, vest_tree=tkn.vest_tree, proxy_address=tkn.proxy_address)
            tid = TokenInventoryDAO(addresses=token_inv.addresses, tokens=[tokens])
            res = await locked(tid)
            membership[tkn.token_id] = res

        return membership

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')

@r.post("/locked/")
async def locked(tid: TokenInventoryDAO):
    try:
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

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')

@r.get("/price/{token_id}")
async def get_token_price(token_id: str):
    sql = text(f'''
        select token_price, token_name, decimals
        from tokens 
        where token_id = :token_id
    ''')
    with eng.begin() as con:
        res = con.execute(sql, {'token_id': token_id}).fetchone()

    return {
        'id': token_id,
        'name': res['token_name'],
        'decimals': res['decimals'],
        'price': res['token_price'],
    }

@r.get("/candles/{token_id}")
async def get_token_price(token_id: str):
    sql = text(f'''
        select date, price, market
        from ohlc 
        where token_id = :token_id
        order by date desc
    ''')
    with eng.begin() as con:
        res = con.execute(sql, {'token_id': token_id}).fetchall()

    price = res[0]['price']
    dateStamp = res[0]['date']
    market = res[0]['market']

    sql = text(f'''
        with tkn as (
            select amount/power(10, decimals) as tot
            from tokens 
            where token_id = '1fd6e032e8476c4aa54c18c1a308dce83940e8f4a28f576440513ed7326ad489'
        )
        select max(price) as ath
            , min(price) as atl
            , tkn.tot
        from ohlc 
        cross join tkn
        where token_id = :token_id
            and date > now() - interval '52 weeks'
            and price is not null
        group by tkn.tot
    ''')
    with eng.begin() as con:
        res = con.execute(sql, {'token_id': token_id}).fetchone()

    allTimeHigh = res['ath']
    allTimeLow = res['atl']
    totalSupply = res['tot']

    sql = text(f'''
        select token_name
            , k.token_id
            , k.token_price
            , k.supply_actual as current_total_supply
            , t.amount/power(10, t.decimals) as initial_total_supply
            , (t.amount - k.supply)/power(10, t.decimals) as burned
            , k.token_price * (supply - vested - emitted - stake_pool)/power(10, t.decimals) as market_cap
            , (supply - vested - emitted - stake_pool)/power(10, t.decimals) as in_circulation
        from tokenomics_paideia k
            join tokens t on t.token_id = k.token_id
    ''')
    # with eng.begin() as con:
    #     res = con.execute(sql).fetchone()

    # currentSupply = res['current_total_supply']
    # marketCap = res['market_cap']

    return {
        'id': token_id,
        'price': price,
        'marketCap': 0, # marketCap
        'allTimeHigh': allTimeHigh,
        'allTimeLow': allTimeLow,
        'dateStamp': dateStamp,
        'supply': 0, # currentSupply
        'total': totalSupply, # ?? incl burned

        'market': {
            'name': market,
            'dataPoints': res,
        }
    }

@r.get("/info/")
async def mint(token: Token):
    return {}
