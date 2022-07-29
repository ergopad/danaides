import uvicorn
import logging

from time import time
from os import getpid
from pydantic import BaseModel
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from utils.db import eng

# from api.v1.routes.users import users_router
# from api.v1.routes.auth import auth_router

app = FastAPI(
    title="Danaides",
    docs_url="/api/docs",
    openapi_url="/api"
)

#region Routers
# app.include_router(users_router,        prefix="/api/users",         tags=["users"], dependencies=[Depends(get_current_active_user)])
# app.include_router(auth_router,         prefix="/api/auth",          tags=["auth"])
#endregion Routers

# origins = ["*"]
origins = [
    "https://*.ergopad.io",
    "http://75.155.140.173:3000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    # allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.middleware("http")
async def add_logging_and_process_time(req: Request, call_next):
    try:
        logging.debug(f"""### REQUEST: {req.url} | host: {req.client.host}:{req.client.port} | pid {getpid()} ###""")
        beg = time()
        resNext = await call_next(req)
        tot = f'{time()-beg:0.3f}'
        resNext.headers["X-Process-Time-MS"] = tot
        logging.debug(f"""### %%% TOOK {tot} / ({req.url}) %%% ###""")
        return resNext

    except Exception as e:
        logging.debug(e)
        return resNext
        pass

@app.get("/api/ping")
async def ping():
    return {"hello": "world"}

class token(BaseModel):
    id: str
    name: str = ''
    decimals: int = 0
    amount: int = 0

@app.post("/api/burn/")
async def burn(token: token):
    # check is valid token
    logging.debug(f'burning token: {token.id}')

    # build tx
    tx = {
        'hello': 'world'
    }
    logging.debug(f'transaction: {tx}')

    # try to sign/submit
    return {"tx": tx}

@app.post("/api/snapshot/")
async def burn(token: token):
    return {}

@app.post("/api/balances/")
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

# MAIN
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", reload=True, port=8000)
