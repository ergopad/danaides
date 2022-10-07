# import asyncio

from utils.logger import logger
# from concurrent.futures.process import ProcessPoolExecutor
from http import HTTPStatus
from time import time
from fastapi import BackgroundTasks, APIRouter, Depends # , HTTPException, status, FastAPI
from typing import Dict
from uuid import UUID, uuid4
# from pydantic import BaseModel, Field
from utils.db import eng, text

tasks_router = r = APIRouter()

@r.get("/refresh/{matview}")
async def refresh_matview(matview):
    try:
        with eng.begin() as con:
            sql = f'''refresh materialized view {matview}'''
            con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: {e}')

@r.get("/refreshall/")
async def refresh_all_matviews(matview):
    try:
        with eng.begin() as con:
            sql = f'''
                select matviewname
                from pg_matviews 
                where schemaname = 'public'
                    -- and left(viewname, 2) = 'v_'
            '''
            res = con.execunte(sql).fetchall()

        for r in res:
            mv = r['viewname']
            try: 
                logger.debug(f'refreshing table {mv}')
                await refresh_matview(mv)
            except Exception as e:
                logger.error(f'ERR: {e}')        
                pass

    except Exception as e:
        logger.error(f'ERR: {e}')

# get status, given uid
@r.get("/status/{uid}")
async def status_handler(uid: UUID):
    return {
        'uid': jobs[uid],
        'status': jobs[uid].status,
        'elapsed__sec': time()-jobs[uid].start
    }

### MAIN