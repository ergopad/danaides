from utils.logger import logger, myself
from time import time
from fastapi import APIRouter
from utils.db import eng
from requests import get, post
from os import getenv

tasks_router = r = APIRouter()

USE_CELERY = getenv('USE_CELERY', 'False').lower() in ('true', '1', 't')

@r.get("/refresh/{matview}")
async def refresh_matview(matview):
    try:
        if USE_CELERY:
            res = post('http://d-flower:5555/api/task/async-apply/tasks.refresh_matview', json={'args':[matview]})
            if res.ok: logger.debug(res.text)
            else: logger.warning(res.status_code)
        else:
            with eng.begin() as con:
                sql = f'''refresh materialized view concurrently {matview}'''
                con.execute(sql)

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')

@r.get("/refreshall/")
async def refresh_all_matviews():
    try:
        with eng.begin() as con:
            sql = f'''
                select matviewname
                from pg_matviews 
                where schemaname = 'public'
                    -- and left(viewname, 2) = 'v_'
            '''
            res = con.execute(sql).fetchall()

        for r in res:
            mv = r['matviewname']
            try: 
                logger.debug(f'refreshing table {mv}')
                await refresh_matview(mv)
            except Exception as e:
                logger.error(f'ERR: {e}')        
                pass

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')

### MAIN