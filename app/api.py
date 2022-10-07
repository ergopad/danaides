import uvicorn
import logging
# import asyncio

from time import time
from os import getpid
from fastapi import FastAPI, Depends, Request # , BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi_utils.tasks import repeat_every
# from fastapi.concurrency import run_in_threadpool
from utils.db import init_db, refresh_views
from utils.logger import logger, myself
# from concurrent.futures.process import ProcessPoolExecutor
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from typing import Dict
from http import HTTPStatus

# from routes.dashboard import dashboard_router
from routes.snapshot import snapshot_router
from routes.token import token_router
from routes.tasks import tasks_router

app = FastAPI(
    title="Danaides",
    docs_url="/api/docs",
    openapi_url="/api"
)

CLEANUP_INTERVAL = 5 # mins
JOB_CHECK_INTERVAL = 60 # seconds

class Job(BaseModel):
    uid: UUID = Field(default_factory=uuid4)
    status: str = 'in_progress'
    params: dict = {}
    result: int = None
    start__ms: int = round(time() * 1000)
    end__ms: int = 0

jobs: Dict[UUID, Job] = {}

#region Routers
# app.include_router(dashboard_router, prefix="/api/dashboard", tags=["dashboard"]) #, dependencies=[Depends(get_current_active_user)])
app.include_router(snapshot_router, prefix="/api/snapshot", tags=["snapshot"])
app.include_router(token_router, prefix="/api/token", tags=["token"])
app.include_router(tasks_router, prefix="/api/tasks", tags=["tasks"])
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

@app.on_event("startup")
async def on_startup():
    logger.debug('init database')
    init_db()
    
    logger.debug('refresh materialized views')
    refresh_views()

@app.on_event("startup")
@repeat_every(seconds=JOB_CHECK_INTERVAL)
def cleanup_jobs() -> None:
    try:
        logger.debug(f'job:: cleanup\n  currently stored: {len(jobs)}')

        # find the keys
        del_keys = []
        for uid in jobs:
            # if older than 5m from finish
            if jobs[uid].end__ms < (time()-60*CLEANUP_INTERVAL)*1000:
                # logger.warning(f'{uid} needs to be cleaned')
                del_keys.append(uid)
                # logger.debug(f'key found {uid}')

        # remove from jobs dict
        for uid in del_keys:
            logger.warning(f'removing {uid}')
            # jobs.pop(uid, None) # attempt to remove regardless if exists in dict or not
            del jobs[uid]
            logger.warning(f'job:: cleanup task {uid}')

    except Exception as e:
        logger.error(f'ERR: {myself()}, {e}')
        
# @app.on_event("shutdown")
# async def on_shutdown():
#     app.state.executor.shutdown()

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

# drop and pop table
# @app.get("/api/tasks/dnp/{tbl}", status_code=HTTPStatus.ACCEPTED)
# async def drop_n_pop(tbl: str, background_tasks: BackgroundTasks):
#     try:
#         if tbl in [jobs[j].params['tbl'] for j in jobs if jobs[j].status == 'in_progress']:
#             uid = str([j for j in jobs if jobs[j].status == 'in_progress' and (jobs[j].params['tbl'] == tbl)][0])
#             logger.info(f'job currently processing; {uid}')        
#         else:
#             new_task = Job()
#             uid = str(new_task.uid)
#             jobs[new_task.uid] = new_task
#             jobs[new_task.uid].params['tbl'] = tbl
#             jobs[new_task.uid].start__ms = round(time() * 1000)        
#             res = await run_in_threadpool(lambda: dnp(tbl))
#             logger.debug(f'job {uid} status: {res}')
#             jobs[new_task.uid].status = 'complete'  
#             jobs[new_task.uid].end__ms = round(time() * 1000)
#             # jobs[new_task.uid].status = res['row_count_after']
#             
#         return {'uid': uid, 'table_name': tbl, 'status': 'in_process'}
# 
#     except Exception as e:
#         logger.error(f'ERR: {myself()}, {e}')

# get status, given uid
# @app.get("/api/tasks/status/{uid}")
# async def status_handler(uid: UUID):
#     if uid in jobs:
#         status = jobs[uid].status
#         if status == 'complete': elapsed = (jobs[uid].end__ms-jobs[uid].start__ms)/1000.0
#         else: elapsed = (round(time() * 1000)-jobs[uid].start__ms)/1000.0
#     else:
#         status = 'not_found'
#         elapsed = None
# 
#     return {
#         'uid': uid,
#         'status': status,
#         'elapsed__sec': elapsed
#     }

# @app.get("/api/tasks/alljobs")
# async def all_jobs():
#     return jobs

# @app.get("/api/tasks/pending")
# async def all_jobs():
#     res = {}
#     for uid in jobs:
#         if jobs[uid].status == 'in_progress':
#             res[uid] = (round(time() * 1000)-jobs[uid].start__ms)/1000.0
# 
#     return res

@app.get("/api/ping")
async def ping():
    return {"hello": "world"}

# MAIN
if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", reload=True, port=8000)
