import uvicorn

from time import time
from os import getpid
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from utils.db import init_db, refresh_views
from utils.logger import logger, myself, LEIF
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from typing import Dict
# from fastapi import BackgroundTasks
# from fastapi_utils.tasks import repeat_every
# from fastapi.concurrency import run_in_threadpool
# from concurrent.futures.process import ProcessPoolExecutor
# from http import HTTPStatus

from routes.snapshot import snapshot_router
from routes.token import token_router
from routes.tasks import tasks_router
# from routes.dashboard import dashboard_router

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
app.include_router(snapshot_router, prefix="/api/snapshot", tags=["snapshot"])
app.include_router(token_router, prefix="/api/token", tags=["token"])
app.include_router(tasks_router, prefix="/api/tasks", tags=["tasks"])
# app.include_router(dashboard_router, prefix="/api/dashboard", tags=["dashboard"]) #, dependencies=[Depends(get_current_active_user)])
#endregion Routers

origins = [
    "https://*.ergopad.io",
    "https://*.paideia.im"
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
    try:
        logger.debug('init database')
        init_db()
        
        logger.debug('refresh materialized views')
        # first time, make sure to refresh for matviews created, "with no data"
        refresh_views(concurrently=False)

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')

@app.middleware("http")
async def add_logging_and_process_time(req: Request, call_next):
    try:
        try: 
            logger.log(LEIF, f"""REQUEST: {req.url} | host: {req.client.host}:{req.client.port} | pid {getpid()}""")
        except Exception as e: 
            logger.log(LEIF, f"""REQUEST ERROR: {e}""")
            pass
        beg = time()
        res_next = await call_next(req)
        tot = f'{time()-beg:0.3f}'
        res_next.headers["X-Process-Time-MS"] = tot
        logger.log(LEIF, f"""TOOK {tot} / ({req.url})""")
        return res_next

    except Exception as e:
        logger.error(f'ERR: {myself()}; {e}')
        return res_next

@app.get("/api/ping")
async def ping():
    return {"hello": "world"}

# MAIN
if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", reload=True, port=8000)
