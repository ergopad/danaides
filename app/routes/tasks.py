from utils.logger import logger
# from concurrent.futures.process import ProcessPoolExecutor
from http import HTTPStatus
from time import time
from fastapi import BackgroundTasks, APIRouter
from typing import Dict
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from utils.db import dnp

tasks_router = r = APIRouter()

### INIT
class Job(BaseModel):
    uid: UUID = Field(default_factory=uuid4)
    status: str = 'in_progress'
    params: dict = {}
    result: int = None
    start__ms: int = round(time() * 1000)

jobs: Dict[UUID, Job] = {}

### FUNCTIONS
async def rip(uid: UUID, param: str) -> None:
    jobs[uid].result = await run_in_process(dnp, param)
    jobs[uid].status = 'complete'

# drop and pop table
@r.post("/dnp/{tbl}", status_code=HTTPStatus.ACCEPTED)
async def drop_n_pop(tbl: str, background_tasks: BackgroundTasks):
    if tbl in [jobs[j].params['tbl'] for j in jobs if jobs[j].status == 'in_progress']:
        uid = str([j for j in jobs if jobs[j].status == 'in_progress' and (jobs[j].params['tbl'] == tbl)][0])
        logger.info(f'job currently processing; {uid}')        
    else:
        new_task = Job()
        jobs[new_task.uid] = new_task
        jobs[new_task.uid].params['tbl'] = tbl
        background_tasks.add_task(rip, new_task.uid, tbl)
        uid = str(new_task.uid)
        
    return {'uid': uid, 'table_name': tbl, 'status': 'in_process'}
###########################################################################

# get status, given uid
@r.get("/status/{uid}")
async def status_handler(uid: UUID):
    if uid in jobs:
        status = jobs[uid].status
        elapsed = (round(time() * 1000)-jobs[uid].start__ms)/1000.0
    else:
        status = 'not_found'
        elapsed = None

    return {
        'uid': uid,
        'status': status,
        'elapsed__sec': elapsed
    }

@r.get("/alljobs")
async def all_jobs():
    return jobs
