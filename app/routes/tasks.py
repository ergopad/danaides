import asyncio

from utils.logger import logger
from concurrent.futures.process import ProcessPoolExecutor
from http import HTTPStatus
from time import time
from fastapi import BackgroundTasks, APIRouter, Depends # , HTTPException, status, FastAPI
from typing import Dict
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from utils.db import eng, text, dnp

tasks_router = r = APIRouter()

### INIT
class Job(BaseModel):
    uid: UUID = Field(default_factory=uuid4)
    status: str = 'in_progress'
    result: int = None
    start: time()

jobs: Dict[UUID, Job] = {}
dnp_tracker: Dict[str, UUID] = {}

### FUNCTIONS
async def run_in_process(fn, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(r.state.executor, fn, *args)  # wait and return result

###########################################################################
# drop and pop
###########################################################################
async def drop_n_pop(uid: UUID, table_name: str) -> None:
    jobs[uid].result = await run_in_process(dnp, table_name)
    jobs[uid].status = 'complete'
    
    # remove table from tracker
    dnp_tracker.pop(table_name, None)

# drop and pop table
@r.post("/dnp/{tbl}", status_code=HTTPStatus.ACCEPTED)
async def task_handler(tbl: str, background_tasks: BackgroundTasks):
    if tbl in dnp_tracker:
        logger.info(f'job currently processing; {dnp_tracker[tbl]}')
        return {}
    else:
        new_task = Job()
        jobs[new_task.uid] = new_task
        background_tasks.add_task(drop_n_pop, new_task.uid, tbl)
        return new_task
###########################################################################

# get status, given uid
@r.get("/status/{uid}")
async def status_handler(uid: UUID):
    return {
        'uid': jobs[uid],
        'status': jobs[uid].status,
        'elapsed__sec': time()-jobs[uid].start
    }

### MAIN