import uvicorn
import logging

from time import time
from os import getpid
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from utils.db import init_db
from concurrent.futures.process import ProcessPoolExecutor

# from routes.dashboard import dashboard_router
from routes.snapshot import snapshot_router
from routes.token import token_router
# from routes.tasks import tasks_router   

app = FastAPI(
    title="Danaides",
    docs_url="/api/docs",
    openapi_url="/api"
)

#region Routers
# app.include_router(dashboard_router, prefix="/api/dashboard", tags=["dashboard"]) #, dependencies=[Depends(get_current_active_user)])
app.include_router(snapshot_router, prefix="/api/snapshot", tags=["snapshot"])
app.include_router(token_router, prefix="/api/token", tags=["token"])
# app.include_router(tasks_router, prefix="/api/tasks", tags=["tasks"])
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
    await init_db()
    app.state.executor = ProcessPoolExecutor()

@app.on_event("shutdown")
async def on_shutdown():
    app.state.executor.shutdown()

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

# MAIN
if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", reload=True, port=8000)
