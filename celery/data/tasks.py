import logging

from os import getenv
from datetime import datetime
from celery import Celery
from time import time, sleep
from sqlalchemy import create_engine, text

### INIT
app = Celery("tasks", broker=getenv('CELERY_BROKER_URL', 'redis://'), backend=getenv('CELERY_RESULT_BACKEND', 'd-redis'))
app.conf.CELERY_ACCEPT_CONTENT = ['pickle', 'json', 'msgpack', 'yaml']
app.conf.CELERY_WORKER_SEND_TASK_EVENTS = True

DB_DANAIDES = f"postgresql://{getenv('DANAIDES_USER')}:{getenv('DANAIDES_PASSWORD')}@{getenv('POSTGRES_HOST')}:{getenv('POSTGRES_PORT')}/{getenv('POSTGRES_DB')}"
eng = create_engine(DB_DANAIDES)

import inspect
myself = lambda: inspect.stack()[1][3]

### TASKS
@app.task
def refresh_matview(matview):
    try:
        with eng.begin() as con:
            sql = f'''refresh materialized view concurrently {matview}'''
            con.execute(sql)

    except Exception as e:
        logging.error(f'ERR: {myself()}; {e}')

@app.task
def refresh_all_matviews():
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
                logging.info(f'refreshing table {mv}')
                refresh_matview(mv)
            except Exception as e:
                logging.error(f'ERR: {e}')        
                pass

    except Exception as e:
        logging.error(f'ERR: {myself()}; {e}')


#region DEFAULT
@app.task
def add(x, y):
    return x + y

@app.task
def sleep(seconds):
    sleep(seconds)

@app.task
def echo(msg, timestamp=False):
    return "%s: %s" % (datetime.now(), msg) if timestamp else msg

@app.task
def error(msg):
    raise Exception(msg)
#endregion DEFAULT

### MAIN
if __name__ == "__main__":
    app.start()
