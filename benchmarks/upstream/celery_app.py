"""Paired durable-request Celery baseline using the same application callable."""

import os
import sys

if sys.platform == "darwin":
    import billiard
    billiard.set_start_method("fork", force=True)

from celery import Celery
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from examples.dbworker_integration.runtime import Request, execute, adapter

if os.environ.get("BENCHMARK_INITIALIZE_ADAPTER"):
    adapter(os.environ["BENCHMARK_INITIALIZE_ADAPTER"])

app = Celery("upstream_benchmark", broker=os.environ["BENCHMARK_REDIS_URL"])
app.conf.update(task_acks_late=True, worker_prefetch_multiplier=1,
                task_reject_on_worker_lost=True, task_ignore_result=True)


@app.task(name="benchmark.execute_request")
def execute_request(identity: int):
    engine = create_engine(os.environ["DBWORKER_DATABASE_URL"])
    sessions = sessionmaker(engine)
    try:
        with sessions() as session:
            request = session.get(Request, identity)
            suite, payload = request.suite, dict(request.payload)
        result = execute(suite, payload, guarded=False)
        with sessions.begin() as session:
            session.get(Request, identity).result = result
    finally:
        engine.dispose()
