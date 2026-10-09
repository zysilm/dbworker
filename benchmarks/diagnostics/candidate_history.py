"""Measure one fixed empty SQLite queue containing 100,000 finished source rows.

This is a read-query diagnostic, not a worker or application performance score.
It uses the original candidate SQL and never executes or claims synthetic history.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path

import dbworker
from sqlalchemy import create_engine, insert, select
from sqlalchemy.orm import sessionmaker

from benchmarks.common.load import distribution
from benchmarks.diagnostics.worker_handoff import Base, Job, handle
from dbworker import Coordinator, ExecutionStatus, now


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    directory = args.output.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    url = f"sqlite:///{directory / 'history.sqlite3'}"
    engine = create_engine(url)
    try:
        Base.metadata.create_all(engine)
        runtime = Coordinator(sessionmaker(engine), database_url=url)
        runtime.transactional_worker(name="posthog_workflow", source=Job, concurrency=64,
                                     eligible=lambda: select(Job).where(Job.next_run <= time.time()))(handle)
        runtime.create_worker_tables()
        worker = runtime.workers["posthog_workflow"]
        with engine.begin() as connection:
            for offset in range(0, 100000, 1000):
                ids = list(range(offset + 1, offset + 1001))
                connection.execute(insert(Job.__table__), [{
                    "id": identity, "operation_id": f"history-{identity}", "stage": "notification",
                    "parent_id": None, "payload": {"user_id": identity}, "attempts": 0,
                    "next_run": 0, "complete": True} for identity in ids])
                connection.execute(insert(worker.table), [{"source_id": identity,
                    "execution_status": ExecutionStatus.FINISHED, "claim_token": None,
                    "lease_expires_at": None, "error": None} for identity in ids])
        query = runtime._candidate(worker, now())
        durations = []
        with engine.connect() as connection:
            # Prime the page cache once; initialization and warmup are untimed.
            if connection.execute(query).scalar() is not None:
                raise AssertionError("Finished history unexpectedly became eligible")
            for _ in range(100):
                started = time.monotonic()
                value = connection.execute(query).scalar()
                durations.append(time.monotonic() - started)
                if value is not None:
                    raise AssertionError("Finished history unexpectedly became eligible")
            sql = str(query.compile(engine, compile_kwargs={"literal_binds": True}))
            plan = [list(row) for row in connection.exec_driver_sql("EXPLAIN QUERY PLAN " + sql)]
            report = {"scope": "Empty SQLite candidate-query diagnostic; no jobs execute; no official performance score",
                      "core_sha256": hashlib.sha256(Path(dbworker.__file__).read_bytes()).hexdigest(),
                      "configuration": {"finished_source_rows": 100000, "finished_ledger_rows": 100000,
                                        "query_repetitions": 100, "warmup_queries": 1, "workers_started": 0},
                      "sqlite_version": connection.exec_driver_sql("SELECT sqlite_version()").scalar(),
                      "journal_mode": connection.exec_driver_sql("PRAGMA journal_mode").scalar(),
                      "query_seconds": distribution(durations), "candidate_sql": sql, "query_plan": plan}
        (directory / "result.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
        print(json.dumps(report["query_seconds"], indent=2))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
