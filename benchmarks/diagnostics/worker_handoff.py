"""Profile the actual coordinator with a PostHog-shaped two-stage PostgreSQL queue.

This is a scheduler diagnostic, not an application benchmark: native 2FA, Django,
rendering and SMTP are deliberately absent. No result enters the official table.
Run from an ordinary local directory outside cloud synchronization.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import socket
import subprocess
import threading
import time
from collections import Counter
from pathlib import Path

from sqlalchemy import JSON, Float, Index, Integer, String, create_engine, event, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from dbworker import Coordinator, ExecutionStatus, Finished, now
from benchmarks.common.load import distribution, run_load


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "posthog_notification_job"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    operation_id: Mapped[str] = mapped_column(String(128))
    stage: Mapped[str] = mapped_column(String(32))
    parent_id: Mapped[str | None] = mapped_column(String(160), nullable=True)
    payload: Mapped[dict] = mapped_column(JSON)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_run: Mapped[float] = mapped_column(Float, default=0)
    complete: Mapped[bool] = mapped_column(default=False)


def receipt(phase, identity, stage):
    line = json.dumps({"phase": phase, "id": identity, "stage": stage,
                       "timestamp": time.monotonic(), "pid": os.getpid()}) + "\n"
    descriptor = os.open(os.environ["HANDOFF_TRACE"], os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        payload = line.encode()
        if os.write(descriptor, payload) != len(payload):
            raise RuntimeError("Incomplete diagnostic receipt")
    finally:
        os.close(descriptor)


def handle(job, session):
    identity, stage = job.id, job.stage
    receipt("started", identity, stage)
    time.sleep(float(os.environ["HANDOFF_HANDLER_SECONDS"]))
    if stage == "notification":
        session.add(Job(operation_id=job.operation_id, stage="delivery",
                        parent_id=f"notification:{job.operation_id}", payload=job.payload))
    job.complete = True
    event.listen(session, "after_commit", lambda _: receipt("succeeded", identity, stage), once=True)
    return Finished()


class ProfiledCoordinator(Coordinator):
    """Measure claims without copying or modifying the scheduling loop."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.claims = []
        self.renewals = []

    def claim(self, worker):
        started = time.monotonic()
        value = super().claim(worker)
        self.claims.append({"started": started, "finished": time.monotonic(),
                            "source_id": value.source_id if value else None})
        return value

    def renew(self, worker, claims):
        claims = list(claims)
        started = time.monotonic()
        super().renew(worker, claims)
        self.renewals.append({"seconds": time.monotonic() - started, "claims": len(claims)})


def summarize_receipts(events):
    starts, finishes = {}, {}
    for row in events:
        target = starts if row["phase"] == "started" else finishes
        if row["id"] in target:
            raise AssertionError("Duplicate diagnostic execution")
        target[row["id"]] = row
    if set(starts) != set(finishes):
        raise AssertionError("Missing terminal diagnostic execution")
    boundaries = []
    durations = []
    for identity, row in starts.items():
        end = finishes[identity]["timestamp"]
        durations.append(end - row["timestamp"])
        boundaries.extend(((row["timestamp"], 1), (end, -1)))
    active = peak = 0
    occupied = 0.0
    previous = min(timestamp for timestamp, _ in boundaries)
    for timestamp, delta in sorted(boundaries):
        occupied += active * (timestamp - previous)
        active += delta
        peak = max(peak, active)
        previous = timestamp
    elapsed = max(timestamp for timestamp, _ in boundaries) - min(timestamp for timestamp, _ in boundaries)
    return {"tasks": len(starts), "stage_counts": dict(Counter(row["stage"] for row in starts.values())),
            "peak_running": peak, "mean_running_during_execution_span": occupied / elapsed,
            "execution_seconds": distribution(durations),
            "processes_used": len({row["pid"] for row in starts.values()})}, starts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--operations", type=int, default=5000)
    parser.add_argument("--window-seconds", type=float, default=60)
    parser.add_argument("--handler-seconds", type=float, default=.02)
    parser.add_argument("--timeout-seconds", type=float, default=600)
    parser.add_argument("--eligibility", choices=("baseline", "pending-index"), default="baseline",
                        help="Diagnostic-only prototype: exclude complete source rows and index pending work")
    args = parser.parse_args()
    directory = args.output.resolve()
    directory.mkdir(parents=True, exist_ok=False)
    os.environ.update(HANDOFF_TRACE=str(directory / "executions.jsonl"),
                      HANDOFF_HANDLER_SECONDS=str(args.handler_seconds))
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    cluster = directory / "postgres"
    with (directory / "initdb.log").open("wb") as stream:
        subprocess.run(["initdb", "-D", str(cluster), "-A", "trust", "-U", "benchmark",
                        "--encoding=UTF8"], stdout=stream, stderr=subprocess.STDOUT, check=True)
    log = (directory / "postgres.log").open("wb")
    postgres = subprocess.Popen(["postgres", "-D", str(cluster), "-h", "127.0.0.1", "-p", str(port),
                                 "-k", ""], stdout=log, stderr=subprocess.STDOUT)
    runtime = None
    engine = monitor_engine = None
    observer = None
    stop = threading.Event()
    timeline, sql = [], []
    try:
        import psycopg
        deadline = time.monotonic() + 30
        connection = None
        while connection is None:
            try:
                connection = psycopg.connect(f"host=127.0.0.1 port={port} user=benchmark dbname=postgres", autocommit=True)
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Owned PostgreSQL did not start")
                time.sleep(.05)
        with connection:
            connection.execute("CREATE DATABASE requests")
        url = f"postgresql+psycopg://benchmark@127.0.0.1:{port}/requests"
        engine = create_engine(url)
        monitor_engine = create_engine(url)
        Base.metadata.create_all(engine)
        if args.eligibility == "pending-index":
            Index("diagnostic_pending_job", Job.next_run, Job.id,
                  postgresql_where=Job.complete.is_(False)).create(engine)
        sessions = sessionmaker(engine, expire_on_commit=False)
        runtime = ProfiledCoordinator(sessions, database_url=url, poll_seconds=.05,
                                      max_poll_seconds=.25, lease_seconds=300)
        def eligible():
            query = select(Job).where(Job.next_run <= time.time())
            return query.where(Job.complete.is_(False)) if args.eligibility == "pending-index" else query
        runtime.transactional_worker(name="posthog_workflow", source=Job, concurrency=args.workers,
                                     eligible=eligible)(handle)
        runtime.create_worker_tables()

        @event.listens_for(engine, "before_cursor_execute")
        def before(connection, cursor, statement, parameters, context, executemany):
            context.diagnostic_started = time.monotonic()

        @event.listens_for(engine, "after_cursor_execute")
        def after(connection, cursor, statement, parameters, context, executemany):
            if threading.current_thread().name == "posthog_workflow":
                sql.append({"seconds": time.monotonic() - context.diagnostic_started,
                            "kind": "candidate" if "FOR UPDATE" in statement else statement.split()[0]})

        started = time.monotonic()
        runtime.start()
        def observe():
            with monitor_engine.connect() as connection:
                while not stop.is_set():
                    total, complete = connection.execute(select(func.count(), func.count().filter(Job.complete))).one()
                    ledger = connection.execute(select(runtime.workers["posthog_workflow"].table.c.execution_status,
                        func.count()).group_by(runtime.workers["posthog_workflow"].table.c.execution_status)).all()
                    connection.rollback()
                    timeline.append({"seconds": time.monotonic() - started, "published": total,
                                     "complete": complete, "outstanding": total - complete,
                                     "execution_status": {str(status): count for status, count in ledger}})
                    stop.wait(.5)
        observer = threading.Thread(target=observe, name="diagnostic-observer")
        observer.start()
        def submit(index, _):
            with sessions.begin() as session:
                session.add(Job(operation_id=f"notification-{index:04d}", stage="notification", payload={"user_id": index}))
            return index
        load = run_load(range(args.operations), submit, producers=8, duration_seconds=args.window_seconds)
        deadline = time.monotonic() + args.timeout_seconds
        table = runtime.workers["posthog_workflow"].table
        while True:
            with sessions() as session:
                done = session.scalar(select(func.count()).select_from(table).where(table.c.execution_status == ExecutionStatus.FINISHED))
                failed = session.scalar(select(func.count()).select_from(table).where(table.c.execution_status == ExecutionStatus.FAILED))
            if failed:
                raise AssertionError("Diagnostic worker failed")
            if done == args.operations * 2:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Finished {done}/{args.operations * 2} diagnostic tasks")
            time.sleep(.1)
        elapsed = time.monotonic() - started
        stop.set()
        observer.join()
        runtime.stop()
        receipts = [json.loads(line) for line in (directory / "executions.jsonl").read_text().splitlines()]
        execution, starts = summarize_receipts(receipts)
        assert execution["stage_counts"] == {"notification": args.operations, "delivery": args.operations}
        claims = [row for row in runtime.claims if row["source_id"] is not None]
        assert len(claims) == args.operations * 2
        dispatch = [starts[row["source_id"]]["timestamp"] - row["finished"] for row in claims]
        candidate = runtime._candidate(runtime.workers["posthog_workflow"], now()).with_for_update(
            skip_locked=True, of=Job.__table__)
        query = str(candidate.compile(engine, compile_kwargs={"literal_binds": True}))
        with engine.connect() as connection:
            plan = [row[0] for row in connection.exec_driver_sql("EXPLAIN (ANALYZE, BUFFERS) " + query)]
        report = {"scope": "PostHog-shaped scheduler diagnostic; no native 2FA/rendering/SMTP; not official benchmark",
                  "configuration": vars(args) | {"output": str(directory), "producer_count": 8},
                  "environment": {"python": platform.python_version(), "platform": platform.platform()},
                  "wall_seconds": elapsed, "execution": execution,
                  "claim_seconds": distribution([row["finished"] - row["started"] for row in claims]),
                  "claim_total_seconds": sum(row["finished"] - row["started"] for row in claims),
                  "claim_to_handler_seconds": distribution(dispatch),
                  "renewals": runtime.renewals, "sql_counts": dict(Counter(row["kind"] for row in sql)),
                  "candidate_sql_seconds": distribution([row["seconds"] for row in sql if row["kind"] == "candidate"]),
                  "producer_load": load, "timeline": timeline,
                  "peak_published_outstanding": max(row["outstanding"] for row in timeline),
                  "candidate_sql": query, "completed_queue_explain": plan,
                  "instrumentation": "Parent claim/SQL timers, append-only child start/commit receipts, 0.5s separate-connection observer"}
        (directory / "result.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
        print(json.dumps({key: report[key] for key in ("wall_seconds", "execution", "claim_seconds",
            "claim_total_seconds", "claim_to_handler_seconds", "candidate_sql_seconds", "peak_published_outstanding")}, indent=2))
    finally:
        stop.set()
        if observer:
            observer.join()
        if runtime:
            runtime.stop()
        if engine:
            engine.dispose()
        if monitor_engine:
            monitor_engine.dispose()
        postgres.terminate()
        try:
            postgres.wait(timeout=10)
        except subprocess.TimeoutExpired:
            postgres.kill()
            postgres.wait()
        log.close()


if __name__ == "__main__":
    main()
