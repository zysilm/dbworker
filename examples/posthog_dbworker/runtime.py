"""Two durable stages matching native notification and delivery task boundaries."""
from __future__ import annotations
import os
import time
from sqlalchemy import JSON, Float, Integer, String, create_engine, event, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from dbworker import Coordinator, Finished, Unfinished
from benchmarks.common.native_observer import job_context, record, worker_origin, argument_digest
from examples.dbworker_integration.runtime import forbid_task_dispatch

STAGES = {"posthog.tasks.email.send_two_factor_auth_enabled_email": "notification",
          "posthog.email._send_email": "delivery"}

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


def node(operation_id, stage):
    return f"{stage}:{operation_id}"


def task_name(stage):
    """Use the same reviewed native identity for publication and execution."""
    return next(name for name, task_stage in STAGES.items() if task_stage == stage)


def enqueue(session, operation_id, user_id):
    job = Job(operation_id=operation_id, stage="notification", payload={"user_id": user_id})
    session.add(job)
    record("notification", operation_id, node(operation_id, "notification"), "submitted", backend="dbworker",
           task_name=task_name("notification"), argument_sha256=argument_digest((user_id,), {}))
    return job


def handle(job, session):
    from examples.posthog_dbworker import adapter
    app = adapter.initialize()
    from django.db import close_old_connections
    identity, op, stage, parent, payload, attempts = job.id, job.operation_id, job.stage, job.parent_id, dict(job.payload), job.attempts
    session.rollback()
    identity_node = node(op, stage)
    name = task_name(stage)
    origin = worker_origin(app.tasks[name], "posthog")
    record(stage, op, identity_node, "started", parent, backend="dbworker",
           task_name=name, native_worker_origin=origin,
           argument_sha256=argument_digest((payload["user_id"],), {}) if stage == "notification"
           else argument_digest((), payload))
    close_old_connections()
    try:
        with job_context(op, identity_node), forbid_task_dispatch():
            if stage == "notification":
                children = []
                def submit(arguments):
                    child = Job(operation_id=op, stage="delivery", parent_id=identity_node, payload=arguments)
                    session.add(child)
                    children.append(child)
                    record("delivery", op, node(op, "delivery"), "submitted", identity_node, backend="dbworker",
                           task_name=task_name("delivery"), argument_sha256=argument_digest((), arguments))
                adapter.notify(payload["user_id"], submit)
                if len(children) != 1:
                    raise RuntimeError("Native notification did not produce exactly one independent delivery")
            elif stage == "delivery":
                adapter.deliver(payload)
            else:
                raise ValueError("Unknown notification stage")
    except Exception:
        session.rollback()
        stored = session.get(Job, identity)
        if attempts >= 3:
            record(stage, op, identity_node, "failed", parent, backend="dbworker")
            raise
        from celery.utils.time import get_exponential_backoff_interval
        stored.attempts = attempts + 1
        stored.next_run = time.time() + get_exponential_backoff_interval(factor=1, retries=attempts, maximum=600, full_jitter=True)
        record(stage, op, identity_node, "retried", parent, backend="dbworker", attempt=stored.attempts)
        return Unfinished()
    finally:
        close_old_connections()
    session.get(Job, identity).complete = True
    # The coordinator commits source completion and its execution ledger
    # together. Observe success only after that owned transaction commits.
    event.listen(session, "after_commit", lambda committed: record(
        stage, op, identity_node, "succeeded", parent, backend="dbworker"), once=True)
    return Finished()


def coordinator(url, concurrency=2):
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    runtime = Coordinator(sessionmaker(engine, expire_on_commit=False), database_url=url,
        poll_seconds=.05, max_poll_seconds=.25, lease_seconds=300)
    # Both stages share a two-process resource budget through one worker, but
    # each source row is a separate business job and the graph verifies fan-out.
    runtime.transactional_worker(name="posthog_workflow", source=Job, concurrency=concurrency,
                                 eligible=lambda: select(Job).where(Job.next_run <= time.time()))(handle)
    runtime.create_worker_tables()
    return runtime
