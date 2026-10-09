"""Durable one-for-one Saleor export and notification job variation."""

from __future__ import annotations

import os
import uuid
import threading
from contextvars import ContextVar
from contextlib import contextmanager
from types import SimpleNamespace

from kombu.serialization import dumps, loads
from sqlalchemy import Integer, String, Text, create_engine, event
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from dbworker import Coordinator, Finished
from benchmarks.common.native_observer import job_context, record, worker_origin, argument_digest

STAGES = {
    "export-products": "export",
    "saleor.plugins.admin_email.tasks.send_email_with_link_to_download_file_task": "email",
    "saleor.plugins.admin_email.tasks.send_export_failed_email_task": "failure_email",
}


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "saleor_benchmark_job"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_id: Mapped[str] = mapped_column(String(40), unique=True)
    operation_id: Mapped[str] = mapped_column(String(40))
    parent_id: Mapped[str | None] = mapped_column(String(40), nullable=True)
    task_name: Mapped[str] = mapped_column(String(200))
    payload: Mapped[str] = mapped_column(Text)


def enqueue(task_name, args, kwargs, operation_id, parent_id=None):
    if task_name not in STAGES:
        raise RuntimeError(f"Unsupported Saleor continuation: {task_name}")
    _, _, encoded = dumps([list(args), kwargs], serializer="json")
    encoded_args, encoded_kwargs = loads(encoded, "application/json", "utf-8")
    fingerprint = argument_digest(encoded_args, encoded_kwargs)
    identity = str(uuid.uuid4())
    engine = create_engine(os.environ["DBWORKER_DATABASE_URL"])
    try:
        with sessionmaker(engine).begin() as session:
            session.add(Job(task_id=identity, operation_id=str(operation_id), parent_id=parent_id,
                            task_name=task_name, payload=encoded))
            # Record publication intent before making the durable row visible,
            # just as native observation precedes broker transport. A failed
            # commit leaves an unmatched intent that fails trace admission.
            record(STAGES[task_name], operation_id, identity, "submitted", parent_id,
                   backend="dbworker", task_name=task_name, argument_sha256=fingerprint)
    finally:
        engine.dispose()
    return SimpleNamespace(id=identity)


_dispatch_context = ContextVar("saleor_dbworker_dispatch", default=None)
_dispatch_lock = threading.RLock()
_dispatch_users = 0
_dispatch_originals = None


@contextmanager
def durable_children(operation_id, parent_id):
    """Replace publication with durable jobs while isolating concurrent contexts.

    The process-wide interceptor is installed once for overlapping contexts.
    Task arguments and correlation come from the calling thread's ContextVar,
    never from a closure shared by another producer.
    """
    from celery.app.task import Task
    from kombu import Producer
    global _dispatch_users, _dispatch_originals

    def dispatch(task, args=None, kwargs=None, **options):
        context = _dispatch_context.get()
        if context is None:
            raise RuntimeError("Saleor DBWorker publication has no durable context")
        if any(options.get(key) for key in ("countdown", "eta", "link", "link_error")):
            raise RuntimeError("Unsupported delayed or linked Saleor continuation")
        return enqueue(task.name, args or (), kwargs or {}, *context)

    def reject(*args, **kwargs):
        raise RuntimeError("Saleor DBWorker attempted broker publication")

    token = _dispatch_context.set((operation_id, parent_id))
    with _dispatch_lock:
        if _dispatch_users == 0:
            _dispatch_originals = (Task.apply_async, Producer.publish)
            Task.apply_async, Producer.publish = dispatch, reject
        _dispatch_users += 1
    try:
        yield
    finally:
        with _dispatch_lock:
            _dispatch_users -= 1
            if _dispatch_users == 0:
                Task.apply_async, Producer.publish = _dispatch_originals
                _dispatch_originals = None
        _dispatch_context.reset(token)


def handle(job, session):
    from examples.saleor_dbworker.adapter import initialize
    initialize()
    from django.db import close_old_connections
    from saleor.celeryconf import app

    identity, operation_id, parent_id = job.task_id, job.operation_id, job.parent_id
    task_name, payload = job.task_name, job.payload
    session.rollback()
    task = app.tasks[task_name]
    args, kwargs = loads(payload, "application/json", "utf-8")
    stage = STAGES[task_name]
    origin = worker_origin(task, "saleor")
    fingerprint = argument_digest(args, kwargs)
    record(stage, operation_id, identity, "started", parent_id, backend="dbworker",
           task_name=task_name, native_worker_origin=origin, argument_sha256=fingerprint)
    close_old_connections()
    try:
        with job_context(operation_id, identity), durable_children(operation_id, identity):
            try:
                result = task(*args, **kwargs)
            except Exception as exc:
                task.on_failure(exc, identity, args, kwargs, SimpleNamespace(type=type(exc)))
                raise
            task.on_success(result, identity, args, kwargs)
    except Exception:
        record(stage, operation_id, identity, "failed", parent_id, backend="dbworker", task_name=task_name)
        raise
    finally:
        close_old_connections()
    # The coordinator commits Finished after this handler returns. Trace success
    # only after that durable transaction, not before Work reaches FINISHED.
    def committed(committed_session):
        record(stage, operation_id, identity, "succeeded", parent_id,
               backend="dbworker", task_name=task_name, argument_sha256=fingerprint)

    event.listen(session, "after_commit", committed, once=True)
    return Finished()


def coordinator(database_url, concurrency=2):
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    runtime = Coordinator(sessionmaker(engine, expire_on_commit=False), database_url=database_url,
                          poll_seconds=.05, max_poll_seconds=.25, lease_seconds=300)
    runtime.transactional_worker(name="saleor", source=Job, concurrency=concurrency)(handle)
    runtime.create_worker_tables()
    return runtime
