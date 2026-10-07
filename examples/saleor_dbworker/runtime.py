"""Durable one-for-one Saleor export and notification job variation."""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from types import SimpleNamespace

from kombu.serialization import dumps, loads
from sqlalchemy import Integer, String, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from dbworker import Coordinator, Finished
from benchmarks.common.native_observer import job_context, record

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
    identity = str(uuid.uuid4())
    engine = create_engine(os.environ["DBWORKER_DATABASE_URL"])
    try:
        with sessionmaker(engine).begin() as session:
            session.add(Job(task_id=identity, operation_id=str(operation_id), parent_id=parent_id,
                            task_name=task_name, payload=encoded))
        record(STAGES[task_name], operation_id, identity, "submitted", parent_id, backend="dbworker")
    finally:
        engine.dispose()
    return SimpleNamespace(id=identity)


@contextmanager
def durable_children(operation_id, parent_id):
    from celery.app.task import Task
    from kombu import Producer

    original, publish = Task.apply_async, Producer.publish

    def dispatch(task, args=None, kwargs=None, **options):
        if any(options.get(key) for key in ("countdown", "eta", "link", "link_error")):
            raise RuntimeError("Unsupported delayed or linked Saleor continuation")
        return enqueue(task.name, args or (), kwargs or {}, operation_id, parent_id)

    def reject(*args, **kwargs):
        raise RuntimeError("Saleor DBWorker attempted broker publication")

    Task.apply_async, Producer.publish = dispatch, reject
    try:
        yield
    finally:
        Task.apply_async, Producer.publish = original, publish


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
    record(stage, operation_id, identity, "started", parent_id, backend="dbworker")
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
        record(stage, operation_id, identity, "failed", parent_id, backend="dbworker")
        raise
    finally:
        close_old_connections()
    record(stage, operation_id, identity, "succeeded", parent_id, backend="dbworker")
    return Finished()


def coordinator(database_url, concurrency=2):
    engine = create_engine(database_url)
    Base.metadata.create_all(engine)
    runtime = Coordinator(sessionmaker(engine, expire_on_commit=False), database_url=database_url,
                          poll_seconds=.05, max_poll_seconds=.25, lease_seconds=300)
    runtime.transactional_worker(name="saleor", source=Job, concurrency=concurrency)(handle)
    return runtime
