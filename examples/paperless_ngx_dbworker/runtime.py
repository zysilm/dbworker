"""Durable one-task-per-job Paperless scheduler with original business bodies."""
from __future__ import annotations

import base64
import contextvars
import inspect
import os
import pickle
import uuid
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from sqlalchemy import event, DateTime, JSON, String, Text, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from dbworker import Coordinator, Finished
from benchmarks.common.business_input import business_input
from benchmarks.common.native_observer import argument_digest, job_context, record

STAGES = {"documents.tasks.consume_file": "ingestion",
          "documents.tasks.index_document": "deferred_index",
          "documents.tasks.update_document_in_llm_index": "ai_index",
          "documents.workflows.webhooks.send_webhook": "workflow_webhook"}
_current = contextvars.ContextVar("paperless_dbworker_job", default=None)


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "paperless_dbworker_job"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    operation_id: Mapped[str] = mapped_column(String(120))
    parent_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    task_name: Mapped[str] = mapped_column(String(200))
    arguments: Mapped[str] = mapped_column(Text)
    headers: Mapped[dict] = mapped_column(JSON)
    available_at: Mapped[datetime] = mapped_column(DateTime)
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)


def initialize():
    from examples.paperless_ngx_dbworker.adapter import initialize as setup
    setup()


def encode_arguments(args, kwargs):
    # Trusted, locally constructed fixture objects only, same object semantics
    # as native signed-pickle. Not an untrusted public submission endpoint.
    return base64.b64encode(pickle.dumps((tuple(args), dict(kwargs)))).decode("ascii")


def decode_arguments(value):
    return pickle.loads(base64.b64decode(value))


def enqueue(task, args=(), kwargs=None, *, headers=None, countdown=0, eta=None, **options):
    context = _current.get()
    if context is None:
        raise RuntimeError("Paperless submission has no durable operation context")
    if task.name not in STAGES:
        raise RuntimeError(f"Unsupported Paperless continuation task: {task.name}")
    if any(options.get(key) for key in ("link", "link_error", "chain", "chord", "group_id")):
        raise RuntimeError("Paperless canvas continuation requires a declared persistent graph implementation")
    op, parent = context
    identity = str(options.get("task_id") or uuid.uuid4())
    headers = dict(headers or {})
    now = datetime.now(timezone.utc)
    available = eta or now + timedelta(seconds=countdown or 0)
    if isinstance(available, str):
        available = datetime.fromisoformat(available)
    if available.tzinfo is not None:
        available = available.astimezone(timezone.utc).replace(tzinfo=None)
    values = dict(kwargs or {})
    from documents.signals.handlers import before_task_publish_handler
    before_task_publish_handler(sender=task.name, headers={**headers, "id": identity, "task": task.name},
                                body=(tuple(args), values, {}))
    engine = create_engine(os.environ["DBWORKER_DATABASE_URL"])
    try:
        with Session(engine) as session, session.begin():
            session.add(Job(id=identity, operation_id=op, parent_id=parent, task_name=task.name,
                            arguments=encode_arguments(args, values), headers=headers, available_at=available))
            # Publication intent precedes eligibility visibility at commit.
            record(STAGES[task.name], op, identity, "submitted", parent, backend="dbworker", task_name=task.name, argument_sha256=argument_digest(args, values), business_input=business_input(task.name, args, values))
    finally:
        engine.dispose()
    return SimpleNamespace(id=identity)


_routing_lock = threading.Lock()
_routing_users = 0
_routing_originals = None


@contextmanager
def route_tasks(operation_id, parent_id=None):
    """Keep publication routing installed while concurrent contexts are active."""
    global _routing_users, _routing_originals
    from celery.app.task import Task
    from kombu import Producer
    token = _current.set((str(operation_id), parent_id))

    def dispatch(task, args=None, kwargs=None, **options):
        return enqueue(task, args or (), kwargs or {}, **options)

    def reject(*args, **kwargs):
        raise RuntimeError("Unexpected broker publication or eager Celery execution in DBWorker")

    with _routing_lock:
        if _routing_users == 0:
            _routing_originals = (Task.apply_async, Producer.publish, Task.apply)
            Task.apply_async, Producer.publish, Task.apply = dispatch, reject, reject
        _routing_users += 1
    try:
        yield
    finally:
        _current.reset(token)
        with _routing_lock:
            _routing_users -= 1
            if _routing_users == 0:
                Task.apply_async, Producer.publish, Task.apply = _routing_originals
                _routing_originals = None


def handle(job: Job, session: Session):
    initialize()
    identity, op, parent, name = job.id, job.operation_id, job.parent_id, job.task_name
    arguments, headers = job.arguments, dict(job.headers)
    session.rollback()
    from paperless.celery import app
    from django.db import close_old_connections
    from documents.signals.handlers import task_prerun_handler, task_postrun_handler, task_failure_handler
    task = app.tasks[name]
    args, kwargs = decode_arguments(arguments)
    context = SimpleNamespace(name=name, request=SimpleNamespace(id=identity, headers=headers, retries=0))
    close_old_connections()
    task_prerun_handler(task_id=identity, task=context)
    from benchmarks.common.native_observer import worker_origin
    try:
        proof = {"native_worker_origin": worker_origin(task, "paperless_ngx")}
    except Exception as error:
        proof = {"native_worker_origin_error": type(error).__name__ + ": " + str(error)}
    record(STAGES[name], op, identity, "started", parent, backend="dbworker", task_name=name, argument_sha256=argument_digest(args, kwargs), business_input=business_input(name, args, kwargs), **proof)
    try:
        with job_context(op, identity), route_tasks(op, identity):
            body = inspect.unwrap(task.run)
            # Only DBWorker adapts binding. The Celery arm never invokes .run.
            if inspect.ismethod(body):
                result = body.__func__(context, *args, **kwargs)
            else:
                result = body(*args, **kwargs)
        task_postrun_handler(task_id=identity, task=context, retval=result, state="SUCCESS")
        stored = session.get(Job, identity)
        stored.result = {"value": result}
        # Completion becomes observable only after the source and ledger commit.
        event.listen(session, "after_commit", lambda committed: record(
            STAGES[name], op, identity, "succeeded", parent, backend="dbworker", task_name=name), once=True)
        return Finished()
    except Exception as exc:
        task_failure_handler(task_id=identity, sender=context, exception=exc, traceback=exc.__traceback__)
        record(STAGES[name], op, identity, "failed", parent, backend="dbworker", task_name=name)
        raise
    finally:
        close_old_connections()


def coordinator(url, concurrency=2):
    engine = create_engine(url)
    runtime = Coordinator(sessionmaker(engine, expire_on_commit=False), database_url=url,
                          poll_seconds=.05, max_poll_seconds=.25, lease_seconds=300)
    runtime.transactional_worker(name="paperless", source=Job, concurrency=concurrency,
        eligible=lambda: select(Job).where(Job.available_at <= datetime.now(timezone.utc).replace(tzinfo=None)))(handle)
    Base.metadata.create_all(engine)
    return runtime
