"""Persist one native historical Sentry email envelope per DBWorker delivery."""

from __future__ import annotations

import pickle
import sys
from contextlib import contextmanager

from sqlalchemy import LargeBinary, String, create_engine, event, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from dbworker import Coordinator, Finished
from benchmarks.common.native_observer import job_context, record, worker_origin
from benchmarks.common.argument_evidence import argument_digest


class Base(DeclarativeBase):
    pass


class Delivery(Base):
    __tablename__ = "sentry_email_delivery"
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    operation_id: Mapped[str] = mapped_column(String(100), index=True)
    task_name: Mapped[str] = mapped_column(String(100))
    recipient: Mapped[str] = mapped_column(String(250))
    envelope: Mapped[bytes] = mapped_column(LargeBinary)


_initialized = False


def initialize():
    """Configure the pristine application once in each persistent process."""
    global _initialized
    if _initialized:
        return
    if sys.version_info < (3, 12):
        raise RuntimeError("DBWorker's supported runtime requires Python >=3.12")
    from django.apps import apps
    if not apps.ready:
        from sentry.runner import configure
        configure(skip_service_validation=True)
    import sentry.tasks.email
    _initialized = True


def execute(delivery: Delivery, session):
    initialize()
    from sentry.celery import app
    envelope = pickle.loads(delivery.envelope)
    if envelope.get("task") != delivery.task_name or envelope.get("id") != delivery.id:
        raise ValueError("Durable native email envelope identity changed")
    kwargs = envelope.get("kwargs", {})
    message = kwargs.get("message")
    if message is None or list(message.to) != [delivery.recipient]:
        raise ValueError("A delivery must contain exactly one distinct recipient message")
    task = app.tasks[delivery.task_name]
    record("delivery", delivery.operation_id, delivery.id, "started", backend="dbworker",
           task_name=task.name, native_worker_origin=worker_origin(task, "sentry"),
           argument_sha256=argument_digest(envelope.get("args", ()), kwargs))
    try:
        with job_context(delivery.operation_id, delivery.id):
            task.run(*envelope.get("args", ()), **kwargs)
    except BaseException as exc:
        record("delivery", delivery.operation_id, delivery.id, "failed", backend="dbworker", error=type(exc).__name__)
        raise
    operation_id, node_id = delivery.operation_id, delivery.id
    event.listen(session, "after_commit", lambda committed: record(
        "delivery", operation_id, node_id, "succeeded", backend="dbworker"), once=True)
    return Finished()


def coordinator(database_url, *, concurrency=2):
    engine = create_engine(database_url)
    sessions = sessionmaker(engine, expire_on_commit=False)
    Base.metadata.create_all(engine)
    runtime = Coordinator(sessions, database_url=database_url, poll_seconds=.05, max_poll_seconds=.05)
    runtime.transactional_worker(name="sentry_delivery", source=Delivery,
                                 eligible=lambda: select(Delivery).order_by(Delivery.id),
                                 concurrency=concurrency)(execute)
    runtime.create_worker_tables()
    return runtime, engine, sessions


@contextmanager
def publication_to_dbworker(sessions):
    """Replace transport publication, preserving native producer and job stages."""
    from kombu import Producer
    original = Producer.publish
    published, errors = [], []

    def persist(producer, body, *args, **kwargs):
        try:
            if not isinstance(body, dict) or body.get("task") not in (
                    "sentry.tasks.email.send_email", "sentry.tasks.email.send_email_control"):
                raise ValueError("Unexpected publication outside the declared native email workflow")
            if kwargs.get("serializer") != "pickle":
                raise ValueError("Native Sentry email serializer must remain pickle")
            message = body.get("kwargs", {}).get("message")
            if message is None or len(message.to) != 1 or not message.to[0]:
                raise ValueError("Native delivery granularity must be one recipient message")
            operation_id = str(message.extra_headers["X-Benchmark"])
            node_id = str(body.get("id") or "")
            if not node_id:
                raise ValueError("Missing native protocol-1 task identity")
            with sessions.begin() as session:
                session.add(Delivery(id=node_id, operation_id=operation_id, task_name=body["task"],
                                     recipient=message.to[0], envelope=pickle.dumps(body, protocol=pickle.HIGHEST_PROTOCOL)))
            published.append((operation_id, message.to[0], node_id))
        except BaseException as exc:
            errors.append(exc)
            raise

    Producer.publish = persist
    try:
        yield published, errors
    finally:
        Producer.publish = original
