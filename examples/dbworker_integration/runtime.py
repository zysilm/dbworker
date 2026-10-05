"""Run a registered application boundary from an immutable durable request."""

from __future__ import annotations

import importlib
import os
import sys
from contextlib import contextmanager
from functools import cache
from typing import Any

from sqlalchemy import JSON, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from dbworker import Coordinator, Finished

ADAPTERS = {name: f"examples.{name}_dbworker.adapter" for name in
            ("superset", "saleor", "paperless_ngx", "posthog", "sentry")}


class Base(DeclarativeBase):
    pass


class Request(Base):
    __tablename__ = "integration_request"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    suite: Mapped[str] = mapped_column(String(40))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


class UnexpectedTaskDispatch(RuntimeError):
    pass


@contextmanager
def forbid_task_dispatch():
    """Reject nested broker publication and eager Celery execution in a handler.

    A handler process executes one business operation at a time. This guard is
    process-local and deliberately does not modify the upstream checkout.
    """
    from celery.app.task import Task
    from kombu import Producer

    def reject(*args, **kwargs):
        raise UnexpectedTaskDispatch("The DBWorker operation attempted Celery publication or eager execution")

    publish, apply = Producer.publish, Task.apply
    Producer.publish, Task.apply = reject, reject
    try:
        yield
    finally:
        Producer.publish, Task.apply = publish, apply


@cache
def adapter(suite: str):
    module = importlib.import_module(ADAPTERS[suite])
    module.initialize()
    return module


def execute(suite: str, payload: dict[str, Any], *, guarded: bool = True) -> dict[str, Any]:
    if suite not in ADAPTERS:
        raise ValueError(f"Unknown integration: {suite}")
    def run():
        module = adapter(suite)
        django_apps = sys.modules.get("django.apps")
        connections = None
        if django_apps is not None and django_apps.apps.ready:
            from django.db import close_old_connections
            connections = close_old_connections
            connections()
        try:
            return module.execute(payload)
        finally:
            if connections is not None:
                connections()
    if guarded:
        with forbid_task_dispatch():
            return run()
    return run()


def handle(request: Request, session: Session) -> Finished:
    identity, suite, payload = request.id, request.suite, dict(request.payload)
    # The upstream ORM owns its transaction. Release the coordinator's read
    # transaction before calling it, then save only our result in this session.
    session.rollback()
    result = execute(suite, payload)
    stored = session.get(Request, identity)
    if stored is None:
        raise RuntimeError("The durable request disappeared")
    stored.result = result
    return Finished()


def coordinator(database_url: str, *, concurrency: int = 2) -> Coordinator:
    engine = create_engine(database_url)
    runtime = Coordinator(sessionmaker(engine, expire_on_commit=False), database_url=database_url,
                          poll_seconds=.05, max_poll_seconds=.25, lease_seconds=300)
    runtime.transactional_worker(name="integration", source=Request, concurrency=concurrency)(handle)
    Base.metadata.create_all(engine)
    return runtime


def main() -> None:
    import signal
    import threading

    runtime = coordinator(os.environ["DBWORKER_DATABASE_URL"], concurrency=int(os.environ.get("DBWORKER_CONCURRENCY", "2")))
    stopped = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stopped.set())
    runtime.start()
    try:
        stopped.wait()
    finally:
        runtime.stop()


if __name__ == "__main__":
    main()
