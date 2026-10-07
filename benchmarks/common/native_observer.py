"""Observe original Celery tasks without defining or replacing task bodies.

This module can be loaded with Celery's --include option. Producers import it
before submission. Trace writes are append-only and serialized between processes.
"""
from __future__ import annotations

import contextvars
import json
import os
import time
import threading
from contextlib import contextmanager
from pathlib import Path

_current = contextvars.ContextVar("benchmark_operation", default=None)
_parent = contextvars.ContextVar("benchmark_parent", default=None)
_published_context = {}
_trace_offset = 0
_trace_identity = None
_context_lock = threading.RLock()


@contextmanager
def operation(operation_id):
    token = _current.set(str(operation_id))
    try:
        yield
    finally:
        _current.reset(token)


@contextmanager
def job_context(operation_id, node_id):
    first = _current.set(str(operation_id))
    second = _parent.set(str(node_id))
    try:
        yield
    finally:
        _parent.reset(second)
        _current.reset(first)


def record(stage, operation_id, task_id, event, parent_id=None, *, backend=None, **details):
    path = os.environ.get("BENCHMARK_TRACE_PATH")
    if not path:
        return
    import fcntl
    row = {"schema_version": 1, "backend": backend or os.environ.get("BENCHMARK_BACKEND", "celery"),
           "stage": str(stage), "operation_id": str(operation_id) if operation_id is not None else None,
           "node_id": str(task_id), "parent_id": str(parent_id) if parent_id else None,
           "event": event, "timestamp_ns": time.time_ns(), "details": details}
    payload = (json.dumps(row, sort_keys=True, allow_nan=False) + "\n").encode()
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(destination, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        offset = 0
        while offset < len(payload):
            offset += os.write(descriptor, payload[offset:])
    finally:
        os.close(descriptor)


def _stage(name):
    mapping = json.loads(os.environ.get("BENCHMARK_TASK_STAGES", "{}"))
    return mapping.get(name, "unmapped:" + str(name))


def _publish(sender=None, headers=None, body=None, **kwargs):
    """Record publication intent before transport can hand work to a worker.

    A failed publish leaves an incomplete job, which success admission rejects.
    Protocol 1 bodies and transport headers remain unchanged; correlation uses
    the native task identity and this append-only observation artifact.
    """
    legacy = body if isinstance(body, dict) else {}
    if not legacy and headers is not None:
        if _current.get() is not None:
            headers["benchmark_operation"] = _current.get()
        if _parent.get() is not None:
            headers["benchmark_parent"] = _parent.get()
    headers = headers or {}
    node_id = headers.get("id") or legacy.get("id")
    op = headers.get("benchmark_operation") or _current.get()
    parent = headers.get("benchmark_parent") or headers.get("parent_id") or legacy.get("parent_id") or _parent.get()
    record(_stage(sender), op, node_id, "submitted", parent, task_name=sender)


def _context(node_id):
    global _trace_offset, _trace_identity
    import fcntl
    path = os.environ.get("BENCHMARK_TRACE_PATH")
    if not path or not Path(path).exists():
        return (None, None)
    with _context_lock, open(path, "rb") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_SH)
        state = os.fstat(stream.fileno())
        identity = (str(Path(path).resolve()), state.st_dev, state.st_ino)
        if identity != _trace_identity or state.st_size < _trace_offset:
            _published_context.clear()
            _trace_offset = 0
            _trace_identity = identity
        stream.seek(_trace_offset)
        while True:
            position = stream.tell()
            line = stream.readline()
            if not line:
                _trace_offset = position
                break
            if not line.endswith(b"\n"):
                raise ValueError("Incomplete native observation trace write")
            row = json.loads(line)
            if not isinstance(row, dict):
                raise ValueError("Native observation trace record must be an object")
            if row.get("event") == "submitted":
                _published_context[row["node_id"]] = (row.get("operation_id"), row.get("parent_id"))
    return _published_context.get(str(node_id), (None, None))


def _started(sender=None, task_id=None, task=None, **kwargs):
    task = task or sender
    headers = getattr(task.request, "headers", None) or {}
    observed_op, observed_parent = _context(task_id)
    op = headers.get("benchmark_operation") or observed_op
    _current.set(op)
    _parent.set(task_id)
    record(_stage(task.name), op, task_id, "started",
           headers.get("benchmark_parent") or getattr(task.request, "parent_id", None) or observed_parent, task_name=task.name)


def _finished(sender=None, task_id=None, task=None, state=None, **kwargs):
    task = task or sender
    headers = getattr(task.request, "headers", None) or {}
    event = {"SUCCESS": "succeeded", "FAILURE": "failed", "RETRY": "retried", "REVOKED": "revoked"}.get(state, "unknown")
    observed_op, observed_parent = _context(task_id)
    record(_stage(task.name), headers.get("benchmark_operation") or observed_op, task_id, event,
           headers.get("benchmark_parent") or getattr(task.request, "parent_id", None) or observed_parent, task_name=task.name, state=state)
    _current.set(None)
    _parent.set(None)


def install():
    from celery import signals
    signals.before_task_publish.connect(_publish, weak=False, dispatch_uid="benchmark-native-before")
    # Remove the old after-publish receiver if install() is called in an already
    # instrumented process; publication must produce exactly one submitted row.
    signals.after_task_publish.disconnect(dispatch_uid="benchmark-native-published")
    signals.task_prerun.connect(_started, weak=False, dispatch_uid="benchmark-native-started")
    signals.task_postrun.connect(_finished, weak=False, dispatch_uid="benchmark-native-finished")


try:
    install()
except ImportError:
    # The orchestration environment need not install Celery.
    pass
