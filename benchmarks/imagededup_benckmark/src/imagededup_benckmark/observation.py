"""Observe native task execution without replacing tasks or changing their work."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4


def snapshot(stage: str, source_id: int) -> dict:
    with sqlite3.connect(os.environ["IMAGE_OBSERVATION_DATABASE"], timeout=30) as connection:
        if stage == "build":
            row = connection.execute("SELECT workspace_id, hash_value FROM feature_artifact WHERE id=?", (source_id,)).fetchone()
            return {"workspace_id": row[0], "items": int(row[1] is not None)}
        row = connection.execute("SELECT workspace_id, candidates_scored_count FROM comparison_request WHERE id=?", (source_id,)).fetchone()
        return {"workspace_id": row[0], "items": row[1]}


def emit(record: dict) -> None:
    payload = (json.dumps({**record, "timestamp": time.time(), "pid": os.getpid()}, sort_keys=True) + "\n").encode()
    descriptor = os.open(os.environ["IMAGE_OBSERVATION_FILE"], os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        if os.write(descriptor, payload) != len(payload):
            raise OSError("Incomplete image observation record")
    finally:
        os.close(descriptor)


def begin(stage: str, source_id: int | None, *, task_id: str | None = None) -> dict:
    record = {"attempt_id": uuid4().hex, "task_id": task_id, "stage": stage, "source_id": source_id,
              "backend": os.environ["IMAGE_OBSERVATION_BACKEND"]}
    if source_id is not None:
        record.update(snapshot(stage, source_id))
    emit({**record, "event": "started"})
    return record


def end(record: dict, state: str) -> None:
    observed = snapshot(record["stage"], record["source_id"]) if record["source_id"] is not None else {"items": 0}
    emit({**record, "event": "finished", "state": state,
          "items_after": observed["items"], "page_items": observed["items"] - record.get("items", 0)})


@contextmanager
def observe_handler(stage: str, source_id: int, session):
    """Record DBWorker success only after its business and outcome commit."""
    from sqlalchemy import event

    record = begin(stage, source_id)
    try:
        yield
    except BaseException:
        end(record, "FAILURE")
        raise
    else:
        event.listen(session, "after_commit", lambda committed: end(record, "SUCCESS"), once=True)


if os.environ.get("IMAGE_OBSERVATION_BACKEND") == "celery":
    from celery.signals import task_postrun, task_prerun

    _active: dict[str, dict] = {}

    def before_task(sender=None, task_id=None, args=None, **kwargs):
        stage = {"images.build": "build", "images.compare": "comparison", "images.dispatch": "dispatch"}.get(sender.name)
        if stage is not None:
            _active[task_id] = begin(stage, int(args[0]) if stage != "dispatch" else None, task_id=task_id)

    def after_task(task_id=None, state=None, **kwargs):
        record = _active.pop(task_id, None)
        if record is not None:
            end(record, state)

    task_prerun.connect(before_task, weak=False)
    task_postrun.connect(after_task, weak=False)


def read_records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    # Exclude only an unfinished final append. Malformed complete records fail.
    complete = path.read_bytes().rsplit(b"\n", 1)[0]
    return [json.loads(line) for line in complete.splitlines()]
