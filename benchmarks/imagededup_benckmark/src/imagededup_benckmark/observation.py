"""Observe native task execution without replacing tasks or changing their work."""

from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import closing, contextmanager
from contextvars import ContextVar
from pathlib import Path
from uuid import uuid4

_current_attempt: ContextVar[dict | None] = ContextVar("image_observation_attempt", default=None)
_transaction_observers_installed = False


def _flushed(session, flush_context):
    record = session.info.get("image_observation_attempt") or _current_attempt.get()
    if record is None:
        return
    if record["stage"] == "build":
        from sqlalchemy import inspect

        for item in session.dirty:
            if getattr(getattr(type(item), "__table__", None), "name", None) != "feature_artifact":
                continue
            history = inspect(item).attrs.hash_value.history
            if history.has_changes() and history.added and history.added[-1] is not None:
                session.info["image_observation_attempt"] = record
                session.info.setdefault("image_observation_pending_hash_writes", []).append(
                    {"artifact_id": int(item.id), "matched_rows": 1, "kind": "orm_flush"})
        return
    if record["stage"] != "comparison":
        return
    rows = [item for item in session.new
            if getattr(getattr(type(item), "__table__", None), "name", None) == "scored_candidate"]
    if rows:
        session.info["image_observation_attempt"] = record
        session.info.setdefault("image_observation_pending_rows", []).extend(
            [[int(item.request_id), int(item.candidate_artifact_id)] for item in rows])


def _executed(orm_execute_state):
    """Observe the original bulk hash update and return its unchanged Result."""
    session = orm_execute_state.session
    record = session.info.get("image_observation_attempt") or _current_attempt.get()
    if record is None or record["stage"] != "build" or not orm_execute_state.is_update:
        return None
    statement = orm_execute_state.statement
    if getattr(getattr(statement, "table", None), "name", None) != "feature_artifact":
        return None
    values = getattr(statement, "_values", {}) or {}
    hashes = [value for key, value in values.items() if getattr(key, "key", key) == "hash_value"]
    if not hashes or getattr(hashes[0], "value", None) is None:
        return None
    # Both pinned handlers use Session.execute(update(...).values(hash_value=...)).
    # invoke_statement executes that same statement once; rowcount is inspected
    # without fetching results, replacing arguments, or changing SQL/transactions.
    result = orm_execute_state.invoke_statement()
    session.info["image_observation_attempt"] = record
    session.info.setdefault("image_observation_pending_hash_writes", []).append(
        {"artifact_id": record["source_id"], "matched_rows": result.rowcount, "kind": "bulk_update"})
    return result


def _committed(session):
    rows = session.info.pop("image_observation_pending_rows", [])
    record = session.info.get("image_observation_attempt")
    if rows and record is not None:
        record.setdefault("_committed_scored_rows", []).extend(rows)
    hashes = session.info.pop("image_observation_pending_hash_writes", [])
    if hashes and record is not None:
        record.setdefault("_committed_hash_writes", []).extend(hashes)
    positive_hashes = sum(max(0, write["matched_rows"]) for write in hashes)
    if record is not None and (rows or positive_hashes):
        committed_ns = time.time_ns()
        record["_business_committed_timestamp_ns"] = committed_ns
        receipt = {"attempt_id": record["attempt_id"], "stage": record["stage"],
                   "source_id": record["source_id"], "workspace_id": record["workspace_id"],
                   "timestamp_ns": committed_ns, "page_items": len(rows) + positive_hashes}
        # Compact positive-work receipts avoid reparsing the large candidate trace
        # on every progress probe. No zero-write/retry/drain event advances them.
        path = Path(os.environ["IMAGE_OBSERVATION_FILE"]).with_name("business-commits.jsonl")
        payload = (json.dumps(receipt, sort_keys=True) + "\n").encode()
        descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            if os.write(descriptor, payload) != len(payload):
                raise OSError("Incomplete image business commit receipt")
        finally:
            os.close(descriptor)


def _rolled_back(session):
    session.info.pop("image_observation_pending_rows", None)
    session.info.pop("image_observation_pending_hash_writes", None)


def install_transaction_observers():
    """Attribute ORM inserts to their own successful transaction, without writes."""
    global _transaction_observers_installed
    if not _transaction_observers_installed:
        from sqlalchemy import event
        from sqlalchemy.orm import Session

        event.listen(Session, "after_flush", _flushed)
        event.listen(Session, "do_orm_execute", _executed, retval=True)
        event.listen(Session, "after_commit", _committed)
        event.listen(Session, "after_rollback", _rolled_back)
        _transaction_observers_installed = True


def snapshot(stage: str, source_id: int) -> dict:
    with closing(sqlite3.connect(os.environ["IMAGE_OBSERVATION_DATABASE"], timeout=30)) as connection:
        if stage == "build":
            row = connection.execute("SELECT workspace_id, hash_value FROM feature_artifact WHERE id=?", (source_id,)).fetchone()
            return {"workspace_id": row[0], "items": int(row[1] is not None)}
        row = connection.execute("SELECT workspace_id, candidates_scored_count, query_artifact_id FROM comparison_request WHERE id=?", (source_id,)).fetchone()
        return {"workspace_id": row[0], "items": row[1], "query_artifact_id": row[2]}


def emit(record: dict) -> None:
    public = {key: value for key, value in record.items() if not key.startswith("_")}
    payload = (json.dumps({**public, "timestamp": time.time(), "timestamp_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(), "pid": os.getpid()}, sort_keys=True) + "\n").encode()
    descriptor = os.open(os.environ["IMAGE_OBSERVATION_FILE"], os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        if os.write(descriptor, payload) != len(payload):
            raise OSError("Incomplete image observation record")
    finally:
        os.close(descriptor)


def begin(stage: str, source_id: int | None, *, task_id: str | None = None, details: dict | None = None) -> dict:
    record = {"attempt_id": uuid4().hex, "task_id": task_id, "stage": stage, "source_id": source_id,
              "backend": os.environ["IMAGE_OBSERVATION_BACKEND"]}
    if source_id is not None:
        record.update(snapshot(stage, source_id))
    record.update(details or {})
    emit({**record, "event": "started"})
    return record


def end(record: dict, state: str) -> None:
    observed = snapshot(record["stage"], record["source_id"]) if record["source_id"] is not None else {"items": 0}
    page_items = observed["items"] - record.get("items", 0)
    details = {}
    if record["stage"] == "build":
        writes = record.get("_committed_hash_writes", [])
        built = [row["artifact_id"] for row in writes
                 for _ in range(max(0, row["matched_rows"]))]
        page_items = len(built)
        details = {"built_artifact_ids": built, "build_accounting": "transaction_committed_hash_writes",
                   "hash_write_observations": writes}
    if record["stage"] == "comparison":
        # A native task publishes its continuation before task_postrun. Another
        # worker may commit several pages before this task's signal executes, so
        # differences of shared request counters cannot identify individual work.
        # Count only this attempt's ScoredCandidate inserts after their commit.
        rows = record.get("_committed_scored_rows", [])
        page_items = len(rows)
        details = {"scored_rows": rows, "page_accounting": "transaction_committed_orm_inserts"}
    emit({**record, "event": "finished", "state": state,
          "items_after": observed["items"], "page_items": page_items,
          "business_committed_timestamp_ns": record.get("_business_committed_timestamp_ns"), **details})


@contextmanager
def observe_handler(stage: str, source_id: int, session):
    """Record DBWorker success only after its business and outcome commit."""
    from sqlalchemy import event

    install_transaction_observers()
    record = begin(stage, source_id)
    session.info["image_observation_attempt"] = record
    token = _current_attempt.set(record)
    try:
        yield
    except BaseException:
        end(record, "FAILURE")
        raise
    else:
        def completed(committed):
            _committed(committed)
            end(record, "SUCCESS")

        event.listen(session, "after_commit", completed, once=True)
    finally:
        _current_attempt.reset(token)


def retry_diagnostics(reason) -> dict:
    """Serialize Celery's original retry exception without changing its policy."""
    exception = getattr(reason, "exc", None)
    delay = getattr(reason, "when", None)
    return {"exception_class": (f"{type(exception).__module__}.{type(exception).__qualname__}"
                                if exception is not None else None),
            "exception_message": str(exception) if exception is not None else None,
            "retry_delay": delay if isinstance(delay, (int, float, str)) or delay is None else str(delay)}


if os.environ.get("IMAGE_OBSERVATION_BACKEND") == "celery":
    from celery.signals import task_postrun, task_prerun, task_retry

    _active: dict[str, dict] = {}

    def before_task(sender=None, task_id=None, args=None, **kwargs):
        stage = {"images.build": "build", "images.compare": "comparison", "images.dispatch": "dispatch"}.get(sender.name)
        if stage is not None:
            install_transaction_observers()
            from benchmarks.common.native_observer import worker_origin
            try:
                origin = worker_origin(sender, "imagededup")
            except Exception as exc:
                origin = {"passed": False, "proof_error": str(exc)}
            record = begin(stage, int(args[0]) if stage != "dispatch" else None, task_id=task_id,
                           details={"native_worker_origin": origin,
                                    "source_revision": int(args[1]) if stage != "dispatch" else None,
                                    "parent_task_id": getattr(sender.request, "parent_id", None),
                                    "root_task_id": getattr(sender.request, "root_id", None)})
            _active[task_id] = record
            _current_attempt.set(record)

    def after_task(task_id=None, state=None, **kwargs):
        record = _active.pop(task_id, None)
        if record is not None:
            end(record, state)
        _current_attempt.set(None)

    def retry_task(request=None, reason=None, **kwargs):
        record = _active.get(getattr(request, "id", None))
        if record is not None:
            record["retry_diagnostics"] = retry_diagnostics(reason)

    task_retry.connect(retry_task, weak=False)
    task_prerun.connect(before_task, weak=False)
    task_postrun.connect(after_task, weak=False)


def read_records(path: Path) -> list[dict]:
    if not path.exists():
        return []
    # Exclude only an unfinished final append. Malformed complete records fail.
    complete = path.read_bytes().rsplit(b"\n", 1)[0]
    return [json.loads(line) for line in complete.splitlines()]
