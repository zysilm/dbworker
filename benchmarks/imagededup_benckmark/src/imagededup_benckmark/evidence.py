"""Check observed individual builds and bounded scoring pages by source identity."""

import hashlib
import json
import math
import sqlite3
import time


def quiesce(stack, *, timeout: float) -> list[dict]:
    from imagededup_benckmark.observation import read_records
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        stack.check_alive()
        records = read_records(stack.observation_file)
        opened = {row["attempt_id"] for row in records if row["event"] == "started" and row["stage"] != "dispatch"}
        closed = {row["attempt_id"] for row in records if row["event"] == "finished" and row["stage"] != "dispatch"}
        if opened == closed:
            try:
                snapshot = stack.business_idle_snapshot()
            except sqlite3.OperationalError:
                # The worker service may still be registering its durable tables.
                time.sleep(.1)
                continue
            final_records = read_records(stack.observation_file)
            final_opened = {row["attempt_id"] for row in final_records if row["event"] == "started" and row["stage"] != "dispatch"}
            final_closed = {row["attempt_id"] for row in final_records if row["event"] == "finished" and row["stage"] != "dispatch"}
            if snapshot["idle"] and final_opened == final_closed and final_opened == opened:
                return [*final_records, {"event": "quiescence_barrier", "stage": "barrier",
                                        "timestamp": time.time(), "workspace_id": None,
                                        "observed_attempt_count": len(final_opened), **snapshot}]
        time.sleep(.1)
    raise TimeoutError("Image business task quiescence was not observed")


def verify(records: list[dict], *, workspace: int, artifact_ids: list[int], request_ids: list[int],
           new_builds: bool, page_size: int, record_offset: int) -> dict:
    selected = [row for row in records[record_offset:] if row.get("workspace_id") == workspace]
    start_rows = [row for row in selected if row["event"] == "started"]
    starts = {row["attempt_id"]: row for row in start_rows}
    if len(starts) != len(start_rows):
        raise AssertionError("Duplicate image attempt start evidence")
    finishes = [row for row in selected if row["event"] == "finished"]
    if len(finishes) != len(starts) or any(row["attempt_id"] not in starts for row in finishes):
        raise AssertionError("Missing or duplicate image attempt completion evidence")
    for row in finishes:
        start = starts[row["attempt_id"]]
        if any(row.get(key) != start.get(key) for key in ("stage", "source_id", "workspace_id", "backend", "task_id", "source_revision", "query_artifact_id", "parent_task_id", "root_task_id")):
            raise AssertionError("Image attempt identity changed")
    if len({row["attempt_id"] for row in finishes}) != len(finishes):
        raise AssertionError("Duplicate image attempt completion evidence")
    recovered_retries = []
    for row in finishes:
        if row["state"] == "SUCCESS":
            continue
        diagnostics = row.get("retry_diagnostics", {})
        if (row["state"] != "RETRY" or row.get("backend") != "celery"
                or row.get("stage") not in ("build", "comparison")
                or not row.get("task_id")
                or not isinstance(diagnostics, dict)
                or diagnostics.get("exception_class") != "sqlalchemy.exc.OperationalError"
                or not isinstance(diagnostics.get("exception_message"), str)
                or not diagnostics["exception_message"]
                or row.get("page_items") != 0
                or row.get("built_artifact_ids", []) != []
                or row.get("scored_rows", []) != []
                or any(write.get("matched_rows", 0) > 0 for write in row.get("hash_write_observations", []))):
            raise AssertionError("Image experiment observed an unproven failure or retry")
        retry_finished = row.get("timestamp")
        successes = [candidate for candidate in finishes
                     if candidate["state"] == "SUCCESS"
                     and candidate["attempt_id"] != row["attempt_id"]
                     and all(candidate.get(key) == row.get(key) for key in
                             ("task_id", "stage", "source_id", "workspace_id", "backend"))]
        if not isinstance(retry_finished, (int, float)) or not any(
                isinstance(starts[candidate["attempt_id"]].get("timestamp"), (int, float))
                and starts[candidate["attempt_id"]]["timestamp"] > retry_finished
                and isinstance(candidate.get("timestamp"), (int, float))
                and candidate["timestamp"] >= starts[candidate["attempt_id"]]["timestamp"]
                for candidate in successes):
            raise AssertionError("Native OperationalError retry lacks subsequent same-task recovery")
        recovered_retries.append(row)
    expected_builds = artifact_ids if new_builds else []
    actual_builds = [row["source_id"] for row in finishes if row["stage"] == "build" and row["page_items"] == 1]
    if sorted(actual_builds) != sorted(expected_builds):
        raise AssertionError("Individual image build work did not match submitted identities")
    build_rows = [row for row in finishes if row["stage"] == "build"]
    if any(row["source_id"] not in expected_builds or row["page_items"] not in (0, 1) for row in build_rows):
        raise AssertionError("Unexpected or oversized image build execution")
    for row in build_rows:
        expected = [row["source_id"]] if row["page_items"] else []
        if row.get("build_accounting") != "transaction_committed_hash_writes" or row.get("built_artifact_ids") != expected:
            raise AssertionError("Individual image build lacks transaction-attributed hash write evidence")
    pages = {key: [] for key in request_ids}
    scored = {key: [] for key in request_ids}
    artifact_set = set(artifact_ids)
    # The benchmark submits one comparison per imported artifact in this order.
    query_artifacts = dict(zip(request_ids, artifact_ids))
    if len(pages) != len(request_ids) or len(query_artifacts) != len(request_ids):
        raise AssertionError("Comparison input identities are duplicate or unpaired")
    strict_dependencies = any(row.get("event") == "quiescence_barrier" for row in records[record_offset:])
    positive_revisions = set()
    empty = 0
    for row in finishes:
        if row["stage"] != "comparison":
            continue
        key, width = row["source_id"], row["page_items"]
        if key not in pages or not 0 <= width <= page_size:
            raise AssertionError(f"Unknown comparison identity or oversized scoring page: request={key}, observed={width}, bound={page_size}")
        if strict_dependencies:
            if row.get("query_artifact_id") != query_artifacts[key]:
                raise AssertionError("Comparison page changed the submitted query dependency")
            if row.get("backend") == "celery":
                revision = row.get("source_revision")
                if type(revision) is not int or revision < 0:
                    raise AssertionError("Native page lacks original source revision")
                identity = (key, revision)
                if width and identity in positive_revisions:
                    raise AssertionError("Duplicate successful native comparison revision")
                if width:
                    positive_revisions.add(identity)
        rows = row.get("scored_rows")
        if row.get("page_accounting") != "transaction_committed_orm_inserts" or not isinstance(rows, list) or len(rows) != width:
            raise AssertionError("Individual scoring page lacks transaction-attributed work evidence")
        for pair in rows:
            if not isinstance(pair, list) or len(pair) != 2 or pair[0] != key or pair[1] not in artifact_set:
                raise AssertionError("Scored candidate has unknown input or comparison identity")
            scored[key].append(pair[1])
        pages[key].append(width)
        empty += int(width == 0)
    if any(sum(widths) != len(artifact_ids) - 1 for widths in pages.values()):
        raise AssertionError("Observed scoring work skipped or duplicated candidate pairs")
    if any(len(set(candidates)) != len(candidates) for candidates in scored.values()):
        raise AssertionError("Observed scoring work duplicated candidate identities")
    if any(set(candidates) != artifact_set - {query_artifacts[key]} for key, candidates in scored.items()):
        raise AssertionError("Observed scoring work differs from the exact submitted candidate identities")
    comparisons = [{"input_ordinal": index, "page_sizes": pages[key], "scored_pairs": sum(pages[key])}
                   for index, key in enumerate(request_ids)]
    normalized = {"build_inputs": list(range(len(expected_builds))),
                  "comparison_inputs": list(range(len(request_ids))),
                  "scored_pairs": len(request_ids) * (len(artifact_ids) - 1), "page_bound": page_size}
    return {
        "passed": True, "scope": "successful_business_work", "submitted_builds": len(expected_builds),
        "completed_builds": len(actual_builds), "submitted_comparisons": len(request_ids),
        "completed_comparisons": len(comparisons), "scored_pairs": normalized["scored_pairs"],
        "maximum_page_items": max((width for widths in pages.values() for width in widths), default=0),
        "page_bound": page_size, "comparisons": comparisons,
        "business_attempts": len(finishes), "recovered_retry_attempts": len(recovered_retries),
        "empty_comparison_attempts": empty,
        "duplicate_build_deliveries": len(build_rows) - len(actual_builds),
        "build_hash_update_attempts": sum(len(row.get("hash_write_observations", [])) for row in build_rows),
        "zero_match_hash_update_attempts": sum(write.get("matched_rows") == 0 for row in build_rows
                                               for write in row.get("hash_write_observations", [])),
        "control_dispatch_attempts": sum(row["event"] == "finished" and row["stage"] == "dispatch"
                                         for row in records[record_offset:]),
        "failed_attempts": 0, "missing_attempts": 0,
        "workload_digest": hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest(),
        "quiescence_verified": validate_quiescence(records[record_offset:]),
        "dependency_scope": "per_request_exact_candidates_and_bounded_pages",
        "identical_execution_attempt_graph": False,
    }


def validate_quiescence(records: list[dict]) -> bool:
    """Replay actual live receipts; closed task traces alone cannot prove idle."""
    receipts = [row for row in records if row.get("event") == "quiescence_barrier"]
    if not receipts:
        return False  # Historical traces have no broker/durable-work receipt.
    row = receipts[-1]
    def positive_time(value):
        return type(value) in (float, int) and math.isfinite(value) and value > 0
    if (row.get("idle") is not True or row.get("stage") != "barrier"
            or not positive_time(row.get("observed_at")) or not positive_time(row.get("timestamp"))
            or row["observed_at"] > row["timestamp"]):
        raise AssertionError("Invalid image idle receipt")
    finishes = [record for record in records if record.get("event") == "finished" and record.get("stage") != "dispatch"]
    if any(record.get("timestamp", 0) > row["timestamp"] for record in finishes):
        raise AssertionError("Image work finishes after quiescence barrier")
    if row.get("backend") == "dbworker":
        counts = row.get("unfinished_work")
        if not isinstance(counts, dict) or set(counts) != {"artifact_build_work", "comparison_work"} or any(
                type(value) is not int or value != 0 for value in counts.values()):
            raise AssertionError("DBWorker durable work is not proven idle")
    elif row.get("backend") == "celery":
        states, lanes = row.get("worker_states"), row.get("redis_lanes")
        if row.get("worker_responses_complete") is not True or type(row.get("pending_business_outbox")) is not int or row["pending_business_outbox"] != 0:
            raise AssertionError("Celery worker or outbox receipt is incomplete")
        if not isinstance(states, dict) or set(states) != {"active", "reserved", "scheduled"}:
            raise AssertionError("Missing Celery inspection states")
        workers = [set(state) for state in states.values() if isinstance(state, dict)]
        if len(workers) != 3 or len(workers[0]) != 2 or any(worker != workers[0] for worker in workers):
            raise AssertionError("Incomplete Celery worker responses")
        if any(type(count) is not int or count != 0 for state in states.values() for count in state.values()):
            raise AssertionError("Celery business tasks remain in workers")
        if not isinstance(lanes, list) or not lanes or {lane.get("queue") for lane in lanes} != {"image_build", "image_compare"}:
            raise AssertionError("Missing native Redis priority lanes")
        if row.get("redis_priority_steps") != [0, 3, 6, 9] or any(type(lane.get("priority")) is not int for lane in lanes):
            raise AssertionError("Redis receipt omits original configured priority lanes")
        priorities = [{lane["priority"] for lane in lanes if lane["queue"] == queue} for queue in ("image_build", "image_compare")]
        if priorities[0] != set(row.get("redis_priority_steps", [])) or priorities[0] != priorities[1] or not priorities[0] or len(lanes) != 2 * len(priorities[0]) or any(
                type(lane.get("messages")) is not int or lane["messages"] != 0 for lane in lanes):
            raise AssertionError("Redis lanes are incomplete or not empty")
    else:
        raise AssertionError("Unknown image barrier backend")
    return True
