"""Check observed individual builds and bounded scoring pages by source identity."""

import hashlib
import json
import time


def quiesce(stack, *, timeout: float) -> list[dict]:
    from imagededup_benckmark.observation import read_records
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        stack.check_alive()
        records = read_records(stack.observation_file)
        opened = {row["attempt_id"] for row in records if row["event"] == "started" and row["stage"] != "dispatch"}
        closed = {row["attempt_id"] for row in records if row["event"] == "finished" and row["stage"] != "dispatch"}
        if opened == closed and stack.native_business_idle():
            return read_records(stack.observation_file)
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
        if any(row.get(key) != start.get(key) for key in ("stage", "source_id", "workspace_id", "backend")):
            raise AssertionError("Image attempt identity changed")
    if any(row["state"] != "SUCCESS" for row in finishes):
        raise AssertionError("Success-only image experiment observed a failure or retry")
    expected_builds = artifact_ids if new_builds else []
    actual_builds = [row["source_id"] for row in finishes if row["stage"] == "build" and row["page_items"] == 1]
    if sorted(actual_builds) != sorted(expected_builds):
        raise AssertionError("Individual image build work did not match submitted identities")
    build_rows = [row for row in finishes if row["stage"] == "build"]
    if any(row["source_id"] not in expected_builds or row["page_items"] not in (0, 1) for row in build_rows):
        raise AssertionError("Unexpected or oversized image build execution")
    pages = {key: [] for key in request_ids}
    scored = {key: [] for key in request_ids}
    artifact_set = set(artifact_ids)
    # The benchmark submits one comparison per imported artifact in this order.
    query_artifacts = dict(zip(request_ids, artifact_ids))
    if len(pages) != len(request_ids) or len(query_artifacts) != len(request_ids):
        raise AssertionError("Comparison input identities are duplicate or unpaired")
    empty = 0
    for row in finishes:
        if row["stage"] != "comparison":
            continue
        key, width = row["source_id"], row["page_items"]
        if key not in pages or not 0 <= width <= page_size:
            raise AssertionError(f"Unknown comparison identity or oversized scoring page: request={key}, observed={width}, bound={page_size}")
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
        "business_attempts": len(finishes), "empty_comparison_attempts": empty,
        "duplicate_build_deliveries": len(build_rows) - len(actual_builds),
        "control_dispatch_attempts": sum(row["event"] == "finished" and row["stage"] == "dispatch"
                                         for row in records[record_offset:]),
        "failed_attempts": 0, "missing_attempts": 0,
        "workload_digest": hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest(),
        "quiescence_verified": True,
    }
