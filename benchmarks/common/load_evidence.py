"""Reconstruct task load from admitted native lifecycle events, without probes."""

from collections import defaultdict
import math


def distribution(values):
    """Nearest-rank percentiles of observed durations, in seconds."""
    values = sorted(values)
    if not values:
        return None
    return {"count": len(values), "p50_seconds": values[math.ceil(len(values) * .50) - 1],
            "p95_seconds": values[math.ceil(len(values) * .95) - 1],
            "p99_seconds": values[math.ceil(len(values) * .99) - 1],
            "max_seconds": values[-1]}


def task_load_metrics(events, operations):
    """Describe one fixed load point; this cannot locate maximum capacity.

    Queue waiting includes reservation and dispatch. Outstanding counts only
    already published tasks, not future children or scheduled producer calls.
    Native graph and timing admission must validate these events separately.
    """
    wanted = set(operations)
    tasks = defaultdict(dict)
    for event in events:
        if event.get("operation_id") in wanted and event.get("event") in ("submitted", "started", "succeeded"):
            phase = event["event"]
            node = event["node_id"]
            if phase in tasks[node]:
                raise ValueError("Duplicate lifecycle event in load evidence")
            tasks[node][phase] = event["timestamp_ns"]
    if not tasks:
        raise ValueError("Missing task load evidence")
    for phases in tasks.values():
        if set(phases) != {"submitted", "started", "succeeded"} or not phases["submitted"] <= phases["started"] <= phases["succeeded"]:
            raise ValueError("Incomplete or unordered task load evidence")
    origin = min(p["submitted"] for p in tasks.values())
    changes = sorted(((timestamp, phase) for p in tasks.values() for phase, timestamp in p.items()),
                     key=lambda item: (item[0], {"submitted": 0, "started": 1, "succeeded": 2}[item[1]]))
    counts = dict(submitted=0, started=0, succeeded=0)
    series = []
    cursor = 0
    last = max(p["succeeded"] for p in tasks.values())
    peak = 0
    for second in range(math.ceil((last - origin) / 1e9) + 1):
        boundary = min(origin + (second + 1) * 1_000_000_000, last)
        previous = counts.copy()
        while cursor < len(changes) and changes[cursor][0] <= boundary:
            counts[changes[cursor][1]] += 1
            peak = max(peak, counts["submitted"] - counts["succeeded"])
            cursor += 1
        series.append({"elapsed_seconds": (boundary - origin) / 1e9,
                       **{phase + "_in_interval": counts[phase] - previous[phase] for phase in counts},
                       "published_waiting": counts["submitted"] - counts["started"],
                       "running": counts["started"] - counts["succeeded"],
                       "published_outstanding": counts["submitted"] - counts["succeeded"]})
        if boundary == last:
            break
    submission_end = max(p["submitted"] for p in tasks.values())
    return {"scope": "one_fixed_load_point_not_maximum_capacity", "task_count": len(tasks),
            "queue_wait": distribution([(p["started"] - p["submitted"]) / 1e9 for p in tasks.values()]),
            "task_execution": distribution([(p["succeeded"] - p["started"]) / 1e9 for p in tasks.values()]),
            "task_end_to_end": distribution([(p["succeeded"] - p["submitted"]) / 1e9 for p in tasks.values()]),
            "peak_published_outstanding": peak,
            "seconds_after_last_task_publication": (last - submission_end) / 1e9,
            "timeline": series,
            "contention_scope": "Concurrent native calls exercise connection and lock contention; database lock wait time is not directly measured."}


def validate_fixed_load(row, profile, *, image=False):
    """Reject omitted requests, changed schedules and reduced producer budgets."""
    if "producers" not in profile:
        return  # Historical contracts did not require producer receipts.
    load = row.get("metrics", {}).get("load", {})
    if image and row["scenario"] == "build":
        expected_bulk = {"mode": "native_bulk_import", "producer_count": 1,
                         "business_requests": 1, "imported_images": profile["images"],
                         "sustained_submission_tested": False}
        for name, value in expected_bulk.items():
            if load.get(name) != value or type(load.get(name)) is not type(value):
                raise ValueError("Image build changed its native bulk submission: " + name)
        return
    count = profile["images" if image else "requests"]
    producers = min(profile["producers"], count)
    duration = profile["submission_window_seconds"]
    expected = {"schema_version": 1, "mode": "fixed_open_loop_waves", "producers": producers,
                "operations": count, "submitted_operations": count, "dropped_operations": 0,
                "duration_seconds": duration, "waves": math.ceil(count / producers)}
    for name, value in expected.items():
        if load.get(name) != value or isinstance(load.get(name), bool):
            raise ValueError("Fixed producer profile differs: " + name)
    records = load.get("submissions", [])
    if len(records) != count:
        raise ValueError("Missing producer submission receipts")
    epoch = load.get("schedule_epoch_timestamp_ns")
    if type(epoch) is not int or epoch <= 0:
        raise ValueError("Missing producer schedule epoch")
    from benchmarks.common.timing_evidence import _window_values, CLOCK_COHERENCE_NS, CLOCK_SLEW_PPM
    begin, finish, _ = _window_values(row.get("measurement_window"))
    lower = begin["timestamp_ns"] - begin["uncertainty_ns"]
    upper = finish["timestamp_ns"] + finish["uncertainty_ns"]
    if not lower <= epoch <= upper:
        raise ValueError("Producer schedule epoch is outside the measured window")
    def finite_number(value):
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    for index, record in enumerate(records):
        offset = (index // producers) * duration / expected["waves"]
        planned_offset = record.get("planned_offset_seconds")
        if (type(record.get("index")) is not int or record["index"] != index
                or type(record.get("producer_index")) is not int or record["producer_index"] != index % producers
                or record.get("status") != "submitted"
                or not finite_number(planned_offset) or abs(planned_offset - offset) > 1e-9
                or type(record.get("planned_timestamp_ns")) is not int
                or record["planned_timestamp_ns"] != epoch + int(offset * 1e9)):
            raise ValueError("Producer schedule omitted or reassigned work")
        start, end = record.get("started_timestamp_ns"), record.get("finished_timestamp_ns")
        if type(start) is not int or type(end) is not int or end < start:
            raise ValueError("Invalid producer receipt timestamps")
        clock_allowance = CLOCK_COHERENCE_NS + int((end - epoch) * CLOCK_SLEW_PPM / 1_000_000)
        if start < record["planned_timestamp_ns"] - clock_allowance:
            raise ValueError("Producer started before its fixed scheduled arrival")
        if start < lower - clock_allowance or end > upper + clock_allowance:
            raise ValueError("Producer receipt is outside the measured window")
        for name in ("submit_seconds", "lateness_seconds"):
            if not finite_number(record.get(name)) or record[name] < 0:
                raise ValueError("Invalid producer receipt duration: " + name)
        if abs(record["submit_seconds"] - (end - start) / 1e9) > 1e-9:
            raise ValueError("Submission latency disagrees with receipt timestamps")
    wall = load.get("submission_wall_seconds")
    if not finite_number(wall) or wall < duration:
        raise ValueError("Producer measurement omitted the sustained window")
    if epoch + int(wall * 1e9) > upper + CLOCK_COHERENCE_NS + int(wall * 1e9 * CLOCK_SLEW_PPM / 1_000_000):
        raise ValueError("Producer duration exceeds the measured window")
