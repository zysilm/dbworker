"""Measured boundaries tying monotonic duration to cross-process Unix timestamps.

All workers in one arm share the runner's host clock. The midpoint of two Unix
clock reads brackets a monotonic read; its explicit uncertainty is retained.
The coherence allowance is 1 ms plus 100 ppm of the interval, accommodating
ordinary clock slewing while rejecting clock jumps. Event coverage itself has
no fixed slack: only the captured clock-read uncertainty is allowed.
"""
from __future__ import annotations

import math
import time

from benchmarks.common.workflow_graph import WorkflowMismatch

CLOCK_DOMAIN = "unix_time_ns"
CLOCK_SLEW_PPM = 100
CLOCK_COHERENCE_NS = 1_000_000


def _boundary():
    before = time.time_ns()
    monotonic = time.monotonic_ns()
    after = time.time_ns()
    if after < before:
        raise WorkflowMismatch("Wall clock moved backwards during boundary capture")
    return {"timestamp_ns": (before + after) // 2, "monotonic_ns": monotonic,
            "uncertainty_ns": (after - before + 1) // 2}


def begin_window():
    """Call immediately before the first measured business submission."""
    return _boundary()


def end_window(start):
    """Call after submission returns and all required business effects complete."""
    return {"schema_version": 1, "clock_domain": CLOCK_DOMAIN,
            "start": dict(start), "end": _boundary()}


def _window_values(window):
    if (not isinstance(window, dict) or type(window.get("schema_version")) is not int
            or window.get("schema_version") != 1 or window.get("clock_domain") != CLOCK_DOMAIN):
        raise WorkflowMismatch("Missing or invalid measured-window schema")
    boundaries = []
    for name in ("start", "end"):
        boundary = window.get(name)
        if not isinstance(boundary, dict):
            raise WorkflowMismatch("Missing measured boundary")
        for field in ("timestamp_ns", "monotonic_ns", "uncertainty_ns"):
            value = boundary.get(field)
            if type(value) is not int or value < (0 if field == "uncertainty_ns" else 1):
                raise WorkflowMismatch("Invalid measured boundary clock value")
        # Clock reads should be tiny; a delayed reader cannot claim a broadly
        # uncertain window that silently admits out-of-window business work.
        if boundary["uncertainty_ns"] > CLOCK_COHERENCE_NS:
            raise WorkflowMismatch("Measured clock capture uncertainty exceeds 1 ms")
        boundaries.append(boundary)
    start, end = boundaries
    duration_ns = end["monotonic_ns"] - start["monotonic_ns"]
    wall_ns = end["timestamp_ns"] - start["timestamp_ns"]
    if duration_ns <= 0 or wall_ns <= 0:
        raise WorkflowMismatch("Measured boundaries are reversed or empty")
    tolerance = (CLOCK_COHERENCE_NS + duration_ns * CLOCK_SLEW_PPM // 1_000_000
                 + start["uncertainty_ns"] + end["uncertainty_ns"])
    if abs(duration_ns - wall_ns) > tolerance:
        raise WorkflowMismatch("Wall clock and monotonic measured interval disagree")
    return start, end, duration_ns


def elapsed_seconds(window):
    """Use this exact value as metrics.wall_seconds, without independent timers."""
    return _window_values(window)[2] / 1_000_000_000


def validate_timing_window(window, wall_seconds, events, operation_ids, *, terminal_event="succeeded"):
    """Admit boundaries and coverage of every measured operation's events.

    Pass actual persisted events, not a summary or synthetic replacement.
    Callers must also enforce their complete graph/count/output contract. Only
    explicit measured IDs are selected; warmup is validated by graph admission.
    The terminal event is configurable for the image observer's own schema.
    """
    start, end, duration_ns = _window_values(window)
    if (isinstance(wall_seconds, bool) or not isinstance(wall_seconds, (int, float))
            or not math.isfinite(wall_seconds) or wall_seconds <= 0
            or abs(wall_seconds - duration_ns / 1_000_000_000) > 1e-9):
        raise WorkflowMismatch("Reported duration differs from captured monotonic boundaries")
    operations = list(operation_ids)
    if (not operations or any(not isinstance(op, str) or not op for op in operations)
            or len(set(operations)) != len(operations)):
        raise WorkflowMismatch("Measured operation identities must be explicit and unique")
    if not isinstance(terminal_event, str) or not terminal_event:
        raise WorkflowMismatch("Terminal event must be explicit")
    phases = {op: set() for op in operations}
    event_count = 0
    lower = start["timestamp_ns"] - start["uncertainty_ns"]
    upper = end["timestamp_ns"] + end["uncertainty_ns"]
    for event in events:
        if not isinstance(event, dict):
            raise WorkflowMismatch("Measured events must be objects")
        op = event.get("operation_id")
        if not isinstance(op, str) or not op:
            raise WorkflowMismatch("Measured event has no operation identity")
        if op not in phases:
            continue
        timestamp = event.get("timestamp_ns")
        if type(timestamp) is not int or timestamp <= 0:
            raise WorkflowMismatch("Measured event has no valid Unix timestamp")
        if not lower <= timestamp <= upper:
            raise WorkflowMismatch("Measured business event lies outside timing boundaries")
        phase = event.get("event")
        if not isinstance(phase, str) or not phase:
            raise WorkflowMismatch("Measured event has no valid phase")
        phases[op].add(phase)
        event_count += 1
    if any(not {"submitted", terminal_event} <= seen for seen in phases.values()):
        raise WorkflowMismatch("Measured window lacks submission or terminal coverage")
    return {"schema_version": 1, "passed": True, "clock_domain": CLOCK_DOMAIN,
            "operations": len(operations), "events": event_count,
            "duration_ns": duration_ns, "clock_slew_ppm": CLOCK_SLEW_PPM,
            "clock_coherence_ns": CLOCK_COHERENCE_NS}
