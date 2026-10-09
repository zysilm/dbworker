"""Fixed open-loop producer waves with isolated persistent producer threads.

A single schedule is selected per scenario. Overload never drops requests or
moves planned arrivals; lateness is retained when a producer cannot keep up.
"""
from __future__ import annotations

import math
import threading
import time
from concurrent.futures import ThreadPoolExecutor


def distribution(values):
    """Return deterministic nearest-rank percentiles, in the input units."""
    values = sorted(values)
    if not values:
        return {"count": 0, "min": None, "median": None, "p95": None, "p99": None, "max": None}
    def percentile(fraction):
        return values[max(0, math.ceil(len(values) * fraction) - 1)]
    return {"count": len(values), "min": values[0], "median": percentile(.5),
            "p95": percentile(.95), "p99": percentile(.99), "max": values[-1]}


def run_load(items, submit, *, producers=8, duration_seconds=60, producer_factory=None):
    """Submit every item exactly once and return ordered, serializable evidence.

    ``submit(item, index)`` runs on a persistent producer thread. An optional
    ``producer_factory(producer_index)`` creates that thread's own submit callable
    before the shared epoch. Results must be JSON serializable. Exceptions are
    propagated after all producer threads stop, so failed runs cannot be reported
    as reduced successful workloads. This function measures producer submission,
    not asynchronous business completion.
    """
    items = list(items)
    if isinstance(producers, bool) or not isinstance(producers, int) or producers < 1:
        raise ValueError("producers must be a positive integer")
    if not math.isfinite(duration_seconds) or duration_seconds < 0:
        raise ValueError("duration_seconds must be finite and nonnegative")
    if not items:
        raise ValueError("A load profile must contain at least one operation")
    producer_count = min(producers, len(items))
    waves = math.ceil(len(items) / producer_count)
    records = [None] * len(items)
    results = [None] * len(items)
    state = {"active": 0, "peak": 0}
    guard = threading.Lock()
    epoch = {}
    def release():
        epoch.update(monotonic_ns=time.monotonic_ns(), timestamp_ns=time.time_ns())
    barrier = threading.Barrier(producer_count, action=release)

    def producer(producer_index):
        try:
            callback = producer_factory(producer_index) if producer_factory else submit
        except BaseException:
            barrier.abort()
            raise
        barrier.wait()
        for index in range(producer_index, len(items), producer_count):
            offset = (index // producer_count) * duration_seconds / waves
            planned = epoch["monotonic_ns"] + int(offset * 1_000_000_000)
            remaining = (planned - time.monotonic_ns()) / 1_000_000_000
            if remaining > 0:
                time.sleep(remaining)
            actual = time.monotonic_ns()
            timestamp = time.time_ns()
            with guard:
                state["active"] += 1
                state["peak"] = max(state["peak"], state["active"])
            error = None
            try:
                results[index] = callback(items[index], index)
            except BaseException as caught:
                error = caught
            finally:
                finished = time.monotonic_ns()
                with guard:
                    state["active"] -= 1
                records[index] = {"index": index, "producer_index": producer_index,
                                  "planned_offset_seconds": offset,
                                  "planned_timestamp_ns": epoch["timestamp_ns"] + int(offset * 1_000_000_000),
                                  "started_timestamp_ns": timestamp,
                                  "finished_timestamp_ns": timestamp + finished - actual,
                                  "lateness_seconds": max(0, actual - planned) / 1_000_000_000,
                                  "submit_seconds": (finished - actual) / 1_000_000_000,
                                  "status": "failed" if error else "submitted"}
            if error:
                raise error

    with ThreadPoolExecutor(max_workers=producer_count, thread_name_prefix="benchmark-producer") as pool:
        futures = [pool.submit(producer, index) for index in range(producer_count)]
        failures = []
        for future in futures:
            try:
                future.result()
            except BaseException as error:
                failures.append(error)
        if failures:
            # Preserve the original callback/factory error over barrier fallout.
            raise next((error for error in failures if not isinstance(error, threading.BrokenBarrierError)), failures[0])
    # Keep the complete offered-load window even when its final wave finishes early.
    remaining = duration_seconds - (time.monotonic_ns() - epoch["monotonic_ns"]) / 1_000_000_000
    if remaining > 0:
        time.sleep(remaining)
    elapsed = (time.monotonic_ns() - epoch["monotonic_ns"]) / 1_000_000_000
    return {"schema_version": 1, "mode": "fixed_open_loop_waves", "producers": producer_count,
            "operations": len(items), "waves": waves, "duration_seconds": duration_seconds,
            "schedule_epoch_timestamp_ns": epoch["timestamp_ns"],
            "offered_operations_per_second": len(items) / duration_seconds if duration_seconds else None,
            "submission_wall_seconds": elapsed, "submitted_operations": len(records),
            "peak_concurrent_calls": state["peak"], "dropped_operations": 0,
            "submission_latency_seconds": distribution([row["submit_seconds"] for row in records]),
            "schedule_lateness_seconds": distribution([row["lateness_seconds"] for row in records]),
            "submissions": records, "results": results}
