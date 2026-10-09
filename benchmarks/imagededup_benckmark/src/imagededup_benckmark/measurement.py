"""Read-only progress probes and process-tree CPU/RSS measurements."""

import math
import sqlite3
import statistics
import threading
import time
from typing import Any

import httpx
import psutil

from imagededup_benckmark.runtime import Stack
from benchmarks.common.timing_evidence import begin_window, end_window, elapsed_seconds


def distribution(values: list[float], *, scale: float = 1) -> dict[str, float | int | None]:
    ordered = sorted(value * scale for value in values)
    return {"count": len(values), "median": statistics.median(ordered) if ordered else None,
            "p95": ordered[min(len(ordered) - 1, math.ceil(len(ordered) * .95) - 1)] if ordered else None,
            "max": max(ordered) if ordered else None}


def business_capacity(progress: list[dict[str, Any]], *, submission_start: float,
                      submission_window: float) -> dict[str, Any]:
    """Describe one fixed offered load without inferring maximum capacity."""
    points = [point for point in progress if submission_start <= point["seconds"] <= submission_start + submission_window]
    early = [point["unfinished_comparison_requests"] for point in points
             if point["seconds"] - submission_start < submission_window / 3]
    late = [point["unfinished_comparison_requests"] for point in points
            if point["seconds"] - submission_start >= submission_window * 2 / 3]
    first = statistics.mean(early) if early else None
    last = statistics.mean(late) if late else None
    growth = last - first if first is not None and last is not None else None
    return {"scope": "unfinished persisted business comparisons during one fixed offered load",
            "sample_count": len(points), "early_third_mean_backlog": first,
            "late_third_mean_backlog": last, "late_minus_early_backlog": growth,
            "backlog_growth_observed": growth > 1 if growth is not None else None,
            "maximum_capacity_measured": False,
            "interpretation": "Backlog growth indicates pressure at this offered load; bounded observations do not establish a maximum sustainable rate."}


class Measurement:
    def __init__(self, stack: Stack, workspace_id: int, images: int, comparisons: int,
                 *, interval: float, timeout: float, new_builds: bool = True) -> None:
        self.stack, self.workspace_id = stack, workspace_id
        self.images, self.comparisons = images, comparisons
        self.interval, self.timeout = interval, timeout
        self.new_builds = new_builds
        self.commit_record_offset = 0
        self.stop_event = threading.Event()
        self.done_event = threading.Event()
        self.thread = threading.Thread(target=self._monitor, daemon=True)
        self.started = time.perf_counter()
        self.finished: float | None = None
        self.submission_finished: float | None = None
        self.error: BaseException | None = None
        self.samples: list[dict[str, Any]] = []
        self.cpu_initial: dict[tuple[int, float], float] = {}
        self.cpu_latest: dict[tuple[int, float], float] = {}
        self.roles: dict[tuple[int, float], str] = {}
        self.peak_rss = 0
        self.api_latency: list[float] = []
        self.api_errors = 0
        self.sql_busy_probes = 0
        self.probe_count = 0
        self.first_scored: float | None = None
        self.builds_finished: float | None = None
        self.overlap_observed = False
        self.last_progress: dict[str, Any] = {}

    def resources(self, *, baseline: bool = False) -> None:
        rss = 0
        for role, root in self.stack.processes.items():
            try:
                parent = psutil.Process(root.pid)
                processes = [parent, *parent.children(recursive=True)]
            except psutil.NoSuchProcess:
                continue
            for process in processes:
                try:
                    key = (process.pid, process.create_time())
                    cpu = process.cpu_times()
                    total = cpu.user + cpu.system
                    if key not in self.cpu_initial:
                        self.cpu_initial[key] = total if baseline else 0.0
                    self.cpu_latest[key] = total
                    self.roles[key] = role if process.pid == root.pid else role + "_children"
                    rss += process.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        self.peak_rss = max(self.peak_rss, rss)

    def start(self) -> None:
        self.resources(baseline=True)
        from imagededup_benckmark.observation import read_records
        self.commit_record_offset = len(read_records(self.stack.business_commit_file))
        self.window_start = begin_window()
        self.started = self.window_start["monotonic_ns"] / 1_000_000_000
        self.thread.start()

    def positive_commits_observed(self) -> bool:
        from imagededup_benckmark.observation import read_records
        records = [row for row in read_records(self.stack.business_commit_file)[self.commit_record_offset:]
                   if row["workspace_id"] == self.workspace_id]
        builds = sum(row["page_items"] for row in records if row["stage"] == "build")
        pairs = sum(row["page_items"] for row in records if row["stage"] == "comparison")
        return builds == (self.images if self.new_builds else 0) and pairs == self.comparisons * (self.images - 1)

    def _monitor(self) -> None:
        try:
            with sqlite3.connect(self.stack.database, timeout=.05) as connection, httpx.Client(
                base_url=f"http://127.0.0.1:{self.stack.api_port}", timeout=1,
            ) as client:
                connection.execute("PRAGMA query_only=ON")
                next_api_probe = 0.0
                while not self.stop_event.is_set():
                    self.stack.check_alive()
                    elapsed = time.perf_counter() - self.started
                    if elapsed > self.timeout:
                        raise TimeoutError(f"Scenario exceeded {self.timeout}s; last progress: {self.last_progress}")
                    self.resources()
                    try:
                        # Only common application tables are read. No generated
                        # worker table names, ledger scans or readiness writes.
                        row = connection.execute('''SELECT
                            (SELECT COUNT(*) FROM feature_artifact WHERE workspace_id=? AND hash_value IS NOT NULL),
                            COUNT(*), COALESCE(SUM(candidates_scored_count),0),
                            COALESCE(SUM(candidates_scored_count=?),0)
                            FROM comparison_request WHERE workspace_id=?''',
                            (self.workspace_id, self.images - 1, self.workspace_id)).fetchone()
                    except sqlite3.OperationalError:
                        self.sql_busy_probes += 1
                        self.stop_event.wait(self.interval)
                        continue
                    ready, requests, scored, completed = map(int, row)
                    if getattr(self.stack, "backend", None) == "dbwork":
                        build_status = "SELECT COUNT(*) FROM artifact_build_work w JOIN feature_artifact a ON a.id=w.source_id WHERE a.workspace_id=? AND w.execution_status='finished'"
                        comparison_status = "SELECT COUNT(*) FROM comparison_work w JOIN comparison_request r ON r.id=w.source_id WHERE r.workspace_id=? AND w.execution_status='finished'"
                    elif getattr(self.stack, "backend", None) == "redis_celery":
                        build_status = "SELECT COUNT(*) FROM feature_artifact WHERE workspace_id=? AND execution_status='finished'"
                        comparison_status = "SELECT COUNT(*) FROM comparison_request WHERE workspace_id=? AND execution_status='finished'"
                    else:
                        raise ValueError("Unknown image application backend")
                    try:
                        terminal_builds = connection.execute(build_status, (self.workspace_id,)).fetchone()[0]
                        terminal_comparisons = connection.execute(comparison_status, (self.workspace_id,)).fetchone()[0]
                    except sqlite3.OperationalError:
                        self.sql_busy_probes += 1
                        self.stop_event.wait(self.interval)
                        continue
                    elapsed = time.perf_counter() - self.started
                    sample = {"seconds": elapsed, "built": ready, "requests": requests,
                              "scored_pairs": scored, "completed_requests": completed,
                              "unfinished_comparison_requests": requests - completed}
                    self.last_progress = sample
                    self.probe_count += 1
                    self.samples.append(sample)
                    if len(self.samples) > 2000:
                        self.samples = self.samples[::2]
                    if ready == self.images and self.builds_finished is None:
                        self.builds_finished = elapsed
                    if scored and self.first_scored is None:
                        self.first_scored = elapsed
                    if scored and ready < self.images:
                        self.overlap_observed = True
                    if (ready == self.images and requests == self.comparisons and completed == self.comparisons
                            and terminal_builds == self.images and terminal_comparisons == self.comparisons
                            and self.positive_commits_observed()):
                        self.finished = elapsed
                        self.done_event.set()
                        return
                    if elapsed >= next_api_probe:
                        probe_start = time.perf_counter()
                        try:
                            response = client.get(f"/workspaces/{self.workspace_id}/artifacts", params={"limit": 1})
                            response.raise_for_status()
                            self.api_latency.append(time.perf_counter() - probe_start)
                        except httpx.HTTPError:
                            self.api_errors += 1
                        next_api_probe = elapsed + .25
                    self.stop_event.wait(self.interval)
        except BaseException as exc:
            self.error = exc
            self.done_event.set()

    def finish(self, *, submission_finished: float) -> dict[str, Any]:
        """Include the final submitting HTTP response even if business finished first."""
        if submission_finished < self.started:
            raise ValueError("Submission completion precedes measurement start")
        self.submission_finished = submission_finished - self.started
        self.done_event.wait(self.timeout + 2)
        measurement_window = end_window(self.window_start)
        self.stop_event.set()
        self.thread.join(timeout=3)
        if self.error:
            raise self.error
        if self.finished is None:
            raise TimeoutError("No completion observation")
        business_finished = self.finished
        self.finished = elapsed_seconds(measurement_window)
        if self.finished < max(business_finished, self.submission_finished):
            raise ValueError("Measurement boundary excludes completion barrier")
        self.resources()
        cpu: dict[str, float] = {}
        for key, value in self.cpu_latest.items():
            role = self.roles[key]
            cpu[role] = cpu.get(role, 0) + max(0, value - self.cpu_initial[key])
        total_cpu = sum(cpu.values())
        return {
            "wall_seconds": self.finished,
            "measurement_window": measurement_window,
            "business_finished_seconds": business_finished,
            "cpu_seconds": total_cpu, "cpu_seconds_by_role": cpu,
            "average_cpu_cores_used": total_cpu / self.finished,
            "peak_summed_rss_bytes": self.peak_rss,
            "images_per_second": self.images / self.finished,
            "pairs_per_second": self.comparisons * (self.images - 1) / self.finished,
            "builds_finished_seconds": self.builds_finished,
            "first_scored_seconds": self.first_scored,
            "scoring_before_all_builds_finished": self.overlap_observed,
            "api_probe_latency_ms": distribution(self.api_latency, scale=1000),
            "api_probe_errors": self.api_errors, "sql_busy_probes": self.sql_busy_probes,
            "progress_probe_count": self.probe_count, "progress": self.samples,
        }

    def cancel(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=3)
