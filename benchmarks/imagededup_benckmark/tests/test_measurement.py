"""Deterministic completion-boundary tests; no workers or performance runs."""
import unittest
import json
import sqlite3
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from imagededup_benckmark.measurement import Measurement


class MeasurementBoundaryTest(unittest.TestCase):
    def test_terminal_probe_before_final_http_response_still_includes_submission(self):
        meter = Measurement(SimpleNamespace(processes={}), 1, 1, 0, interval=.01, timeout=1)
        meter.started = 1.0
        meter.window_start = {"timestamp_ns": 1_000_000_000, "monotonic_ns": 1_000_000_000, "uncertainty_ns": 0}
        meter.finished = .5  # Persisted terminal outcome while final HTTP response is pending.
        meter.done_event.set()
        meter.thread = Mock()
        end = {"timestamp_ns": 3_000_000_000, "monotonic_ns": 3_000_000_000, "uncertainty_ns": 0}
        window = {"schema_version": 1, "clock_domain": "unix_time_ns", "start": meter.window_start, "end": end}
        # The final HTTP response returns at absolute monotonic 2.5 seconds.
        with patch("imagededup_benckmark.measurement.end_window", return_value=window):
            result = meter.finish(submission_finished=2.5)
        self.assertEqual(result["business_finished_seconds"], .5)
        self.assertEqual(result["wall_seconds"], 2.0)
        self.assertGreaterEqual(result["wall_seconds"], meter.submission_finished)

    def test_rejects_boundary_that_excludes_submission_instead_of_clamping(self):
        meter = Measurement(SimpleNamespace(processes={}), 1, 1, 0, interval=.01, timeout=1)
        meter.started = 1.0
        meter.window_start = {"timestamp_ns": 1_000_000_000, "monotonic_ns": 1_000_000_000, "uncertainty_ns": 0}
        meter.finished = .5
        meter.done_event.set()
        meter.thread = Mock()
        window = {"schema_version": 1, "clock_domain": "unix_time_ns", "start": meter.window_start,
                  "end": {"timestamp_ns": 2_000_000_000, "monotonic_ns": 2_000_000_000, "uncertainty_ns": 0}}
        with patch("imagededup_benckmark.measurement.end_window", return_value=window):
            with self.assertRaisesRegex(ValueError, "excludes completion"):
                meter.finish(submission_finished=2.5)


    def test_durable_terminal_probe_waits_for_delayed_positive_commit_callback(self):
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory, "application.db")
            commits = Path(directory, "business-commits.jsonl")
            with sqlite3.connect(database) as connection:
                connection.executescript("""CREATE TABLE feature_artifact(workspace_id INTEGER, hash_value TEXT, execution_status TEXT);
                    CREATE TABLE comparison_request(workspace_id INTEGER, candidates_scored_count INTEGER, execution_status TEXT);
                    INSERT INTO feature_artifact VALUES(1, 'hash', 'finished');""")
            stack = SimpleNamespace(backend="redis_celery", database=database, business_commit_file=commits,
                                    processes={}, api_port=9, check_alive=lambda: None)
            meter = Measurement(stack, 1, 1, 0, interval=.005, timeout=2)
            checked = threading.Event()
            original = meter.positive_commits_observed
            def probe():
                result = original()
                checked.set()
                return result
            meter.positive_commits_observed = probe
            meter.start()
            try:
                self.assertTrue(checked.wait(1))
                self.assertFalse(meter.done_event.is_set())
                # Durability was already visible, but SQLA after_commit observation
                # was delayed until here. The timing boundary must wait for it.
                committed_ns = time.time_ns()
                commits.write_text(json.dumps({"workspace_id": 1, "stage": "build", "page_items": 1,
                                               "timestamp_ns": committed_ns}) + "\n")
                result = meter.finish(submission_finished=time.perf_counter())
                self.assertGreaterEqual(result["measurement_window"]["end"]["timestamp_ns"], committed_ns)
            finally:
                meter.cancel()
