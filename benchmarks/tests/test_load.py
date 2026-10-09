"""Correctness checks for fixed producer schedules, without performance runs."""
import json
import threading
import time
import unittest

from benchmarks.common.load import run_load


class FixedLoadTests(unittest.TestCase):
    def test_concurrent_waves_preserve_identity_and_independent_producers(self):
        gate = threading.Barrier(4)
        owners = {}
        def factory(producer_index):
            owner = threading.get_ident()
            owners[producer_index] = owner
            def submit(item, index):
                self.assertEqual(owner, threading.get_ident())
                self.assertEqual(index % 4, producer_index)
                gate.wait(timeout=2)
                time.sleep(.002)
                return {"item": item, "index": index}
            return submit
        result = run_load(list(range(8)), None, producers=4, duration_seconds=.02, producer_factory=factory)
        self.assertEqual(len(set(owners.values())), 4)
        self.assertEqual(result["peak_concurrent_calls"], 4)
        self.assertEqual([row["index"] for row in result["results"]], list(range(8)))
        self.assertEqual([row["planned_offset_seconds"] for row in result["submissions"]], [0]*4 + [.01]*4)
        self.assertGreaterEqual(result["submission_wall_seconds"], .02)
        json.dumps(result, allow_nan=False)

    def test_late_work_is_not_dropped_or_rescheduled(self):
        seen = []
        def submit(item, index):
            seen.append(index)
            time.sleep(.008)
        result = run_load(range(3), submit, producers=1, duration_seconds=.003)
        self.assertEqual(seen, [0, 1, 2])
        self.assertEqual([row["planned_offset_seconds"] for row in result["submissions"]], [0, .001, .002])
        self.assertGreater(result["submissions"][-1]["lateness_seconds"], .005)
        self.assertEqual(result["dropped_operations"], 0)

    def test_callback_failure_is_never_a_successful_partial_workload(self):
        def fail(item, index):
            raise RuntimeError("Native publication failed")
        with self.assertRaisesRegex(RuntimeError, "Native publication failed"):
            run_load(range(2), fail, producers=2, duration_seconds=0)

    def test_factory_failure_cannot_deadlock_the_start_barrier(self):
        def factory(index):
            if index == 1:
                raise ValueError("Authentication failed")
            return lambda item, index: None
        with self.assertRaisesRegex(ValueError, "Authentication failed"):
            run_load(range(2), None, producers=2, duration_seconds=0, producer_factory=factory)

    def test_reject_invalid_profile(self):
        for kwargs in ({"producers": 0}, {"duration_seconds": -1}, {"duration_seconds": float("nan")}):
            with self.assertRaises(ValueError):
                run_load([1], lambda item, index: None, **kwargs)
