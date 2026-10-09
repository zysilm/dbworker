"""Replayable idle receipts reject missing native lanes and unfinished work."""
import copy
import unittest

from imagededup_benckmark.evidence import validate_quiescence


class IdleReceiptTest(unittest.TestCase):
    def receipt(self):
        return {"event": "quiescence_barrier", "stage": "barrier", "timestamp": 3.0,
                "observed_at": 2.9, "backend": "celery", "idle": True,
                "worker_responses_complete": True, "pending_business_outbox": 0,
                "worker_states": {state: {"build": 0, "comparison": 0} for state in ("active", "reserved", "scheduled")},
                "redis_priority_steps": [0, 3, 6, 9],
                "redis_lanes": [{"queue": queue, "priority": priority, "messages": 0}
                                for queue in ("image_build", "image_compare") for priority in (0, 3, 6, 9)]}

    def test_legacy_closed_trace_does_not_prove_quiescence(self):
        self.assertFalse(validate_quiescence([]))

    def test_valid_native_snapshot(self):
        self.assertTrue(validate_quiescence([self.receipt()]))

    def test_rejects_incomplete_or_busy_snapshot(self):
        original = self.receipt()
        mutations = [lambda row: row["redis_lanes"].pop(),
                     lambda row: row["redis_lanes"][0].update(messages=1),
                     lambda row: row["worker_states"]["scheduled"].update(build=1),
                     lambda row: row.update(pending_business_outbox=1),
                     lambda row: row["worker_states"].pop("active")]
        for mutate in mutations:
            row = copy.deepcopy(original)
            mutate(row)
            with self.assertRaises(AssertionError):
                validate_quiescence([row])

    def test_dbworker_requires_actual_durable_work_counts(self):
        row = {"event": "quiescence_barrier", "stage": "barrier", "timestamp": 3., "observed_at": 2.9,
               "backend": "dbworker", "idle": True,
               "unfinished_work": {"artifact_build_work": 0, "comparison_work": 0}}
        self.assertTrue(validate_quiescence([row]))
        row["unfinished_work"]["comparison_work"] = 1
        with self.assertRaises(AssertionError):
            validate_quiescence([row])


    def test_rejects_nonfinite_boolean_and_reversed_receipt_times(self):
        for value in (True, False, float("nan"), float("inf"), -1, 0):
            for key in ("observed_at", "timestamp"):
                row = self.receipt()
                row[key] = value
                with self.assertRaises(AssertionError):
                    validate_quiescence([row])
        row = self.receipt()
        row["observed_at"] = 4.
        with self.assertRaises(AssertionError):
            validate_quiescence([row])
        row = self.receipt()
        row["pending_business_outbox"] = False
        with self.assertRaises(AssertionError):
            validate_quiescence([row])

    def test_omitting_same_priority_from_steps_and_both_lanes_is_rejected(self):
        row = self.receipt()
        row["redis_priority_steps"].remove(9)
        row["redis_lanes"] = [lane for lane in row["redis_lanes"] if lane["priority"] != 9]
        with self.assertRaises(AssertionError):
            validate_quiescence([row])
