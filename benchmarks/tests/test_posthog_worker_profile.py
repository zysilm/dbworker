"""Separate 32-worker profiles must preserve native defaults and trusted settings."""
import copy
import unittest

from benchmarks.common.business_evidence import trusted_posthog_worker_concurrency, trusted_warmup_requests
from benchmarks.common.performance_admission import replay_graph
from benchmarks.common.workflow_graph import WorkflowMismatch
from benchmarks.diagnostics.posthog_worker_coverage import warmup_readiness


class PostHogWorkerProfileTests(unittest.TestCase):
    def test_official_default_and_explicit_32_workers(self):
        self.assertEqual(trusted_posthog_worker_concurrency({}), 8)
        profile = {"worker_concurrency": 32, "warmup_requests": 32}
        self.assertEqual(trusted_posthog_worker_concurrency(profile), 32)
        self.assertEqual(trusted_warmup_requests("posthog", profile), 32)

    def test_invalid_or_incomplete_32_profiles(self):
        for profile in ({"worker_concurrency": True}, {"worker_concurrency": 16},
                        {"worker_concurrency": "32"}, {"worker_concurrency": 32},
                        {"worker_concurrency": 32, "warmup_requests": 8},
                        {"worker_concurrency": 8, "warmup_requests": 32}):
            with self.subTest(profile=profile), self.assertRaises(WorkflowMismatch):
                trusted_posthog_worker_concurrency(profile)
        with self.assertRaises(WorkflowMismatch):
            trusted_warmup_requests("saleor", {"worker_concurrency": 32, "warmup_requests": 32})

    def test_sample_configuration_cannot_select_concurrency(self):
        profile = {"worker_concurrency": 32, "warmup_requests": 32}
        row = {"backend": "dbworker", "configuration": {"concurrency": 32,
                "worker_service_readiness": {"concurrency": 32}},
               "native_execution": {"configuration": {"worker_total_concurrency": 32,
                                                         "dbworker_shared_stage_pool": 32}}}
        for settings, field in (("configuration", "concurrency"),
                                ("native_execution", "worker_total_concurrency"),
                                ("native_execution", "dbworker_shared_stage_pool"),
                                ("readiness", "concurrency")):
            changed = copy.deepcopy(row)
            target = changed["configuration"] if settings == "configuration" else (
                changed["configuration"]["worker_service_readiness"] if settings == "readiness" else
                changed["native_execution"]["configuration"])
            target[field] = 8
            with self.subTest(field=field), self.assertRaisesRegex(WorkflowMismatch, "concurrency|configuration"):
                replay_graph(changed, "posthog", ".", 5000, expected_profile=profile)

    def test_full_warm_pool_requires_every_measured_process(self):
        events = []
        for pid in range(1, 33):
            for phase, timestamp in (("started", 1), ("succeeded", 2)):
                events.append({"operation_id": f"warmup:notification-{pid:04d}", "node_id": str(pid),
                               "event": phase, "timestamp_ns": timestamp, "process_id": pid,
                               "details": {"native_worker_origin": {"passed": True}}})
        events.extend([{"operation_id": "notification-0000", "event": "submitted", "timestamp_ns": 3},
                       {"operation_id": "notification-0000", "event": "started", "timestamp_ns": 4, "process_id": 1}])
        self.assertEqual(warmup_readiness(events, 32)["status"], "verified")
        self.assertEqual(warmup_readiness(events, 8)["status"], "insufficient")
        events[-1]["process_id"] = 33
        self.assertEqual(warmup_readiness(events, 32)["status"], "insufficient")


if __name__ == "__main__":
    unittest.main()
