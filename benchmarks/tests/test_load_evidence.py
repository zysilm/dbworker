"""Verify observed queue waiting and backlog under overlapping native tasks."""
import unittest
from benchmarks.common.load_evidence import task_load_metrics


class LoadEvidenceTests(unittest.TestCase):
    def test_overlapping_tasks_and_warmup_exclusion(self):
        events = []
        for node, op, times in [("a", "op", (1, 2, 4)), ("b", "op", (1.5, 4, 5)), ("w", "warmup", (1, 1, 1))]:
            for phase, seconds in zip(("submitted", "started", "succeeded"), times):
                events.append(dict(node_id=node, operation_id=op, event=phase, timestamp_ns=int(seconds * 1e9)))
        metrics = task_load_metrics(events, ["op"])
        self.assertEqual(metrics["task_count"], 2)
        self.assertEqual(metrics["peak_published_outstanding"], 2)
        self.assertEqual(metrics["queue_wait"]["p95_seconds"], 2.5)
        self.assertEqual(metrics["timeline"][0]["published_waiting"], 1)
        self.assertEqual(metrics["timeline"][-1]["published_outstanding"], 0)

    def test_incomplete_evidence_fails(self):
        with self.assertRaises(ValueError):
            task_load_metrics([dict(node_id="a", operation_id="op", event="submitted", timestamp_ns=1)], ["op"])


if __name__ == "__main__":
    unittest.main()


class FixedLoadAdmissionTests(unittest.TestCase):
    def setUp(self):
        from benchmarks.common.load import run_load
        from benchmarks.common.timing_evidence import begin_window, end_window
        self.profile = {"requests": 4, "producers": 2, "submission_window_seconds": .002}
        start = begin_window()
        self.row = {"scenario": "native", "metrics": {"load": run_load(range(4), lambda item, index: None,
                         producers=2, duration_seconds=.002)}, "measurement_window": end_window(start)}

    def admit(self, row):
        from benchmarks.common.load_evidence import validate_fixed_load
        validate_fixed_load(row, self.profile)

    def test_exact_complete_fixed_schedule_passes(self):
        self.admit(self.row)

    def test_reduced_work_or_producer_budget_fails(self):
        import copy
        for field, value in (("operations", 3), ("submitted_operations", 3), ("producers", 1), ("duration_seconds", 0), ("dropped_operations", 1)):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.row)
                changed["metrics"]["load"][field] = value
                with self.assertRaises(ValueError):
                    self.admit(changed)

    def test_omitted_reassigned_or_rescheduled_request_fails(self):
        import copy
        for field, value in (("index", 3), ("producer_index", 1), ("planned_offset_seconds", .001), ("status", "failed")):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.row)
                changed["metrics"]["load"]["submissions"][0][field] = value
                with self.assertRaises(ValueError):
                    self.admit(changed)

    def test_nonfinite_schedule_and_duration_fail(self):
        import copy
        for field in ("planned_offset_seconds", "submission_wall_seconds"):
            with self.subTest(field=field):
                changed = copy.deepcopy(self.row)
                if field == "planned_offset_seconds":
                    changed["metrics"]["load"]["submissions"][0][field] = float("nan")
                else:
                    changed["metrics"]["load"][field] = float("nan")
                with self.assertRaises(ValueError):
                    self.admit(changed)

    def test_receipts_outside_measurement_window_fail(self):
        import copy
        changed = copy.deepcopy(self.row)
        record = changed["metrics"]["load"]["submissions"][0]
        record["finished_timestamp_ns"] = changed["measurement_window"]["end"]["timestamp_ns"] + 1_000_000_000
        with self.assertRaises(ValueError):
            self.admit(changed)


class WorkloadNamingTests(unittest.TestCase):
    def test_names_show_actual_business_quantities(self):
        from benchmarks.render_results import workload_label
        fixtures = [
            ("superset", "sql_lab_group_by", {"requests": 1000}, "1,000 queries / 10,000 rows"),
            ("saleor", "product_csv_export", {"requests": 500, "products": 256}, "500 x 256 products + 500 emails"),
            ("paperless_ngx", "native_unsplit_scan_ingestion", {"requests": 200}, "200 scans"),
            ("posthog", "native_two_factor_notification", {"requests": 1000}, "1,000 requests / 2,000 tasks"),
            ("sentry", "historical_native_email_fanout", {"requests": 5000}, "5,000 requests / 10,000 deliveries"),
        ]
        for suite, scenario, profile, expected in fixtures:
            with self.subTest(suite=suite):
                report = {"suite_id": suite, "configuration": {"profile": profile}, "runs": [{"scenario": scenario}]}
                self.assertIn(expected, workload_label(report, scenario))

    def test_image_names_distinguish_bulk_build_and_directed_comparisons(self):
        from benchmarks.render_results import workload_label
        for scenario, expected in (("build", "1,000 image hashes (one bulk import)"),
                                   ("comparison", "1,000 images / 999,000 directed pairs"),
                                   ("mixed", "Build + compare 1,000 images / 999,000 directed pairs")):
            report = {"suite_id": "imagededup", "configuration": {"images": 1000}, "runs": [{"scenario": scenario}]}
            self.assertIn(expected, workload_label(report, scenario))
