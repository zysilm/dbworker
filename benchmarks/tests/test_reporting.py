import copy
import json
import tempfile
import unittest
from pathlib import Path

from benchmarks.common.reporting import digest, new_report, validate_report, write_json
from benchmarks.render_results import render


def passed_report():
    report = new_report("example", "run-1", "full")
    report["status"] = "passed"
    report["source"] = {"commit": "test-commit"}
    for backend, seconds in (("celery", 2), ("dbworker", 1)):
        report["runs"].append({"scenario": "export", "comparison_mode": "paired_durable_request",
                               "backend": backend, "repetition": 1, "status": "passed",
                               "metrics": {"wall_seconds": seconds},
                               "validation": {"passed": True, "output_digest": "identical-output"}})
    return report


class ReportingTests(unittest.TestCase):
    def test_rejects_unpaired_or_different_outputs(self):
        report = passed_report()
        validate_report(report)
        for mutation in ("unpaired", "mismatch", "duplicate", "invalid_time"):
            bad = copy.deepcopy(report)
            if mutation == "unpaired":
                bad["runs"].pop()
            elif mutation == "mismatch":
                bad["runs"][1]["validation"]["output_digest"] = "wrong"
            elif mutation == "duplicate":
                bad["runs"].append(copy.deepcopy(bad["runs"][0]))
            else:
                bad["runs"][0]["metrics"]["wall_seconds"] = float("nan")
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_report(bad)

    def test_passed_report_requires_all_configured_repetitions(self):
        report = passed_report()
        report["configuration"]["profile"] = {"repetitions": 5}
        with self.assertRaisesRegex(ValueError, "every configured repetition"):
            validate_report(report)

    def test_failed_admission_is_valid_without_samples(self):
        report = new_report("example", "run-1", "full")
        report["status"] = "blocked"
        report["errors"].append({"phase": "setup", "message": "Interpreter unavailable"})
        validate_report(report)

    def test_renderer_checks_coverage_checksums_and_outputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "example.json"
            write_json(path, passed_report())
            index = {"schema_version": 1, "run_id": "run-1", "profile": "full", "status": "passed",
                     "complete_selection": True, "required_suites": ["example"],
                     "reports": [{"suite_id": "example", "path": path.name, "status": "passed", "sha256": digest(path)}]}
            write_json(root / "index.json", index)
            output = root / "results.md"
            first = render(root, output)
            self.assertEqual(first, render(root, output))
            self.assertIn("2.000", first)
            index["complete_selection"] = False
            write_json(root / "index.json", index)
            with self.assertRaises(ValueError):
                render(root, output)
            self.assertIn("partial", render(root, output, allow_partial=True))
            path.write_text(path.read_text() + " ")
            with self.assertRaises(ValueError):
                render(root, output, allow_partial=True)

    def test_atomic_writer_preserves_previous_on_nonfinite_values(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "report.json"
            write_json(path, {"status": "old"})
            with self.assertRaises(ValueError):
                write_json(path, {"metric": float("inf")})
            self.assertEqual(json.loads(path.read_text()), {"status": "old"})


if __name__ == "__main__":
    unittest.main()
