"""Fail-closed native correctness checks execute before benchmark samples."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from benchmarks.upstream.suite import ROOT, main, run_native_correctness


class NativeCorrectnessHookTests(unittest.TestCase):
    def test_posthog_uses_pinned_interpreter_and_registers_log_once(self):
        interpreter = "/tmp/pinned-posthog/bin/python"
        config = {"interpreters": {"dbworker_python": interpreter}}
        report = {"artifacts": {}}
        output = Path("/tmp/native-correctness-output")
        with patch("benchmarks.upstream.suite.run_command", return_value=0) as command:
            run_native_correctness(config, report, output, "posthog")
        command.assert_called_once()
        args, kwargs = command.call_args
        self.assertEqual(args[0], [interpreter, "-m", "unittest",
                                  "benchmarks.tests.test_posthog_totp", "-v"])
        self.assertEqual(kwargs["env"]["POSTHOG_TOTP_TEST_PYTHON"], interpreter)
        self.assertEqual(kwargs["env"]["PYTHONPATH"], os.pathsep.join((str(ROOT), str(ROOT / "src"))))
        self.assertEqual(kwargs["cwd"], ROOT)
        self.assertEqual(kwargs["timeout"], 180)
        self.assertEqual(kwargs["log"], output / "posthog.totp-correctness.log")
        self.assertEqual(report["artifacts"]["correctness"], ["posthog.totp-correctness.log"])

    def test_native_correctness_failure_is_concrete_and_not_ignored(self):
        config = {"interpreters": {"dbworker_python": "/tmp/pinned-posthog/bin/python"}}
        report = {"artifacts": {}}
        with patch("benchmarks.upstream.suite.run_command", return_value=1):
            with self.assertRaisesRegex(RuntimeError,
                    r"native TOTP correctness exited with 1; see posthog.totp-correctness.log"):
                run_native_correctness(config, report, Path("/tmp/output"), "posthog")
        self.assertEqual(report["artifacts"]["correctness"], ["posthog.totp-correctness.log"])

    def test_other_suites_do_not_run_posthog_correctness(self):
        with patch("benchmarks.upstream.suite.run_command") as command:
            for suite in ("saleor", "paperless_ngx", "sentry", "superset", "imagededup"):
                run_native_correctness({}, {"artifacts": {}}, Path("/tmp/output"), suite)
        command.assert_not_called()

    def test_failed_preflight_stops_suite_before_any_backend_sample(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            report_path = output / "posthog.json"
            config_path = output / "config.json"
            report_path.write_text(json.dumps({"artifacts": {}, "runs": [], "errors": []}))
            config_path.write_text(json.dumps({"report_path": str(report_path),
                "output_directory": str(output), "profile": "full",
                "suite": {"suite_id": "posthog", "profiles": {"full": {"repetitions": 5, "requests": 100}}},
                "interpreters": {"dbworker_python": "/tmp/pinned/bin/python"}}))
            with patch.object(sys, "argv", ["suite.py", "--config", str(config_path)]), \
                    patch("benchmarks.upstream.suite.run_command", return_value=1) as command:
                self.assertEqual(main(), 1)
            command.assert_called_once()
            self.assertIn("benchmarks.tests.test_posthog_totp", command.call_args.args[0])
            report = json.loads(report_path.read_text())
            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["runs"], [])
            self.assertIn("posthog.totp-correctness.log", report["errors"][0]["message"])


if __name__ == "__main__":
    unittest.main()
