import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from benchmarks.common.reporting import validate_report
from benchmarks.run_all import main, run_suite


class OrchestrationTests(unittest.TestCase):
    def test_source_mutation_and_forged_source_evidence_fail_admission(self):
        for mutation in ("implementation", "evidence"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                child = root / "child.py"
                child.write_text("""import json, sys
from pathlib import Path
config = json.loads(Path(sys.argv[2]).read_text())
path = Path(config['report_path'])
report = json.loads(path.read_text())
report['status'] = 'passed'
report['runs'] = [
    {'scenario': 'fixture', 'comparison_mode': 'paired', 'backend': backend,
     'repetition': 1, 'status': 'passed', 'metrics': {'wall_seconds': 1},
     'validation': {'passed': True, 'output_digest': 'same'}}
    for backend in ('celery', 'dbworker')]
if config['suite']['mutation'] == 'evidence':
    report['source']['local_commit'] = 'forged'
else:
    source = Path(__file__)
    source.write_text(source.read_text() + '\\n# Changed during execution\\n')
path.write_text(json.dumps(report))
""")
                suite = {"suite_id": "fixture", "repository": "local", "source_path": ".",
                         "entrypoint": str(child), "mutation": mutation,
                         "interpreters": {role + "_python": sys.executable
                                          for role in ("benchmark", "celery", "dbworker")}}
                output = root / "results"
                output.mkdir()
                report = run_suite(suite, output=output, run_id="source-test", profile="full",
                                   overrides={}, timeout=30)
                self.assertEqual(report["status"], "failed")
                self.assertNotEqual(report["source"]["local_commit"], "forged")
                reason = report["errors"][-1]["message"]
                self.assertIn("source changed" if mutation == "implementation"
                              else "source evidence", reason)

    def test_failed_provisioning_preserves_every_suite_report_and_index(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = ("first", "second")
            suites = [{"suite_id": name, "repository": "local", "source_path": ".",
                       "entrypoint": "unused.py", "interpreters": {
                           role + "_python": sys.executable for role in ("benchmark", "celery", "dbworker")}}
                      for name in names]
            registry = root / "registry.json"
            registry.write_text(json.dumps({"schema_version": 1, "suites": suites}))
            output = root / "output"
            with patch("benchmarks.run_all.run_command", return_value=42) as command:
                self.assertEqual(main(["--registry", str(registry), "--output-dir", str(output), "--provision"]), 1)
            self.assertEqual(command.call_count, 2)
            for call in command.call_args_list:
                self.assertTrue(call.args[0][1].endswith("benchmarks/provision.py"))
            index = json.loads((output / "index.json").read_text())
            self.assertEqual(index["status"], "failed")
            self.assertEqual([entry["suite_id"] for entry in index["reports"]], list(names))
            for name in names:
                report = json.loads((output / f"{name}.json").read_text())
                validate_report(report)
                self.assertEqual(report["status"], "failed")
                self.assertEqual(report["artifacts"]["provision_log"], f"{name}.provision.log")
                self.assertIn("exit code 42", report["errors"][-1]["message"])

    def test_six_suite_subprocesses_run_in_registry_order_without_overlap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = root / "record_execution.py"
            child.write_text("""import json, os, sys, time
from pathlib import Path
config = json.loads(Path(sys.argv[2]).read_text())
output = Path(config['output_directory'])
identity = config['suite']['suite_id']
active = output / 'active-suite'
# Exclusive creation fails immediately if another experiment is still active.
descriptor = os.open(active, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
os.close(descriptor)
try:
    with (output / 'execution-order.jsonl').open('a') as log:
        log.write(json.dumps({'suite': identity, 'event': 'start', 'at': time.monotonic_ns()}) + '\\n')
    time.sleep(.1)
    report_path = Path(config['report_path'])
    report = json.loads(report_path.read_text())
    report['status'] = 'passed'
    report['runs'] = [
        {'scenario': 'stub', 'comparison_mode': 'paired', 'backend': backend,
         'repetition': 1, 'status': 'passed', 'metrics': {'wall_seconds': 1},
         'validation': {'passed': True, 'output_digest': 'same'}}
        for backend in ('celery', 'dbworker')]
    report_path.write_text(json.dumps(report))
    with (output / 'execution-order.jsonl').open('a') as log:
        log.write(json.dumps({'suite': identity, 'event': 'end', 'at': time.monotonic_ns()}) + '\\n')
finally:
    active.unlink()
""")
            names = ("imagededup", "superset", "saleor", "paperless_ngx", "posthog", "sentry")
            suites = [{"suite_id": name, "repository": "local", "source_path": ".",
                       "entrypoint": str(child), "interpreters": {
                           role + "_python": sys.executable for role in ("benchmark", "celery", "dbworker")}}
                      for name in names]
            registry = root / "registry.json"
            registry.write_text(json.dumps({"schema_version": 1, "suites": suites}))
            output = root / "output"
            self.assertEqual(main(["--registry", str(registry), "--output-dir", str(output)]), 0)
            events = [json.loads(line) for line in (output / "execution-order.jsonl").read_text().splitlines()]
            self.assertEqual([(event["suite"], event["event"]) for event in events],
                             [(name, event) for name in names for event in ("start", "end")])
            self.assertTrue(all(left["at"] < right["at"] for left, right in zip(events, events[1:])))
            index = json.loads((output / "index.json").read_text())
            self.assertTrue(index["complete_selection"])
            self.assertEqual([entry["suite_id"] for entry in index["reports"]], list(names))
            for name in names:
                validate_report(json.loads((output / f"{name}.json").read_text()))

    def test_missing_venvs_still_produce_one_report_per_suite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            suites = []
            for name in ("first", "second"):
                suites.append({"suite_id": name, "repository": "local", "source_path": ".",
                               "entrypoint": "unused.py", "interpreters": {
                                   role + "_python": str(root / "missing") for role in ("benchmark", "celery", "dbworker")}})
            registry = root / "registry.json"
            registry.write_text(json.dumps({"schema_version": 1, "suites": suites}))
            output = root / "output"
            self.assertEqual(main(["--registry", str(registry), "--output-dir", str(output)]), 1)
            index = json.loads((output / "index.json").read_text())
            self.assertEqual(len(index["reports"]), 2)
            for suite in suites:
                report = json.loads((output / (suite["suite_id"] + ".json")).read_text())
                validate_report(report)
                self.assertEqual(report["status"], "blocked")

    def test_malformed_child_output_does_not_break_the_public_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            child = root / "invalid.py"
            child.write_text("import json, sys\nfrom pathlib import Path\n"
                             "config=json.loads(Path(sys.argv[2]).read_text())\n"
                             "Path(config['report_path']).write_text('[1, 2]')\n")
            suite = {"suite_id": "malformed", "repository": "local", "source_path": ".",
                     "entrypoint": str(child), "interpreters": {
                         role + "_python": sys.executable for role in ("benchmark", "celery", "dbworker")}}
            report = run_suite(suite, output=root, run_id="one", profile="full", overrides={}, timeout=5)
            validate_report(report)
            self.assertEqual(report["status"], "failed")
            self.assertEqual((root / "malformed.invalid.json").read_text(), "[1, 2]")


if __name__ == "__main__":
    unittest.main()
