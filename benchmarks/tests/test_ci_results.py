import copy
import json
import tempfile
import unittest
from pathlib import Path

from benchmarks.ci_results import combine, package
from benchmarks.common.reporting import digest, write_json
from benchmarks.render_results import update_readme
from benchmarks.tests.test_reporting import passed_report


class MatrixResultsTests(unittest.TestCase):
    def fixtures(self, root):
        incoming = root / "incoming"
        names = ["first", "second"]
        suites = []
        for name in names:
            profile = {"requests": 100, "repetitions": 5}
            suites.append({"suite_id": name, "profiles": {"full": profile}, "commit": "pinned"})
            report = passed_report()
            report.update(suite_id=name, run_id="shared")
            report["source"].update(local_commit="revision", commit="pinned")
            report["environment"]["runner_class"] = "github-hosted-docker"
            report["configuration"]["profile"] = profile
            first = copy.deepcopy(report["runs"])
            report["runs"] = []
            for repetition in range(1, 6):
                rows = copy.deepcopy(first)
                for row in rows:
                    row["repetition"] = repetition
                report["runs"].extend(rows)
            directory = incoming / name
            path = directory / f"{name}.json"
            write_json(path, report)
            index = {"schema_version": 1, "run_id": "shared", "profile": "full", "status": "passed",
                     "complete_selection": False, "required_suites": names,
                     "reports": [{"suite_id": name, "path": path.name, "status": "passed", "sha256": digest(path)}]}
            write_json(directory / "index.json", index)
        registry = root / "registry.json"
        write_json(registry, {"suites": suites})
        return incoming, registry

    def test_complete_matrix_and_readme_replacement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            incoming, registry = self.fixtures(root)
            output = root / "results"
            combine(incoming, output, run_id="shared", commit="revision", registry=registry)
            self.assertTrue(json.loads((output / "index.json").read_text())["complete_selection"])
            readme = root / "README.md"
            readme.write_text("# Project\n\nExisting content.\n")
            content = update_readme(output, readme)
            self.assertIn("Existing content.", content)
            self.assertIn("**1.000**", content)
            self.assertIn("| first | export | 2.000 | **1.000** | 2.00 |", content)
            readme.write_text(content)
            self.assertEqual(update_readme(output, readme), content)
            self.assertEqual(content.count("## Benchmark Results"), 1)

    def test_rejects_missing_tampered_stale_and_reduced_workloads(self):
        for mutation in ("missing", "checksum", "revision", "profile", "run_id", "failed", "repetitions"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                incoming, registry = self.fixtures(root)
                path = incoming / "first/first.json"
                if mutation == "missing":
                    (incoming / "first/index.json").unlink()
                elif mutation == "checksum":
                    path.write_text(path.read_text() + " ")
                else:
                    report = json.loads(path.read_text())
                    if mutation == "revision":
                        report["source"]["local_commit"] = "other"
                    elif mutation == "profile":
                        report["configuration"]["profile"]["requests"] = 4
                    elif mutation == "run_id":
                        report["run_id"] = "other"
                    elif mutation == "failed":
                        report["status"] = "failed"
                    else:
                        report["runs"] = report["runs"][:2]
                    write_json(path, report)
                    index_path = incoming / "first/index.json"
                    index = json.loads(index_path.read_text())
                    index["reports"][0]["sha256"] = digest(path)
                    write_json(index_path, index)
                with self.assertRaises((ValueError, FileNotFoundError)):
                    combine(incoming, root / "results", run_id="shared", commit="revision", registry=registry)
                self.assertFalse((root / "results").exists())

    def test_packaging_excludes_runtime_databases_configs_and_media(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source"
            for name in ["index.json", "first.json", "first.log", "first.config.json",
                         "private/sample.json", "private/request.db", "private/document.pdf"]:
                path = source / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("{}")
            output = root / "artifact"
            package(source, output, "first")
            self.assertEqual({str(p.relative_to(output)) for p in output.rglob("*") if p.is_file()},
                             {"index.json", "first.json", "first.log", "private/sample.json"})


if __name__ == "__main__":
    unittest.main()
