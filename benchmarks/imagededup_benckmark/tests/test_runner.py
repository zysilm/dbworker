import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from imagededup_benckmark import run
from imagededup_benckmark.measurement import Measurement, distribution
from imagededup_benckmark.runtime import Stack


class RunnerTest(unittest.TestCase):
    def test_dataset_selection_is_exact_and_missing_images_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for number in (1, 2, 3, 10):
                (root / f"im{number}.jpg").write_bytes(str(number).encode())
            images = run.select_images(root, 3)
            self.assertEqual([path.name for path in images], ["im1.jpg", "im2.jpg", "im3.jpg"])
            prepared = root / "inputs"
            manifest = run.prepare_images(images, prepared)
            self.assertEqual([p.read_bytes() for p in sorted(prepared.iterdir())], [b"1", b"2", b"3"])
            self.assertEqual(len(manifest), 3)
            with self.assertRaises(FileNotFoundError):
                run.select_images(root, 4)

    def test_progress_uses_committed_counts_and_stops(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory, "test.db")
            with sqlite3.connect(database) as connection:
                connection.executescript('''CREATE TABLE feature_artifact (workspace_id INTEGER, hash_value TEXT);
                    CREATE TABLE comparison_request (workspace_id INTEGER, candidates_scored_count INTEGER);
                    INSERT INTO feature_artifact VALUES (1,'a'),(1,'b');
                    INSERT INTO comparison_request VALUES (1,1),(1,1);''')
            stack = cast(Stack, SimpleNamespace(database=database, api_port=9, processes={}, check_alive=lambda: None))
            meter = Measurement(stack, 1, 2, 2, interval=.01, timeout=1)
            meter.start()
            result = meter.finish()
            self.assertEqual(result["progress"][-1]["scored_pairs"], 2)
            self.assertEqual(result["progress"][-1]["completed_requests"], 2)
            self.assertFalse(meter.thread.is_alive())

    def test_failed_start_closes_resources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            stack = Stack("dbwork", Path(directory, "stack"), page_size=250, python=Path(sys.executable))
            with patch.object(stack, "start_process"), patch.object(stack, "wait_for_http", side_effect=RuntimeError("startup")):
                with self.assertRaisesRegex(RuntimeError, "startup"):
                    stack.__enter__()
            self.assertTrue(stack.client.is_closed)

    def test_stacks_and_scenarios_are_sequential_and_order_alternates(self) -> None:
        active: list[str] = []
        entered: list[str] = []
        phases: list[str] = []

        class FakeStack:
            def __init__(self, backend: str, directory: Path, **kwargs: Any) -> None:
                self.backend, self.directory, self.startup_seconds = backend, directory, 0.0
                self.versions: dict[str, str] = {}

            def __enter__(inner) -> Any:
                self.assertEqual(active, [], "Two benchmark stacks overlapped")
                active.append(inner.backend)
                entered.append(inner.backend)
                return inner

            def __exit__(inner, *args: object) -> None:
                active.clear()

        def fake_scenario(stack: FakeStack, kind: str, *args: Any, **kwargs: Any) -> Any:
            phases.append(kind)
            return {"backend": stack.backend, "scenario": kind, "validation": {"passed": True},
                    "metrics": {"wall_seconds": 1.0}}, (1, [1, 2])

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for number in (1, 2):
                (root / f"im{number}.jpg").write_bytes(bytes([number]))
            output = root / "result.json"
            with patch.object(run, "Stack", FakeStack), patch.object(run, "scenario", side_effect=fake_scenario), \
                 patch.object(run.platform, "platform", return_value="test-platform"):
                run.main(["--images", "2", "--warmup-images", "0", "--repetitions", "2",
                          "--dataset-dir", str(root), "--work-dir", str(root / "work"), "--output", str(output)])
            report = json.loads(output.read_text())
            self.assertEqual(entered, ["dbwork", "redis_celery", "redis_celery", "dbwork"])
            self.assertEqual(phases, ["build", "comparison", "mixed"] * 4)
            self.assertEqual(report["status"], "passed")
            self.assertEqual(len(report["runs"]), 12)
            self.assertEqual(report["configuration"]["build_workers"], 4)
            self.assertEqual(report["configuration"]["comparison_workers"], 4)
            self.assertFalse(output.with_suffix(".json.part").exists())

    def test_latency_distribution(self) -> None:
        self.assertEqual(distribution([])["median"], None)
        result = distribution([.001, .002], scale=1000)
        self.assertEqual((result["median"], result["p95"], result["max"]), (1.5, 2, 2))
