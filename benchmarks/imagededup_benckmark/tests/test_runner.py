import json
import importlib.util
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

from imagededup_benckmark import run
from imagededup_benckmark.measurement import Measurement, distribution
from imagededup_benckmark.runtime import Stack, REPOSITORY, sqlite_database_url


class RunnerTest(unittest.TestCase):
    def test_both_original_engines_apply_identical_sqlite_busy_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            url = sqlite_database_url(Path(directory, "application.db"))
            self.assertTrue(url.endswith("?timeout=30"))
            engines = []
            for backend in ("dbwork", "redis_celery"):
                path = REPOSITORY / "examples" / f"imagededup_system_{backend}" / "src" / f"imagededup_system_{backend}" / "db/engine.py"
                specification = importlib.util.spec_from_file_location(f"image_timeout_test_{backend}", path)
                module = importlib.util.module_from_spec(specification)
                with patch.dict(sys.modules, {"imagededup_system_redis_celery.config": SimpleNamespace(settings=SimpleNamespace(database_url=url))}):
                    specification.loader.exec_module(module)
                    engine = module.get_engine() if backend == "redis_celery" else module.create_engine_and_session_factory(url)[0]
                engines.append(engine)
            try:
                for engine in engines:
                    with engine.connect() as connection:
                        self.assertEqual(connection.exec_driver_sql("PRAGMA busy_timeout").scalar(), 30000)
                        self.assertEqual(connection.exec_driver_sql("PRAGMA journal_mode").scalar(), "delete")
            finally:
                for engine in engines:
                    engine.dispose()

    def test_comparison_producers_use_independent_clients_and_preserve_input_order(self) -> None:
        clients = []
        class Client:
            def __init__(self):
                self.calls = []
                self.closed = False
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.closed = True
            def post(self, path, json):
                self.calls.append((path, json))
                key = int(path.rsplit("/", 1)[1])
                return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"id": key + 100})
        def new_client():
            client = Client()
            clients.append(client)
            return client
        load = run.submit_comparisons(SimpleNamespace(producer_client=new_client), list(range(1, 17)),
                                      producers=8, duration_seconds=.01, top_k=10, max_distance=8,
                                      measured_start=time.perf_counter())
        self.assertEqual([row["id"] for row in load["results"]], list(range(101, 117)))
        self.assertEqual(len(clients), 8)
        self.assertTrue(all(client.closed for client in clients))
        for index, client in enumerate(clients):
            self.assertEqual([call[0] for call in client.calls], [f"/comparisons/{index + 1}", f"/comparisons/{index + 9}"])
            self.assertTrue(all(call[1] == {"retained_max_k": 10, "max_distance": 8} for call in client.calls))

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
                    ALTER TABLE feature_artifact ADD COLUMN execution_status TEXT;
                    CREATE TABLE comparison_request (workspace_id INTEGER, candidates_scored_count INTEGER, execution_status TEXT);
                    INSERT INTO feature_artifact VALUES (1,'a','finished'),(1,'b','finished');
                    INSERT INTO comparison_request VALUES (1,1,'finished'),(1,1,'finished');''')
            stack = cast(Stack, SimpleNamespace(backend="redis_celery", database=database, business_commit_file=Path(directory, "commits.jsonl"), api_port=9, processes={}, check_alive=lambda: None))
            meter = Measurement(stack, 1, 2, 2, interval=.01, timeout=1)
            meter.positive_commits_observed = lambda: True
            meter.start()
            result = meter.finish(submission_finished=time.perf_counter())
            self.assertEqual(result["progress"][-1]["scored_pairs"], 2)
            self.assertEqual(result["progress"][-1]["completed_requests"], 2)
            self.assertFalse(meter.thread.is_alive())

    def test_counts_cannot_finish_before_terminal_business_status(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory, "test.db")
            with sqlite3.connect(database) as connection:
                connection.executescript('''CREATE TABLE feature_artifact (workspace_id INTEGER, hash_value TEXT, execution_status TEXT);
                    CREATE TABLE comparison_request (workspace_id INTEGER, candidates_scored_count INTEGER, execution_status TEXT);
                    INSERT INTO feature_artifact VALUES (1,'a','finished'),(1,'b','finished');
                    INSERT INTO comparison_request VALUES (1,1,'working'),(1,1,'finished');''')
            stack = cast(Stack, SimpleNamespace(backend="redis_celery", database=database, business_commit_file=Path(directory, "commits.jsonl"), api_port=9, processes={}, check_alive=lambda: None))
            meter = Measurement(stack, 1, 2, 2, interval=.01, timeout=.05)
            meter.positive_commits_observed = lambda: True
            meter.start()
            with self.assertRaisesRegex(TimeoutError, "Scenario exceeded"):
                meter.finish(submission_finished=time.perf_counter())
            self.assertIsNone(meter.finished)

    def test_failed_start_closes_resources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch("imagededup_benckmark.runtime.free_port", return_value=12345):
                stack = Stack("dbwork", Path(directory, "stack"), page_size=250, python=Path(sys.executable))
            with patch.object(stack, "start_process"), patch.object(stack, "wait_for_http", side_effect=RuntimeError("startup")):
                with self.assertRaisesRegex(RuntimeError, "startup"):
                    stack.__enter__()
            self.assertTrue(stack.client.is_closed)

    def test_dbworker_starts_api_and_worker_service_independently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch("imagededup_benckmark.runtime.free_port", return_value=12345):
                stack = Stack("dbwork", Path(directory, "stack"), page_size=250, python=Path(sys.executable))
            with patch.object(stack, "start_process") as start, patch.object(stack, "wait_for_http"):
                with stack:
                    self.assertEqual([call.args[0] for call in start.call_args_list], ["api", "worker_service"])
                    self.assertIn("imagededup_system_dbwork.main_worker_service", start.call_args_list[1].args[1])

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
            self.assertEqual(report["configuration"]["submission_window_seconds"], 60)
            self.assertIs(type(report["configuration"]["submission_window_seconds"]), int)
            self.assertFalse(output.with_suffix(".json.part").exists())

    def test_single_backend_skips_other_services(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for number in (1, 2):
                (root / f"im{number}.jpg").write_bytes(bytes([number]))
            with patch.object(run, "Stack") as stack, patch.object(run, "scenario") as scenario:
                stack.return_value.__enter__.return_value.startup_seconds = 0.0
                stack.return_value.__enter__.return_value.directory = root
                stack.return_value.__enter__.return_value.versions = {}
                scenario.side_effect = [({"backend": "dbwork", "scenario": kind,
                    "validation": {"passed": True}, "metrics": {"wall_seconds": 1.0}}, (1, [1, 2]))
                    for kind in ("build", "comparison", "mixed")]
                output = root / "result.json"
                run.main(["--backend", "dbwork", "--images", "2", "--warmup-images", "0",
                          "--repetitions", "1", "--dataset-dir", str(root), "--output", str(output)])
                self.assertEqual(stack.call_count, 1)
                self.assertEqual(stack.call_args.args[0], "dbwork")
                report = json.loads(output.read_text())
                self.assertEqual(set(report["summary"]["comparison"]), {"dbwork"})

    def test_latency_distribution(self) -> None:
        self.assertEqual(distribution([])["median"], None)
        result = distribution([.001, .002], scale=1000)
        self.assertEqual((result["median"], result["p95"], result["max"]), (1.5, 2, 2))
