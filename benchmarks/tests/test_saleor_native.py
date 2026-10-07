"""Check native baseline origin and one-for-one durable child publication."""

from __future__ import annotations

import ast
import datetime
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from celery.app.task import Task
from kombu.serialization import loads
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from benchmarks.common.native_admission import check_worker_source
from examples.saleor_dbworker.runtime import Base, Job, STAGES, durable_children, enqueue

ROOT = Path(__file__).resolve().parents[2]


class SaleorNativeTests(unittest.TestCase):
    def test_clean_provisioning_installs_smtp_receiver_in_both_backend_environments(self):
        from benchmarks.provision import provision

        registry = json.loads((ROOT / "benchmarks/registry.json").read_text())
        suite = next(item for item in registry["suites"] if item["suite_id"] == "saleor")
        with patch("benchmarks.provision.run") as invoke:
            provision(["saleor"])
        installs = [call.args for call in invoke.call_args_list if call.args[1:3] == ("pip", "install")]
        for backend in ("celery", "dbworker"):
            interpreter = str(ROOT / suite["interpreters"][f"{backend}_python"])
            selected = [command for command in installs if command[command.index("--python") + 1] == interpreter]
            self.assertEqual(len(selected), 1)
            # Constraints alone do not install the receiver. An explicit request
            # is required for a fresh environment, without manually copied packages.
            self.assertIn("aiosmtpd", selected[0])
            self.assertIn("examples/saleor", selected[0])
            self.assertIn(".", selected[0])
            self.assertIn("benchmarks/locks/saleor.txt", selected[0])

    def test_native_worker_and_settings_are_preserved(self):
        evidence = check_worker_source((ROOT / "benchmarks/upstream/saleor_backend.py").read_text(), "saleor")
        self.assertEqual(evidence["worker_app"], "saleor.celeryconf:app")
        tree = ast.parse((ROOT / "examples/saleor_dbworker/benchmark_settings.py").read_text())
        assigned = {target.id for node in tree.body if isinstance(node, ast.Assign)
                    for target in node.targets if isinstance(target, ast.Name)}
        self.assertNotIn("PLUGINS", assigned)
        self.assertNotIn("CELERY_RESTRICT_WRITER_METHOD", assigned)

    def test_each_child_is_individually_durable_and_never_executed_inline(self):
        with tempfile.TemporaryDirectory() as temporary:
            url = "sqlite:///" + str(Path(temporary) / "jobs.db")
            trace = Path(temporary) / "trace.jsonl"
            engine = create_engine(url)
            Base.metadata.create_all(engine)
            email_name = next(name for name, stage in STAGES.items() if stage == "email")
            task = Task()
            task.name = email_name
            payload = {"export": {"id": "RXhwb3J0RmlsZTox", "created_at": datetime.datetime(2026, 1, 1)}}
            with patch.dict(os.environ, DBWORKER_DATABASE_URL=url, BENCHMARK_TRACE_PATH=str(trace)):
                with patch.object(Task, "run", side_effect=AssertionError("Child executed synchronously")):
                    with durable_children("operation-1", "parent-1"):
                        first = task.delay("user@example.test", payload, {}, "subject", "template")
                        second = task.delay("user@example.test", payload, {}, "subject", "template")
                with sessionmaker(engine)() as session:
                    rows = session.scalars(select(Job).order_by(Job.id)).all()
                    self.assertEqual(len(rows), 2)
                    self.assertEqual({row.task_id for row in rows}, {first.id, second.id})
                    for row in rows:
                        self.assertEqual(row.operation_id, "operation-1")
                        self.assertEqual(row.parent_id, "parent-1")
                        args, kwargs = loads(row.payload, "application/json", "utf-8")
                        self.assertEqual(args[1], payload)
                        self.assertEqual(kwargs, {})
                events = [json.loads(line) for line in trace.read_text().splitlines()]
                self.assertEqual([row["event"] for row in events], ["submitted", "submitted"])
                self.assertEqual([row["stage"] for row in events], ["email", "email"])
            engine.dispose()

    def test_unsupported_continuations_fail_instead_of_being_skipped(self):
        with self.assertRaisesRegex(RuntimeError, "Unsupported Saleor continuation"):
            enqueue("unknown.webhook.task", (), {}, "operation-1")
        task = Task()
        task.name = "export-products"
        with durable_children("operation-1", "parent-1"):
            with self.assertRaisesRegex(RuntimeError, "Unsupported delayed"):
                task.apply_async(args=(), countdown=1)


if __name__ == "__main__":
    unittest.main()
