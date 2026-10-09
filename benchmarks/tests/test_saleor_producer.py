"""Regression checks for the original authenticated Saleor producer boundary."""
from __future__ import annotations

import ast
import base64
from pathlib import Path
from unittest import TestCase
from unittest.mock import Mock

from examples.saleor_dbworker.producer import export_input, export_key, post_export

ROOT = Path(__file__).resolve().parents[2]


class SaleorProducerTests(TestCase):
    def test_request_runs_public_authenticated_graphql_endpoint(self):
        client = Mock()
        client.post.return_value.status_code = 200
        content = {"data": {"exportProducts": {"exportFile": {"id": "RXhwb3J0RmlsZTox"}, "errors": []}}}
        client.post.return_value.json.return_value = content
        self.assertIs(post_export(client, "original-jwt", ["UHJvZHVjdDox"]), content)
        arguments, options = client.post.call_args
        self.assertEqual(arguments[0], "/graphql/")
        self.assertEqual(options["HTTP_AUTHORIZATION"], "JWT original-jwt")
        self.assertEqual(options["HTTP_HOST"], "localhost")
        self.assertEqual(options["content_type"], "application/json")
        self.assertEqual(arguments[1]["variables"]["input"], {
            "scope": "IDS", "ids": ["UHJvZHVjdDox"],
            "exportInfo": {"fields": ["NAME", "PRODUCT_TYPE", "VARIANT_SKU"]}, "fileType": "CSV"})
        self.assertIn("exportProducts(input: $input)", arguments[1]["query"])
        self.assertEqual(export_key(content), 1)

    def test_public_input_does_not_pre_normalize_original_task_arguments(self):
        ids = ["UHJvZHVjdDox"]
        value = export_input(ids)
        ids.append("UHJvZHVjdDoy")
        self.assertEqual(value["ids"], ["UHJvZHVjdDox"])
        self.assertEqual(value["exportInfo"]["fields"], ["NAME", "PRODUCT_TYPE", "VARIANT_SKU"])
        self.assertNotIn("export_file_id", value)

    def test_rejects_protocol_business_and_export_identity_errors(self):
        def content(identity):
            return {"data": {"exportProducts": {"exportFile": {"id": identity}, "errors": []}}}
        bad = [
            {"errors": [{"extensions": {"exception": {"code": "PermissionDenied"}}}]},
            {"data": {"exportProducts": {"errors": [{"code": "INVALID"}]}}},
            {}, content(None), content("bad-base64"),
            content(base64.b64encode(b"Product:1").decode()),
            content(base64.b64encode(b"ExportFile:0").decode()),
        ]
        for value in bad:
            with self.subTest(value=value), self.assertRaises(AssertionError):
                export_key(value)

    def test_rejects_http_failure(self):
        client = Mock()
        client.post.return_value.status_code = 403
        with self.assertRaisesRegex(AssertionError, "HTTP 403"):
            post_export(client, "jwt", ["UHJvZHVjdDox"])

    def test_controller_does_not_recreate_or_directly_publish_native_producer(self):
        tree = ast.parse((ROOT / "benchmarks/upstream/saleor_backend.py").read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                function = ast.unparse(node.func)
                self.assertNotEqual(function, "ExportFile.objects.create")
                self.assertNotEqual(function, "export_started_event")
                self.assertNotEqual(function, "export_products_task.delay")
                self.assertNotEqual(function, "enqueue")
        source = (ROOT / "benchmarks/upstream/saleor_backend.py").read_text()
        self.assertIn('Permission.objects.get(codename="manage_products"', source)
        self.assertIn('user.user_permissions.add(permission)', source)
        self.assertIn('post_export(graphql_client, tokens[index], product_ids)', source)
        self.assertIn('with durable_children(op, None):', source)


class SaleorDurableProducerTests(TestCase):
    def setUp(self):
        try:
            import celery  # noqa: F401
            import sqlalchemy  # noqa: F401
            import dbworker  # noqa: F401
        except ImportError:
            self.skipTest("Durable adapter tests require the CI admission dependencies")

    def test_original_task_delay_is_intercepted_with_original_arguments(self):
        from celery.app.task import Task
        from examples.saleor_dbworker.runtime import durable_children
        from unittest.mock import patch
        task = Task()
        task.name = "export-products"
        original = Task.apply_async
        arguments = (42, {"ids": ["1", "2"]}, {"fields": ["name"]}, "csv")
        with patch("examples.saleor_dbworker.runtime.enqueue") as enqueue:
            with durable_children("0", None):
                task.delay(*arguments)
            enqueue.assert_called_once_with("export-products", arguments, {}, "0", None)
        self.assertIs(Task.apply_async, original)

    def test_unsupported_delayed_producer_fails_closed_and_restores_dispatch(self):
        from celery.app.task import Task
        from examples.saleor_dbworker.runtime import durable_children
        task = Task()
        task.name = "export-products"
        original = Task.apply_async
        with self.assertRaisesRegex(RuntimeError, "Unsupported delayed"):
            with durable_children("0", None):
                task.apply_async(args=(42,), countdown=1)
        self.assertIs(Task.apply_async, original)

    def test_submission_intent_precedes_durable_row_visibility(self):
        import json
        import os
        import tempfile
        from unittest.mock import patch
        from sqlalchemy import create_engine, select
        from sqlalchemy.orm import sessionmaker
        from examples.saleor_dbworker.runtime import Base, Job, enqueue
        observed = []
        with tempfile.TemporaryDirectory() as temporary:
            url = f"sqlite:///{temporary}/queue.db"
            engine = create_engine(url)
            Base.metadata.create_all(engine)

            def intent(stage, operation_id, identity, phase, parent, **details):
                with sessionmaker(engine)() as session:
                    self.assertEqual(session.scalars(select(Job)).all(), [])
                observed.append((stage, operation_id, identity, phase, parent, details))

            try:
                with patch.dict(os.environ, {"DBWORKER_DATABASE_URL": url}), patch(
                        "examples.saleor_dbworker.runtime.record", side_effect=intent):
                    result = enqueue("export-products", (42, {"ids": ["1"]}, {}, "csv"), {}, "0")
                with sessionmaker(engine)() as session:
                    job = session.scalars(select(Job)).one()
                    self.assertEqual(job.task_id, result.id)
                    self.assertEqual(json.loads(job.payload), [[42, {"ids": ["1"]}, {}, "csv"], {}])
                self.assertEqual(len(observed), 1)
                self.assertEqual(observed[0][:2], ("export", "0"))
                self.assertEqual(observed[0][3:5], ("submitted", None))
                self.assertEqual(observed[0][5]["task_name"], "export-products")
            finally:
                engine.dispose()

    def test_handler_success_waits_for_coordinator_commit(self):
        import json
        import sys
        from types import ModuleType, SimpleNamespace
        from unittest.mock import patch
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker
        from examples.saleor_dbworker.runtime import handle

        class OriginalTask:
            name = "export-products"
            called = None

            def __call__(self, *args, **kwargs):
                self.called = (args, kwargs)
                return "original-result"

            def on_success(self, result, identity, args, kwargs):
                self.success = (result, identity, args, kwargs)

        task = OriginalTask()
        django = ModuleType("django")
        django_db = ModuleType("django.db")
        django_db.close_old_connections = Mock()
        django.db = django_db
        native = ModuleType("saleor.celeryconf")
        native.app = SimpleNamespace(tasks={task.name: task})
        engine = create_engine("sqlite://")
        source = SimpleNamespace(task_id="native-identity", operation_id="0", parent_id=None,
                                 task_name=task.name, payload=json.dumps([[42, {"ids": ["1"]}, {}, "csv"], {}]))
        try:
            with sessionmaker(engine)() as session, patch.dict(sys.modules, {
                    "django": django, "django.db": django_db, "saleor.celeryconf": native}), patch(
                    "examples.saleor_dbworker.adapter.initialize"), patch(
                    "examples.saleor_dbworker.runtime.worker_origin", return_value={"passed": True}), patch(
                    "examples.saleor_dbworker.runtime.record") as record:
                handle(source, session)
                self.assertEqual(task.called, ((42, {"ids": ["1"]}, {}, "csv"), {}))
                self.assertEqual([call.args[3] for call in record.call_args_list], ["started"])
                session.commit()
                self.assertEqual([call.args[3] for call in record.call_args_list], ["started", "succeeded"])
        finally:
            engine.dispose()
