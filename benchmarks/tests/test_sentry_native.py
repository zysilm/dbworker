"""Offline checks for native email fan-out transport and fail-closed admission."""

import json
import os
import pickle
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from celery import Celery
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from benchmarks.common import native_observer as observer
from benchmarks.common.native_admission import check_worker_source
from benchmarks.common.workflow_graph import read_trace, validate_graph
from benchmarks.upstream.sentry_backend import preflight, recipients, xml_library_linkage
from examples.sentry_dbworker import adapter


class SentryNativeTest(unittest.TestCase):
    def test_xml_linkage_admits_shared_library_and_rejects_mixed_builds(self):
        etree = SimpleNamespace(LIBXML_COMPILED_VERSION=(2, 9, 14), LIBXML_VERSION=(2, 9, 14))
        xmlsec = SimpleNamespace(get_libxml_compiled_version=lambda: (2, 9, 14),
            get_libxml_version=lambda: (2, 9, 14), get_libxmlsec_version=lambda: (1, 2, 39))
        with patch.dict("sys.modules", {"lxml": SimpleNamespace(etree=etree), "xmlsec": xmlsec}):
            self.assertEqual(xml_library_linkage()["libxml2_versions"]["xmlsec_runtime"], (2, 9, 14))
            etree.LIBXML_COMPILED_VERSION = (2, 14, 5)
            with self.assertRaisesRegex(RuntimeError, "share system libxml2"):
                xml_library_linkage()

    def test_worker_command_names_original_app_and_fixture_two_distinct_recipients(self):
        source = Path("benchmarks/upstream/sentry_backend.py").read_text()
        evidence = check_worker_source(source, "sentry")
        self.assertEqual(evidence["worker_app"], "sentry.celery:app")
        self.assertEqual(len(set(recipients("0"))), 2)
        self.assertNotIn("execute_request", source)
        self.assertNotIn("legacy_send", source)

    def test_missing_native_dependency_leaves_structured_blocker(self):
        original_import = __import__
        def import_without_django(name, *args, **kwargs):
            if name == "django":
                raise ModuleNotFoundError("No module named 'django'")
            return original_import(name, *args, **kwargs)
        with tempfile.TemporaryDirectory() as directory, patch("builtins.__import__", side_effect=import_without_django):
            with self.assertRaisesRegex(RuntimeError, "runtime admission blocked"):
                preflight(Path(directory), "dbworker")
            evidence = json.loads(Path(directory, "admission.json").read_text())
            self.assertEqual(evidence["status"], "blocked")
            self.assertFalse(evidence["workflow_parity"])
            self.assertEqual(evidence["error"]["type"], "ModuleNotFoundError")

    def test_protocol_one_publication_keeps_two_independent_delivery_jobs(self):
        app = Celery("email-transport-unit-test", broker="redis://127.0.0.1:1/0")
        app.conf.update(task_protocol=1, task_serializer="pickle", accept_content=["pickle"], task_ignore_result=True)
        delivered = []
        @app.task(name="sentry.tasks.email.send_email", ignore_result=True)
        def fixture_email(message):
            delivered.append(message.to[0])
        with tempfile.TemporaryDirectory() as directory:
            engine = create_engine(f"sqlite:///{Path(directory, 'delivery.db')}")
            adapter.Base.metadata.create_all(engine)
            sessions = sessionmaker(engine, expire_on_commit=False)
            trace = Path(directory, "workflow.jsonl")
            environment = {"BENCHMARK_TRACE_PATH": str(trace), "BENCHMARK_BACKEND": "dbworker",
                           "BENCHMARK_TASK_STAGES": json.dumps({fixture_email.name: "delivery"})}
            with patch.dict(os.environ, environment), adapter.publication_to_dbworker(sessions) as (published, errors):
                with observer.operation("0"):
                    for index, recipient in enumerate(recipients("0")):
                        message = SimpleNamespace(to=[recipient], extra_headers={"X-Benchmark": "0"})
                        fixture_email.apply_async(kwargs={"message": message}, task_id=f"delivery-{index}")
                self.assertEqual(len(published), 2)
                self.assertEqual(errors, [])
                with sessions() as session:
                    jobs = list(session.scalars(select(adapter.Delivery).order_by(adapter.Delivery.id)))
                    self.assertEqual([job.recipient for job in jobs], recipients("0"))
                    self.assertEqual([pickle.loads(job.envelope)["id"] for job in jobs], ["delivery-0", "delivery-1"])
                native_module = SimpleNamespace(app=app)
                with patch.object(adapter, "initialize"), patch.dict("sys.modules", {"sentry.celery": native_module}):
                    for identity in ("delivery-0", "delivery-1"):
                        with sessions() as session:
                            job = session.get(adapter.Delivery, identity)
                            adapter.execute(job, session)
                            before = read_trace(trace)
                            self.assertFalse(any(row["node_id"] == identity and row["event"] == "succeeded" for row in before))
                            session.commit()
                graph = validate_graph(read_trace(trace), ["0"], {"delivery": 2}, [])
                self.assertEqual(graph["nodes"], 2)
                self.assertEqual(sorted(delivered), recipients("0"))
            engine.dispose()
            app.close()

    def test_publication_rejects_recipient_batching_and_restores_transport(self):
        from kombu import Producer
        with tempfile.TemporaryDirectory() as directory:
            engine = create_engine(f"sqlite:///{Path(directory, 'delivery.db')}")
            adapter.Base.metadata.create_all(engine)
            sessions = sessionmaker(engine)
            original = Producer.publish
            with adapter.publication_to_dbworker(sessions) as (published, errors):
                message = SimpleNamespace(to=recipients("0"), extra_headers={"X-Benchmark": "0"})
                with self.assertRaisesRegex(ValueError, "one recipient"):
                    Producer.publish(None, {"id": "batched", "task": "sentry.tasks.email.send_email",
                                            "kwargs": {"message": message}}, serializer="pickle")
                self.assertEqual(published, [])
                self.assertEqual(len(errors), 1)
            self.assertIs(Producer.publish, original)
            engine.dispose()
