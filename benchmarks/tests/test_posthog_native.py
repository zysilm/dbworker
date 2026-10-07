"""Offline structural admission and durable business-job accounting checks."""
import json
import os
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch
from sqlalchemy import create_engine, inspect, select
from dbworker import ExecutionStatus, Finished, _execute_claim
from sqlalchemy.orm import sessionmaker
from benchmarks.common.native_admission import check_worker_source
from benchmarks.common.native_observer import job_context, record
from benchmarks.common.workflow_graph import read_trace, validate_graph
from examples.posthog_dbworker.runtime import Base, Job, coordinator, enqueue, handle, node
from examples.posthog_dbworker.adapter import route_delivery

ROOT = Path(__file__).resolve().parents[2]

class PostHogNativeTests(unittest.TestCase):
    def test_registered_ledger_exists_and_owned_handler_claim_commits(self):
        # Real source/ledger tables and coordinator claim execution; only the
        # upstream business callable is replaced by an offline test double.
        from examples.posthog_dbworker import adapter
        with tempfile.TemporaryDirectory() as temporary:
            runtime = coordinator(f'sqlite:///{Path(temporary) / "jobs.db"}')
            engine = runtime.session_factory.kw['bind']
            self.assertIn('posthog_workflow_work', inspect(engine).get_table_names())
            with runtime.session_factory.begin() as session:
                job = Job(operation_id='operation', stage='delivery', payload={'subject': 'original'})
                session.add(job)
                session.flush()
                identity = job.id
            trace = Path(temporary) / 'trace.jsonl'
            with patch.dict(os.environ, {'BENCHMARK_TRACE_PATH': str(trace)}), \
                    patch.object(adapter, 'initialize'), patch.object(adapter, 'deliver'), \
                    patch('django.db.close_old_connections'):
                worker = runtime.workers['posthog_workflow']
                claim = runtime.claim(worker)
                self.assertEqual(claim.source_id, identity)
                self.assertIsInstance(_execute_claim(worker, claim, runtime.session_factory), Finished)
                with runtime.session_factory() as session:
                    self.assertTrue(session.get(Job, identity).complete)
                    self.assertEqual(runtime.execution_status(session, worker='posthog_workflow', source_id=identity),
                                     ExecutionStatus.FINISHED)
                self.assertEqual([record['event'] for record in read_trace(trace)], ['started', 'succeeded'])
                self.assertIsNone(runtime.claim(worker))
            engine.dispose()

    def test_delivery_success_observation_waits_for_owned_commit(self):
        # Exercise the real handler's transaction boundary with a business seam
        # test double; this does not claim native SMTP execution.
        from examples.posthog_dbworker import adapter
        with tempfile.TemporaryDirectory() as temporary:
            trace = Path(temporary) / 'trace.jsonl'
            engine = create_engine('sqlite://')
            Base.metadata.create_all(engine)
            sessions = sessionmaker(engine)
            with sessions.begin() as session:
                job = Job(operation_id='operation', stage='delivery', payload={'subject': 'original'})
                session.add(job)
                session.flush()
                identity = job.id
            with patch.dict(os.environ, {'BENCHMARK_TRACE_PATH': str(trace)}), \
                    patch.object(adapter, 'initialize'), patch.object(adapter, 'deliver') as deliver, \
                    patch('django.db.close_old_connections'):
                with sessions() as session:
                    handle(session.get(Job, identity), session)
                    self.assertFalse(any(row['event'] == 'succeeded' for row in read_trace(trace)))
                    session.commit()
                    self.assertEqual([row['event'] for row in read_trace(trace)], ['started', 'succeeded'])
                deliver.assert_called_once_with({'subject': 'original'})
            engine.dispose()

    def test_both_stages_share_one_two_process_pool(self):
        runtime = coordinator("sqlite://", concurrency=2)
        try:
            self.assertEqual(list(runtime.workers), ["posthog_workflow"])
            worker = runtime.workers["posthog_workflow"]
            self.assertEqual(worker.concurrency, 2)
            self.assertIs(worker.source, Job)
            self.assertEqual(runtime._running, {})
        finally:
            runtime.session_factory.kw["bind"].dispose()

    def test_original_application_worker_and_unprojected_bootstrap(self):
        source = (ROOT / "benchmarks/upstream/posthog_backend.py").read_text()
        self.assertEqual(check_worker_source(source, "posthog")["worker_app"], "posthog.celery:app")
        bootstrap = (ROOT / "examples/posthog_dbworker/bootstrap.py").read_text()
        self.assertNotIn("ast", bootstrap)
        self.assertNotIn("types.ModuleType", bootstrap)
        self.assertIn("app.loader.import_default_modules()", bootstrap)

    def test_delivery_seam_preserves_payload_and_restores_native_publication(self):
        # A structural seam test double; no native business execution is claimed.
        class Task:
            def apply_async(self, args=None, kwargs=None, **options):
                raise AssertionError("Offline test attempted broker publication")
        task = Task()
        original = task.apply_async
        captured = []
        module = types.ModuleType("posthog.email")
        module._send_email = task
        payload = {"campaign_key": "native-generated", "to": [{"raw_email": "user@benchmark.invalid"}],
                   "html_body": "original rendered body", "use_http": True}
        with patch.dict("sys.modules", {"posthog.email": module}):
            with route_delivery(captured.append):
                task.apply_async(kwargs=payload)
                with self.assertRaises(ValueError):
                    task.apply_async(args=("unreviewed",))
            self.assertEqual(task.apply_async, original)
        self.assertEqual(captured, [payload])
        self.assertIsNot(captured[0], payload)

    def test_independent_durable_child_identity_and_observed_edges(self):
        # This tests persistence/accounting, not SMTP or upstream task execution.
        with tempfile.TemporaryDirectory() as temporary:
            trace = Path(temporary) / "trace.jsonl"
            engine = create_engine("sqlite://")
            Base.metadata.create_all(engine)
            sessions = sessionmaker(engine)
            with patch.dict(os.environ, {"BENCHMARK_TRACE_PATH": str(trace)}):
                with sessions.begin() as session:
                    enqueue(session, "operation-1", 23)
                with sessions() as session:
                    parent = session.scalar(select(Job))
                    self.assertEqual((parent.stage, parent.payload, parent.complete), ("notification", {"user_id": 23}, False))
                parent_id = node("operation-1", "notification")
                child_id = node("operation-1", "delivery")
                record("notification", "operation-1", parent_id, "started", backend="dbworker")
                with job_context("operation-1", parent_id):
                    with sessions.begin() as session:
                        session.add(Job(operation_id="operation-1", stage="delivery", parent_id=parent_id,
                                        payload={"campaign_key": "original-payload"}))
                    record("delivery", "operation-1", child_id, "submitted", parent_id, backend="dbworker")
                record("notification", "operation-1", parent_id, "succeeded", backend="dbworker")
                record("delivery", "operation-1", child_id, "started", parent_id, backend="dbworker")
                record("delivery", "operation-1", child_id, "succeeded", parent_id, backend="dbworker")
                graph = validate_graph(read_trace(trace), ["operation-1"], {"notification": 1, "delivery": 1}, [("notification", "delivery")])
                self.assertEqual(graph["nodes"], 2)
                self.assertEqual(graph["edge_counts"], [["notification", "delivery", 1]])
                with sessions() as session:
                    rows = session.scalars(select(Job).order_by(Job.id)).all()
                    self.assertEqual(len(rows), 2)
                    self.assertNotEqual(rows[0].id, rows[1].id)
                    self.assertEqual(rows[1].parent_id, parent_id)
            engine.dispose()

if __name__ == "__main__":
    unittest.main()
