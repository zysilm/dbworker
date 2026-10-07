"""Check durable SQL Lab dispatch preserves upstream task arguments and identity."""
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from examples.superset_dbworker.executor import Base, SQLLabDispatch, SQLLabJob


class NativeSQLLabDispatchTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.engine = create_engine(f"sqlite:///{Path(self.directory.name) / 'jobs.db'}")
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(self.engine)
        self.queries = {}
        superset = types.ModuleType("superset")
        superset.db = types.SimpleNamespace(session=types.SimpleNamespace(get=lambda model, key: self.queries.get(key)))
        models = types.ModuleType("superset.models")
        sql_lab = types.ModuleType("superset.models.sql_lab")
        sql_lab.Query = object
        self.modules = patch.dict(sys.modules, {"superset": superset, "superset.models": models,
                                               "superset.models.sql_lab": sql_lab})
        self.modules.start()

    def tearDown(self):
        self.modules.stop()
        self.engine.dispose()
        self.directory.cleanup()

    def test_one_durable_job_per_original_query_preserves_all_native_arguments(self):
        dispatch = SQLLabDispatch(self.sessions)
        native_arguments = {"return_results": False, "store_results": True,
                            "username": "fixture-user", "start_time": 123.5,
                            "expand_data": False, "log_params": {"user_agent": "fixture"}}
        for query_id in range(1, 101):
            self.queries[query_id] = types.SimpleNamespace(client_id=f"query-{query_id}")
            result = dispatch.delay(query_id, "SELECT 1", **native_arguments)
            result.forget()
        with self.sessions() as session:
            jobs = session.scalars(select(SQLLabJob).order_by(SQLLabJob.id)).all()
            self.assertEqual(len(jobs), 100)
            self.assertEqual(len({job.operation_id for job in jobs}), 100)
            for job in jobs:
                self.assertEqual(job.arguments, {"query_id": job.id, "rendered_query": "SELECT 1", **native_arguments})

    def test_duplicate_query_publication_is_rejected_instead_of_silently_folded(self):
        self.queries[1] = types.SimpleNamespace(client_id="query-1")
        dispatch = SQLLabDispatch(self.sessions)
        dispatch.delay(1, "SELECT 1", return_results=False, store_results=True)
        with self.assertRaises(IntegrityError):
            dispatch.delay(1, "SELECT 1", return_results=False, store_results=True)

    def test_missing_business_query_does_not_publish_a_job(self):
        with self.assertRaisesRegex(RuntimeError, "disappeared"):
            SQLLabDispatch(self.sessions).delay(99, "SELECT 1")
        with self.sessions() as session:
            self.assertEqual(session.scalars(select(SQLLabJob)).all(), [])

    def test_application_initialization_reuses_native_worker_flask_application(self):
        from examples.superset_dbworker.adapter import initialize

        tasks = types.ModuleType("superset.tasks")
        worker = types.ModuleType("superset.tasks.celery_app")
        worker.flask_app = object()
        initialize.cache_clear()
        try:
            with patch.dict(sys.modules, {"superset.tasks": tasks, "superset.tasks.celery_app": worker}):
                self.assertIs(initialize(), worker.flask_app)
                self.assertIs(initialize(), worker.flask_app)
        finally:
            initialize.cache_clear()


if __name__ == "__main__":
    unittest.main()
