import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from threading import Barrier

from sqlalchemy import create_engine, event, select, update
from sqlalchemy.orm import sessionmaker

from durable_worker_example.config import Settings
from durable_worker_example.db.engine import Base
from durable_worker_example.db.models import Workspace, Document, FeatureArtifact, ComparisonRequest, TopComparison
from durable_worker_example.worker.runtime import Coordinator


class ClaimsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.engine = create_engine(f"sqlite:///{self.directory.name}/test.db", connect_args={"check_same_thread": False})
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(self.engine, expire_on_commit=False)
        self.workers = [Coordinator(self.sessions, Settings()) for _ in range(2)]
        with self.sessions.begin() as session:
            session.add(Workspace(id=1, name="test"))
            for i in (1, 2):
                session.add(Document(id=i, workspace_id=1, name=str(i), text="apple"))
                session.add(FeatureArtifact(id=i, workspace_id=1, document_id=i, status="pending" if i == 1 else "ready", feature_json={"apple": 1}))

    def tearDown(self):
        for worker in self.workers:
            worker._build_pool.shutdown()
            worker._comparison_pool.shutdown()
        self.engine.dispose()
        self.directory.cleanup()

    def race(self, method):
        barrier = Barrier(2)
        # Force both coordinators to read the same candidate before either
        # attempts the conditional UPDATE.
        update_barrier = Barrier(2)
        def before_update(conn, cursor, statement, parameters, context, executemany):
            if statement.startswith("UPDATE"):
                update_barrier.wait(timeout=10)
        event.listen(self.engine, "before_cursor_execute", before_update)
        def run(worker):
            barrier.wait()
            return getattr(worker, method)()
        try:
            with ThreadPoolExecutor(2) as pool:
                return list(pool.map(run, self.workers))
        finally:
            event.remove(self.engine, "before_cursor_execute", before_update)

    def test_competing_artifact_claims(self):
        self.assertFalse(self.workers[0]._supports_skip_locked)
        claims = self.race('_claim_artifact')
        self.assertEqual(sum(claim is not None for claim in claims), 1)

    def test_stale_artifact_cannot_finish_or_renew(self):
        first = self.workers[0]._claim_artifact()
        with self.sessions.begin() as session:
            session.execute(update(FeatureArtifact).where(FeatureArtifact.id == 1).values(lease_expires_at=datetime.utcnow() - timedelta(seconds=1)))
        second = self.workers[1]._claim_artifact()
        self.assertNotEqual(first[1], second[1])
        self.workers[0]._finish_artifact(1, first[1], {"wrong": 1})
        with self.sessions() as session:
            before = session.get(FeatureArtifact, 1).lease_expires_at
        self.workers[0]._renew_artifact_leases({object(): (1, first[1])})
        with self.sessions() as session:
            artifact = session.get(FeatureArtifact, 1)
            self.assertEqual(artifact.claim_token, second[1])
            self.assertEqual(artifact.lease_expires_at, before)
            self.assertEqual(artifact.status, "building")
        self.workers[1]._finish_artifact(1, second[1], {"correct": 1})
        with self.sessions() as session:
            self.assertEqual(session.get(FeatureArtifact, 1).feature_json, {"correct": 1})

    def prepare_comparison(self):
        with self.sessions.begin() as session:
            session.execute(update(FeatureArtifact).values(status="ready"))
            session.add(ComparisonRequest(id=1, workspace_id=1, query_artifact_id=1))

    def test_competing_comparison_claims(self):
        self.prepare_comparison()
        claims = self.race('_claim_comparison_page')
        self.assertEqual(sum(claim is not None for claim in claims), 1)

    def test_comparison_ownership_and_completion(self):
        self.prepare_comparison()
        first = self.workers[0]._claim_comparison_page()
        with self.sessions.begin() as session:
            session.execute(update(ComparisonRequest).values(lease_expires_at=datetime.utcnow() - timedelta(seconds=1)))
        second = self.workers[1]._claim_comparison_page()
        self.workers[0]._finish_comparison_page(1, first[1], [(2, 0.1)])
        with self.sessions() as session:
            self.assertEqual(session.get(ComparisonRequest, 1).candidates_scored_count, 0)
            self.assertEqual(list(session.scalars(select(TopComparison))), [])
        self.workers[1]._finish_comparison_page(1, second[1], [(2, 1.0)])
        self.workers[1]._finish_comparison_page(1, second[1], [(2, 0.2)])
        with self.sessions() as session:
            request = session.get(ComparisonRequest, 1)
            self.assertEqual(request.status, "ready")
            self.assertEqual(request.candidates_scored_count, 1)
            self.assertEqual(session.scalar(select(TopComparison)).score, 1.0)

    def test_comparison_failure_rolls_back_ownership(self):
        self.prepare_comparison()
        claim = self.workers[0]._claim_comparison_page()
        with self.assertRaises(ValueError):
            self.workers[0]._finish_comparison_page(1, claim[1], [])
        with self.sessions() as session:
            request = session.get(ComparisonRequest, 1)
            self.assertEqual(request.status, "scanning")
            self.assertEqual(request.claim_token, claim[1])
            self.assertEqual(request.candidates_scored_count, 0)


if __name__ == '__main__':
    unittest.main()
