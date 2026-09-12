import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from threading import Barrier
from typing import Any

from sqlalchemy import Connection, String, create_engine, event, select, update
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker

from durable_worker_example.config import Settings
from durable_worker_example.db.engine import Base
from durable_worker_example.db.models import Workspace, Document, FeatureArtifact, ComparisonRequest, ScoredCandidate, TopComparison
from durable_worker_example.domain.workflows import create_workers
from durable_worker_example.worker.runtime import Coordinator, Finished, Unfinished, Worker, Claim, Context, LostClaim, Outcome, now


class ExternalSource(Base):
    __tablename__ = "test_external_source"
    key: Mapped[str] = mapped_column(String(36), primary_key=True)


def no_database_output(source: ExternalSource, context: Context[ExternalSource]) -> Finished:
    return Finished()


class ClaimsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.engine = create_engine(f"sqlite:///{self.directory.name}/test.db", connect_args={"check_same_thread": False})
        self.session_factory = sessionmaker(self.engine, expire_on_commit=False)
        self.builds, self.comparisons = create_workers(replace(Settings(), comparison_page_size=1))
        Base.metadata.create_all(self.engine)
        self.coordinators = [Coordinator(self.session_factory) for _ in range(2)]
        for coordinator in self.coordinators:
            coordinator.register(self.builds)
            coordinator.register(self.comparisons)
            coordinator.create_worker_tables()
        # Unit tests run pure CPU functions in threads; a separate smoke test
        # exercises the production process pools.
        self.cpu = ThreadPoolExecutor(2)
        with self.session_factory.begin() as session:
            session.add(Workspace(id=1, name="test"))
            for i in (1, 2):
                session.add(Document(id=i, workspace_id=1, name=str(i), text="apple"))
                session.add(FeatureArtifact(id=i, workspace_id=1, document_id=i, feature_json=None if i == 1 else {"apple": 1}))

    def tearDown(self) -> None:
        for coordinator in self.coordinators:
            coordinator.stop()
        self.cpu.shutdown()
        self.engine.dispose()
        self.directory.cleanup()

    def run_work(self, worker: Worker[Any], coordinator: int = 0) -> Outcome:
        runtime = self.coordinators[coordinator]
        claim = runtime.claim(worker)
        self.assertIsNotNone(claim)
        return runtime.execute_claim(worker, claim, self.cpu)

    def expire(self, worker: Worker[Any]) -> None:
        with self.session_factory.begin() as session:
            session.execute(update(worker.table).values(lease_expires_at=now() - timedelta(seconds=1)))

    def prepare_comparison(self) -> None:
        self.run_work(self.builds)
        with self.session_factory.begin() as session:
            session.add(ComparisonRequest(id=1, workspace_id=1, query_artifact_id=1, retained_max_k=1))

    def race(self, worker: Worker[Any], statement_prefix: str) -> list[Claim | None]:
        barrier = Barrier(2)
        def before_write(conn: Connection, cursor: object, statement: str, parameters: object, context: object, executemany: bool) -> None:
            if statement.startswith(statement_prefix):
                barrier.wait(timeout=10)
        event.listen(self.engine, "before_cursor_execute", before_write)
        try:
            with ThreadPoolExecutor(2) as pool:
                return list(pool.map(lambda coordinator: coordinator.claim(worker), self.coordinators))
        finally:
            event.remove(self.engine, "before_cursor_execute", before_write)

    def test_simultaneous_first_claim(self) -> None:
        claims = self.race(self.builds, "INSERT INTO artifact_build_work")
        self.assertEqual(sum(c is not None for c in claims), 1)

    def test_simultaneous_reclaim(self) -> None:
        self.coordinators[0].claim(self.builds)
        self.expire(self.builds)
        claims = self.race(self.builds, "UPDATE artifact_build_work")
        self.assertEqual(sum(c is not None for c in claims), 1)

    def test_stale_result_and_renewal(self) -> None:
        first = self.coordinators[0].claim(self.builds)
        self.expire(self.builds)
        second = self.coordinators[1].claim(self.builds)
        with self.assertRaises(LostClaim):
            self.coordinators[0].execute_claim(self.builds, first, self.cpu)
        self.coordinators[0].renew(self.builds, [first])
        with self.session_factory() as session:
            self.assertIsNone(session.get(FeatureArtifact, 1).feature_json)
            self.assertEqual(self.builds.state(session, 1)["claim_token"], second.token)
        self.coordinators[1].execute_claim(self.builds, second, self.cpu)
        self.assertIsNone(self.coordinators[0].claim(self.builds))

    def test_competing_comparison_claims(self) -> None:
        self.prepare_comparison()
        claims = self.race(self.comparisons, "INSERT INTO comparison_work")
        self.assertEqual(sum(c is not None for c in claims), 1)

    def test_comparison_stale_and_duplicate_completion(self) -> None:
        self.prepare_comparison()
        first = self.coordinators[0].claim(self.comparisons)
        self.expire(self.comparisons)
        second = self.coordinators[1].claim(self.comparisons)
        with self.assertRaises(LostClaim):
            self.coordinators[0].execute_claim(self.comparisons, first, self.cpu)
        self.coordinators[1].execute_claim(self.comparisons, second, self.cpu)
        with self.assertRaises(LostClaim):
            self.coordinators[1].execute_claim(self.comparisons, second, self.cpu)
        with self.session_factory() as session:
            self.assertEqual(session.get(ComparisonRequest, 1).candidates_scored_count, 1)
            self.assertEqual(list(session.scalars(select(ScoredCandidate.candidate_artifact_id))), [2])

    def test_result_and_completion_rollback(self) -> None:
        self.prepare_comparison()
        claim = self.coordinators[0].claim(self.comparisons)
        def fail_insert(conn: Connection, cursor: object, statement: str, parameters: object, context: object, executemany: bool) -> None:
            if statement.startswith("INSERT INTO scored_candidate"):
                raise RuntimeError("injected failure")
        event.listen(self.engine, "before_cursor_execute", fail_insert)
        try:
            with self.assertRaises(RuntimeError):
                self.coordinators[0].execute_claim(self.comparisons, claim, self.cpu)
        finally:
            event.remove(self.engine, "before_cursor_execute", fail_insert)
        with self.session_factory() as session:
            self.assertEqual(self.comparisons.state(session, 1)["claim_token"], claim.token)
            self.assertEqual(session.get(ComparisonRequest, 1).candidates_scored_count, 0)
            self.assertEqual(list(session.scalars(select(ScoredCandidate))), [])
            self.assertEqual(list(session.scalars(select(TopComparison))), [])
        self.expire(self.comparisons)
        self.assertIsInstance(self.run_work(self.comparisons), Finished)

    def test_late_lower_id_and_top_k_eviction(self) -> None:
        self.prepare_comparison()
        with self.session_factory.begin() as session:
            session.add(Document(id=0, workspace_id=1, name="late", text="banana"))
            session.add(FeatureArtifact(id=0, workspace_id=1, document_id=0))
        self.assertIsInstance(self.run_work(self.comparisons), Unfinished)
        self.assertIsNone(self.coordinators[0].claim(self.comparisons))
        self.run_work(self.builds)
        self.assertIsInstance(self.run_work(self.comparisons), Finished)
        with self.session_factory() as session:
            self.assertEqual(set(session.scalars(select(ScoredCandidate.candidate_artifact_id))), {0, 2})
            self.assertEqual(list(session.scalars(select(TopComparison.candidate_artifact_id))), [2])
            self.assertEqual(session.get(ComparisonRequest, 1).candidates_scored_count, 2)
        with self.session_factory.begin() as session:
            session.add(ComparisonRequest(id=2, workspace_id=1, query_artifact_id=1))
        self.assertIsInstance(self.run_work(self.comparisons), Unfinished)
        self.assertIsInstance(self.run_work(self.comparisons), Finished)
        with self.session_factory() as session:
            self.assertEqual(len(list(session.scalars(select(ScoredCandidate).where(ScoredCandidate.request_id == 2)))), 2)

    def test_empty_comparison_finishes(self) -> None:
        self.prepare_comparison()
        with self.session_factory.begin() as session:
            session.add(Workspace(id=2, name="other"))
            session.get(FeatureArtifact, 2).workspace_id = 2
        self.assertIsInstance(self.run_work(self.comparisons), Finished)

    def test_failed_remaining_build_allows_finalization(self) -> None:
        self.prepare_comparison()
        with self.session_factory.begin() as session:
            session.add(Document(id=3, workspace_id=1, name="bad", text="bad"))
            session.add(FeatureArtifact(id=3, workspace_id=1, document_id=3))
        self.assertIsInstance(self.run_work(self.comparisons), Unfinished)
        claim = self.coordinators[0].claim(self.builds)
        self.coordinators[0]._fail(self.builds, claim, ValueError("bad input"))
        self.assertIsInstance(self.run_work(self.comparisons), Finished)

    def test_unready_query_does_not_block_later_request(self) -> None:
        with self.session_factory.begin() as session:
            session.add(ComparisonRequest(id=1, workspace_id=1, query_artifact_id=1))
            session.add(ComparisonRequest(id=2, workspace_id=1, query_artifact_id=2))
            session.add(Document(id=3, workspace_id=1, name="ready", text="apple"))
            session.add(FeatureArtifact(id=3, workspace_id=1, document_id=3, feature_json={"apple": 1}))
        claim = self.coordinators[0].claim(self.comparisons)
        self.assertEqual(claim.source_id, 2)

    def test_failed_build_reset(self) -> None:
        claim = self.coordinators[0].claim(self.builds)
        self.coordinators[0]._fail(self.builds, claim, ValueError("bad"))
        self.assertIsNone(self.coordinators[0].claim(self.builds))
        with self.session_factory.begin() as session:
            self.assertEqual(self.builds.state(session, 1)["status"], "failed")
            self.assertTrue(self.builds.reset_failed(session, 1))
        self.run_work(self.builds)
        with self.session_factory() as session:
            self.assertEqual(self.builds.state(session, 1)["status"], "finished")

    def test_noninteger_source_without_domain_result(self) -> None:
        worker = Worker(name="external_task", source=ExternalSource, handler=no_database_output)
        coordinator = self.coordinators[0]
        coordinator.register(worker)
        coordinator.create_worker_tables()
        key = str(uuid.uuid4())
        with self.session_factory.begin() as session:
            session.add(ExternalSource(key=key))
        self.assertIsInstance(self.run_work(worker), Finished)
        with self.session_factory() as session:
            self.assertEqual(worker.state(session, key)["status"], "finished")

    def test_invalid_handler_outcome_rolls_back(self) -> None:
        def handler(source: FeatureArtifact, context: Context[FeatureArtifact]) -> None:
            context.session.get(FeatureArtifact, source.id).feature_json = {"wrong": 1}
            return None
        worker = Worker(name="invalid_result", source=FeatureArtifact, handler=handler)
        self.coordinators[0].register(worker)
        self.coordinators[0].create_worker_tables()
        with self.assertRaises(TypeError):
            self.run_work(worker)
        with self.session_factory() as session:
            self.assertIsNone(session.get(FeatureArtifact, 1).feature_json)


if __name__ == "__main__":
    unittest.main()
