import tempfile
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from datetime import timedelta
from threading import Barrier
from unittest.mock import patch

from sqlalchemy import Connection, String, create_engine, event, select, update
from sqlalchemy.orm import Mapped, Session, mapped_column, sessionmaker

from durable_worker_example.db.engine import Base
from durable_worker_example.db.models import Workspace, Document, FeatureArtifact, ComparisonRequest, ScoredCandidate, TopComparison
from durable_worker_example.domain import workflows
from dbworker import Coordinator, Finished, Unfinished, _Worker, Claim, LostClaim, Outcome, now, _execute_claim


class ExternalSource(Base):
    __tablename__ = "test_external_source"
    key: Mapped[str] = mapped_column(String(36), primary_key=True)


def no_database_output(source: ExternalSource, session: Session) -> Finished:
    return Finished()


class ClaimsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.engine = create_engine(f"sqlite:///{self.directory.name}/test.db", connect_args={"check_same_thread": False})
        self.session_factory = sessionmaker(self.engine, expire_on_commit=False)
        Base.metadata.create_all(self.engine)
        self.coordinators = [Coordinator(self.session_factory, database_url=self.engine.url) for _ in range(2)]
        for coordinator in self.coordinators:
            coordinator.transactional_worker(
                name="artifact_build", source=FeatureArtifact,
                eligible=lambda: select(FeatureArtifact).where(FeatureArtifact.feature_json.is_(None)).order_by(FeatureArtifact.id),
            )(workflows.build_artifact)
            coordinator.transactional_worker(
                name="comparison", source=ComparisonRequest, eligible=workflows.eligible_comparisons,
            )(partial(workflows.compare_artifacts, page_size=1))
            coordinator.create_worker_tables()
        self.builds = self.coordinators[0].workers["artifact_build"]
        self.comparisons = self.coordinators[0].workers["comparison"]
        with self.session_factory.begin() as session:
            session.add(Workspace(id=1, name="test"))
            for i in (1, 2):
                session.add(Document(id=i, workspace_id=1, name=str(i), text="apple"))
                session.add(FeatureArtifact(id=i, workspace_id=1, document_id=i, feature_json=None if i == 1 else {"apple": 1}))

    def tearDown(self) -> None:
        for coordinator in self.coordinators:
            coordinator.stop()
        self.engine.dispose()
        self.directory.cleanup()

    def run_work(self, worker: _Worker, coordinator: int = 0) -> Outcome:
        runtime = self.coordinators[coordinator]
        claim = runtime.claim(worker)
        self.assertIsNotNone(claim)
        return _execute_claim(worker, claim, runtime.session_factory)

    def expire(self, worker: _Worker) -> None:
        with self.session_factory.begin() as session:
            session.execute(update(worker.table).values(lease_expires_at=now() - timedelta(seconds=1)))

    def prepare_comparison(self) -> None:
        self.run_work(self.builds)
        with self.session_factory.begin() as session:
            session.add(ComparisonRequest(id=1, workspace_id=1, query_artifact_id=1, retained_max_k=1))

    def race(self, worker: _Worker, statement_prefix: str) -> list[Claim | None]:
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
            _execute_claim(self.builds, first, self.coordinators[0].session_factory)
        self.coordinators[0].renew(self.builds, [first])
        with self.session_factory() as session:
            self.assertIsNone(session.get(FeatureArtifact, 1).feature_json)
            self.assertEqual(self.builds.state(session, 1)["claim_token"], second.token)
        _execute_claim(self.builds, second, self.coordinators[1].session_factory)
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
            _execute_claim(self.comparisons, first, self.coordinators[0].session_factory)
        _execute_claim(self.comparisons, second, self.coordinators[1].session_factory)
        with self.assertRaises(LostClaim):
            _execute_claim(self.comparisons, second, self.coordinators[1].session_factory)
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
                _execute_claim(self.comparisons, claim, self.coordinators[0].session_factory)
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
        coordinator = self.coordinators[0]
        coordinator.transactional_worker(name="external_task", source=ExternalSource)(no_database_output)
        worker = coordinator.workers["external_task"]
        coordinator.create_worker_tables()
        key = str(uuid.uuid4())
        with self.session_factory.begin() as session:
            session.add(ExternalSource(key=key))
        self.assertIsInstance(self.run_work(worker), Finished)
        with self.session_factory() as session:
            self.assertEqual(worker.state(session, key)["status"], "finished")

    def test_invalid_handler_outcome_rolls_back(self) -> None:
        def handler(source: FeatureArtifact, session: Session) -> None:
            source.feature_json = {"wrong": 1}
            return None
        self.coordinators[0].transactional_worker(name="invalid_result", source=FeatureArtifact)(handler)
        worker = self.coordinators[0].workers["invalid_result"]
        self.coordinators[0].create_worker_tables()
        with self.assertRaises(TypeError):
            self.run_work(worker)
        with self.session_factory() as session:
            self.assertIsNone(session.get(FeatureArtifact, 1).feature_json)

    def test_concurrent_invocations_have_independent_save_transactions(self) -> None:
        coordinator = self.coordinators[0]
        barrier = Barrier(2)
        session_ids: dict[int, int] = {}

        def handler(source: FeatureArtifact, session: Session) -> Finished:
            source_id = source.id
            value = abs(-source_id)
            session.rollback()
            barrier.wait(timeout=10)
            session_ids[source_id] = id(session)
            artifact = session.get(FeatureArtifact, source_id)
            assert artifact is not None
            artifact.feature_json = {"new": value}
            if source_id == 2:
                raise ValueError("rollback this invocation only")
            return Finished()

        coordinator.transactional_worker(name="isolated_saves", source=FeatureArtifact,
                        eligible=lambda: select(FeatureArtifact).order_by(FeatureArtifact.id))(handler)
        worker = coordinator.workers["isolated_saves"]
        coordinator.create_worker_tables()
        first, second = coordinator.claim(worker), coordinator.claim(worker)
        assert first is not None and second is not None
        with ThreadPoolExecutor(2) as handlers:
            successful = handlers.submit(_execute_claim, worker, first, coordinator.session_factory)
            failed = handlers.submit(_execute_claim, worker, second, coordinator.session_factory)
            self.assertIsInstance(successful.result(timeout=10), Finished)
            with self.assertRaisesRegex(ValueError, "rollback this invocation"):
                failed.result(timeout=10)
        self.assertNotEqual(session_ids[1], session_ids[2])
        with self.session_factory() as session:
            self.assertEqual(session.get(FeatureArtifact, 1).feature_json, {"new": 1})
            self.assertEqual(session.get(FeatureArtifact, 2).feature_json, {"apple": 1})
            self.assertEqual(worker.state(session, 1)["status"], "finished")
            self.assertEqual(worker.state(session, 2)["claim_token"], second.token)


    def test_handler_cannot_commit_results_before_completion(self) -> None:
        def handler(source: FeatureArtifact, session: Session) -> Finished:
            source.feature_json = {"premature": 1}
            session.flush()
            session.commit()
            return Finished()

        self.coordinators[0].transactional_worker(name="premature_commit", source=FeatureArtifact)(handler)
        worker = self.coordinators[0].workers["premature_commit"]
        self.coordinators[0].create_worker_tables()
        with self.assertRaisesRegex(RuntimeError, "worker commits"):
            self.run_work(worker)
        with self.session_factory() as session:
            self.assertIsNone(session.get(FeatureArtifact, 1).feature_json)
            self.assertEqual(worker.state(session, 1)["status"], "working")

    def test_stale_explicit_sql_is_rolled_back(self) -> None:
        def handler(source: FeatureArtifact, session: Session) -> Finished:
            key = source.id
            session.rollback()
            session.execute(update(FeatureArtifact).where(FeatureArtifact.id == key).values(feature_json={"stale": 1}))
            return Finished()

        self.coordinators[0].transactional_worker(name="stale_sql", source=FeatureArtifact)(handler)
        worker = self.coordinators[0].workers["stale_sql"]
        self.coordinators[0].create_worker_tables()
        first = self.coordinators[0].claim(worker)
        self.expire(worker)
        replacement = self.coordinators[1].claim(worker)
        with self.assertRaises(LostClaim):
            _execute_claim(worker, first, self.session_factory)
        with self.session_factory() as session:
            self.assertIsNone(session.get(FeatureArtifact, 1).feature_json)
            self.assertEqual(worker.state(session, 1)["claim_token"], replacement.token)

    def test_example_releases_connection_during_cpu_work(self) -> None:
        def build(text: str) -> dict[str, int]:
            self.assertEqual(self.engine.pool.checkedout(), 0)
            return {"apple": 1}

        def score(query: dict[str, int], candidates: list[tuple[int, dict[str, int]]]) -> list[tuple[int, float]]:
            self.assertEqual(self.engine.pool.checkedout(), 0)
            return [(key, 1.0) for key, _ in candidates]

        with patch("durable_worker_example.domain.workflows.build_features", side_effect=build):
            self.prepare_comparison()
        with patch("durable_worker_example.domain.workflows.score_feature_pairs", side_effect=score):
            self.assertIsInstance(self.run_work(self.comparisons), Finished)




if __name__ == "__main__":
    unittest.main()
