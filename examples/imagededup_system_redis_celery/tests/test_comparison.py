"""Comparison correctness with local SQL only; no image stack or broker needed."""

import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, event, select
from sqlalchemy.orm import Session, sessionmaker

from imagededup_system_redis_celery.db.engine import Base
from imagededup_system_redis_celery.db.models import (
    ComparisonRequest, ExecutionStatus, FeatureArtifact, ImageAsset,
    OutboxMessage, ScoredCandidate, TopComparison, Workspace,
)
from imagededup_system_redis_celery.domain.comparison import compare_page
from imagededup_system_redis_celery.outbox import enqueue


class ComparisonTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.engine = create_engine(f"sqlite:///{Path(self.directory.name) / 'test.db'}")
        Base.metadata.create_all(self.engine)
        self.sessions = sessionmaker(self.engine, expire_on_commit=False)
        with self.sessions.begin() as session:
            workspace = Workspace(name="fixtures")
            session.add(workspace)
            session.flush()
            self.workspace_id = workspace.id

    def tearDown(self) -> None:
        self.engine.dispose()
        self.directory.cleanup()

    def artifact(self, value: int | None, *, failed: bool = False) -> int:
        with self.sessions.begin() as session:
            image = ImageAsset(workspace_id=self.workspace_id, name="fixture", file_path="/unused")
            session.add(image)
            session.flush()
            artifact = FeatureArtifact(
                workspace_id=self.workspace_id, image_id=image.id,
                hash_value=f"{value:016x}" if value is not None else None,
                execution_status=(ExecutionStatus.FAILED if failed else
                                  ExecutionStatus.FINISHED if value is not None else None),
            )
            session.add(artifact)
            session.flush()
            return artifact.id

    def request(self, query_id: int, *, k: int = 2, threshold: int = 2) -> int:
        with self.sessions.begin() as session:
            request = ComparisonRequest(workspace_id=self.workspace_id,
                                        query_artifact_id=query_id,
                                        retained_max_k=k, max_distance=threshold)
            session.add(request)
            session.flush()
            return request.id

    def run_page(self, request_id: int, revision: int, *, size: int = 2) -> str | None:
        return compare_page(request_id, revision, self.sessions,
                            page_size=size, dependency_wait=2.0)

    def state(self, request_id: int) -> tuple[int, int, ExecutionStatus | None]:
        with self.sessions() as session:
            request = session.get(ComparisonRequest, request_id)
            assert request is not None
            return request.revision, request.candidates_scored_count, request.execution_status

    def results(self, request_id: int) -> list[tuple[int, int]]:
        with self.sessions() as session:
            return [(row.candidate_artifact_id, row.distance) for row in session.scalars(
                select(TopComparison).where(TopComparison.request_id == request_id)
                .order_by(TopComparison.distance, TopComparison.candidate_artifact_id)
            )]

    def ledger(self, request_id: int) -> list[int]:
        with self.sessions() as session:
            return list(session.scalars(select(ScoredCandidate.candidate_artifact_id)
                                        .where(ScoredCandidate.request_id == request_id)
                                        .order_by(ScoredCandidate.candidate_artifact_id)))

    def test_paged_top_k_and_threshold_keep_nonmatches_in_ledger(self) -> None:
        query = self.artifact(0)
        first = self.artifact(3)  # Distance two, retained on the first page.
        nonmatch = self.artifact((1 << 64) - 1)  # Distance 64.
        nearest = self.artifact(0)
        tied_earlier = self.artifact(1)
        tied_later = self.artifact(2)
        request = self.request(query)
        self.assertIsNotNone(self.run_page(request, 0))
        self.assertEqual(self.results(request), [(first, 2)])
        self.assertEqual(self.ledger(request), [first, nonmatch])
        self.assertEqual(self.state(request), (1, 2, ExecutionStatus.UNFINISHED))
        self.assertIsNotNone(self.run_page(request, 1))
        self.assertIsNone(self.run_page(request, 2))
        self.assertEqual(self.results(request), [(nearest, 0), (tied_earlier, 1)])
        self.assertEqual(self.ledger(request), [first, nonmatch, nearest, tied_earlier, tied_later])
        self.assertEqual(self.state(request), (3, 5, ExecutionStatus.FINISHED))

    def test_late_lower_id_build_is_scored_after_higher_id(self) -> None:
        query = self.artifact(0)
        late = self.artifact(None)
        ready = self.artifact(3)
        request = self.request(query)
        self.run_page(request, 0)
        self.assertEqual(self.ledger(request), [ready])
        self.assertEqual(self.state(request), (1, 1, ExecutionStatus.UNFINISHED))
        with self.sessions.begin() as session:
            artifact = session.get(FeatureArtifact, late)
            assert artifact is not None
            artifact.hash_value = "0000000000000000"
            artifact.execution_status = ExecutionStatus.FINISHED
        self.run_page(request, 1)
        self.assertEqual(self.results(request), [(late, 0), (ready, 2)])
        self.assertEqual(self.ledger(request), [late, ready])
        self.assertEqual(self.state(request), (2, 2, ExecutionStatus.FINISHED))

    def test_unbuilt_query_waits_without_scoring_ready_candidates(self) -> None:
        query = self.artifact(None)
        self.artifact(0)
        request = self.request(query)
        before = datetime.utcnow()
        message_id = self.run_page(request, 0)
        self.assertEqual(self.state(request), (1, 0, ExecutionStatus.UNFINISHED))
        self.assertEqual(self.ledger(request), [])
        with self.sessions() as session:
            message = session.get(OutboxMessage, message_id)
            assert message is not None
            self.assertEqual((message.task_name, message.source_id, message.source_revision),
                             ("images.compare", request, 1))
            self.assertGreaterEqual(message.available_at, before + timedelta(seconds=2))

    def test_failed_candidates_are_skipped_and_do_not_prevent_finishing(self) -> None:
        query = self.artifact(0)
        self.artifact(None, failed=True)
        request = self.request(query)
        self.assertIsNone(self.run_page(request, 0))
        self.assertEqual(self.state(request), (1, 0, ExecutionStatus.FINISHED))

    def test_failed_query_raises_without_recording_progress(self) -> None:
        request = self.request(self.artifact(None, failed=True))
        with self.assertRaisesRegex(ValueError, "query image build failed"):
            self.run_page(request, 0)
        self.assertEqual(self.state(request), (0, 0, None))
        with self.sessions() as session:
            self.assertEqual(list(session.scalars(select(OutboxMessage))), [])

    def test_stale_delivery_cannot_double_write_or_enqueue(self) -> None:
        query = self.artifact(0)
        first = self.artifact(1)
        self.artifact(2)
        request = self.request(query)
        self.run_page(request, 0, size=1)
        self.assertIsNone(self.run_page(request, 0, size=1))
        self.assertEqual(self.state(request), (1, 1, ExecutionStatus.UNFINISHED))
        self.assertEqual(self.ledger(request), [first])
        with self.sessions() as session:
            self.assertEqual(len(list(session.scalars(select(OutboxMessage)))), 1)
        self.run_page(request, 1, size=1)
        self.artifact(0)  # Finished requests do not become live subscriptions.
        self.assertIsNone(self.run_page(request, 2))
        self.assertEqual(self.state(request), (2, 2, ExecutionStatus.FINISHED))

    def test_outbox_failure_rolls_back_page_progress_and_can_replay(self) -> None:
        query = self.artifact(0)
        first = self.artifact(1)
        self.artifact(2)
        request = self.request(query)

        def enqueue_then_fail(session: Session, task_name: str, source_id: int,
                              revision: int = 0, *, delay: float = 0) -> str:
            enqueue(session, task_name, source_id, revision, delay=delay)
            raise RuntimeError("simulated transaction failure")

        with patch("imagededup_system_redis_celery.domain.comparison.enqueue", enqueue_then_fail):
            with self.assertRaisesRegex(RuntimeError, "simulated transaction failure"):
                self.run_page(request, 0, size=1)
        self.assertEqual(self.state(request)[:2], (0, 0))
        self.assertEqual(self.results(request), [])
        self.assertEqual(self.ledger(request), [])
        with self.sessions() as session:
            self.assertEqual(list(session.scalars(select(OutboxMessage))), [])
        self.assertIsNotNone(self.run_page(request, 0, size=1))
        self.assertEqual(self.ledger(request), [first])
        self.assertEqual(self.state(request), (1, 1, ExecutionStatus.UNFINISHED))

    def test_overlapping_delivery_losing_revision_race_cannot_commit(self) -> None:
        query = self.artifact(0)
        first = self.artifact(1)
        self.artifact(2)
        request = self.request(query)
        delivered = False

        def deliver_duplicate_after_read_commit(session: Session) -> None:
            nonlocal delivered
            if not delivered:
                delivered = True
                self.assertIsNotNone(self.run_page(request, 0, size=1))

        # Interleave another delivery after the original releases its read
        # transaction, so both begin with revision zero but only one can commit.
        event.listen(self.sessions, "after_commit", deliver_duplicate_after_read_commit)
        try:
            self.assertIsNone(self.run_page(request, 0, size=1))
        finally:
            event.remove(self.sessions, "after_commit", deliver_duplicate_after_read_commit)
        self.assertTrue(delivered)
        self.assertEqual(self.state(request), (1, 1, ExecutionStatus.UNFINISHED))
        self.assertEqual(self.ledger(request), [first])
        self.assertEqual(self.results(request), [(first, 1)])
        with self.sessions() as session:
            self.assertEqual(len(list(session.scalars(select(OutboxMessage)))), 1)


if __name__ == "__main__":
    unittest.main()
