import logging
import threading
import time
import uuid
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import datetime, timedelta

from sqlalchemy import and_, delete, or_, select, update
from sqlalchemy.orm import sessionmaker

from durable_worker_example.config import Settings
from durable_worker_example.db.models import ComparisonRequest, Document, FeatureArtifact, ScoredCandidate, TopComparison
from durable_worker_example.worker.execution import build_features, score_feature_pairs

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.utcnow()


class Coordinator:
    """Owns database sessions; child processes only perform pure CPU functions."""

    def __init__(self, sessions: sessionmaker, settings: Settings):
        self._sessions = sessions
        self._settings = settings
        with sessions() as session:
            dialect = session.connection().dialect
            version = dialect.server_version_info or ()
            self._supports_skip_locked = (
                dialect.name == "postgresql" and version >= (9, 5)
                or dialect.name == "mysql" and not getattr(dialect, "is_mariadb", False) and version >= (8, 0, 1)
                or dialect.name in ("mysql", "mariadb") and getattr(dialect, "is_mariadb", False) and version >= (10, 6)
            )
        self._stop = threading.Event()
        self._build_pool = ProcessPoolExecutor(max_workers=settings.build_workers)
        self._comparison_pool = ProcessPoolExecutor(max_workers=settings.comparison_workers)
        self._build_thread = threading.Thread(target=self._run_builds, name="feature-coordinator", daemon=True)
        self._comparison_thread = threading.Thread(target=self._run_comparisons, name="comparison-coordinator", daemon=True)

    def start(self) -> None:
        self._build_thread.start()
        self._comparison_thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._build_thread.join(timeout=5)
        self._comparison_thread.join(timeout=5)
        self._build_pool.shutdown(wait=False, cancel_futures=True)
        self._comparison_pool.shutdown(wait=False, cancel_futures=True)

    def _lease_until(self) -> datetime:
        return _now() + timedelta(seconds=self._settings.claim_lease_seconds)

    def _claim_artifact(self) -> tuple[int, str, str] | None:
        claim = self._claim_artifact_with_db_lock if self._supports_skip_locked else self._claim_artifact_with_conditional_update
        return claim()

    def _claim_artifact_with_db_lock(self) -> tuple[int, str, str] | None:
        """Hold the selected row lock until the ownership fields commit."""
        token = str(uuid.uuid4())
        now = _now()
        with self._sessions.begin() as session:
            artifact = session.scalar(
                select(FeatureArtifact)
                .join(Document, Document.id == FeatureArtifact.document_id)
                .where(or_(FeatureArtifact.status == "pending", and_(FeatureArtifact.status == "building", FeatureArtifact.lease_expires_at < now)))
                .order_by(FeatureArtifact.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if artifact is None:
                return None
            artifact.status = "building"
            artifact.claim_token = token
            artifact.lease_expires_at = self._lease_until()
            return artifact.id, token, session.get(Document, artifact.document_id).text

    def _claim_artifact_with_conditional_update(self) -> tuple[int, str, str] | None:
        """The UPDATE decides ownership; the preceding read may be stale."""
        token = str(uuid.uuid4())
        now = _now()
        with self._sessions.begin() as session:
            artifact = session.scalar(
                select(FeatureArtifact)
                .join(Document, Document.id == FeatureArtifact.document_id)
                .where(or_(FeatureArtifact.status == "pending", and_(FeatureArtifact.status == "building", FeatureArtifact.lease_expires_at < now)))
                .order_by(FeatureArtifact.id)
                .limit(1)
            )
            if artifact is None:
                return None
            result = session.execute(
                update(FeatureArtifact)
                .where(
                    FeatureArtifact.id == artifact.id,
                    or_(FeatureArtifact.status == "pending", and_(FeatureArtifact.status == "building", FeatureArtifact.lease_expires_at < now)),
                )
                .values(status="building", claim_token=token, lease_expires_at=self._lease_until())
                .execution_options(synchronize_session=False)
            )
            if result.rowcount != 1:
                return None
            return artifact.id, token, session.get(Document, artifact.document_id).text

    def _finish_artifact(self, artifact_id: int, token: str, features: dict[str, int] | None, error: str | None = None) -> None:
        with self._sessions.begin() as session:
            session.execute(
                update(FeatureArtifact)
                .where(FeatureArtifact.id == artifact_id, FeatureArtifact.status == "building", FeatureArtifact.claim_token == token)
                .values(status="failed" if error else "ready", feature_json=features, error=error, claim_token=None, lease_expires_at=None)
                .execution_options(synchronize_session=False)
            )

    def _renew_artifact_leases(self, active_builds: dict[Future, tuple[int, str]]) -> None:
        if not active_builds:
            return
        with self._sessions.begin() as session:
            for artifact_id, token in active_builds.values():
                session.execute(update(FeatureArtifact).where(FeatureArtifact.id == artifact_id, FeatureArtifact.status == "building", FeatureArtifact.claim_token == token).values(lease_expires_at=self._lease_until()))

    def _run_builds(self) -> None:
        active_builds: dict[Future, tuple[int, str]] = {}
        next_renewal = time.monotonic()
        while not self._stop.is_set():
            while len(active_builds) < self._settings.build_workers:
                claim = self._claim_artifact()
                if claim is None:
                    break
                artifact_id, token, text = claim
                active_builds[self._build_pool.submit(build_features, text)] = (artifact_id, token)
            for future in list(active_builds):
                if not future.done():
                    continue
                artifact_id, token = active_builds.pop(future)
                try:
                    self._finish_artifact(artifact_id, token, future.result())
                except Exception as exc:  # keep one bad input from killing the coordinator
                    logger.exception("Feature build failed for artifact %s", artifact_id)
                    self._finish_artifact(artifact_id, token, None, str(exc))
            if time.monotonic() >= next_renewal:
                self._renew_artifact_leases(active_builds)
                next_renewal = time.monotonic() + self._settings.claim_lease_seconds / 3
            self._stop.wait(self._settings.poll_seconds)

    def _unscored_candidates(self, request):
        scored = select(ScoredCandidate.request_id).where(
            ScoredCandidate.request_id == request.id,
            ScoredCandidate.candidate_artifact_id == FeatureArtifact.id,
        ).exists()
        return select(FeatureArtifact).where(
            FeatureArtifact.workspace_id == request.workspace_id,
            FeatureArtifact.id != request.query_artifact_id,
            FeatureArtifact.status == "ready",
            ~scored,
        ).order_by(FeatureArtifact.id)

    def _claim_comparison_page(self) -> tuple[int, str, dict[str, int], list[tuple[int, dict[str, int]]]] | None:
        claim = self._claim_comparison_page_with_db_lock if self._supports_skip_locked else self._claim_comparison_page_with_conditional_update
        return claim()

    def _claim_comparison_page_with_db_lock(self) -> tuple[int, str, dict[str, int], list[tuple[int, dict[str, int]]]] | None:
        """Lock the request while selecting its next page and recording ownership."""
        token = str(uuid.uuid4())
        now = _now()
        with self._sessions.begin() as session:
            request = session.scalar(
                select(ComparisonRequest)
                .where(or_(ComparisonRequest.status == "pending", ComparisonRequest.status == "partial", and_(ComparisonRequest.status == "scanning", ComparisonRequest.lease_expires_at < now)))
                .order_by(ComparisonRequest.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if request is None:
                return None
            query = session.get(FeatureArtifact, request.query_artifact_id)
            if query is None or query.status != "ready" or query.feature_json is None:
                return None
            candidates = list(session.scalars(
                self._unscored_candidates(request)
                .limit(self._settings.comparison_page_size)
            ))
            if not candidates:
                return None
            request.status = "scanning"
            request.claim_token = token
            request.lease_expires_at = self._lease_until()
            return request.id, token, query.feature_json, [(item.id, item.feature_json) for item in candidates]

    def _claim_comparison_page_with_conditional_update(self) -> tuple[int, str, dict[str, int], list[tuple[int, dict[str, int]]]] | None:
        """Claim only if no page completed since the candidates were selected."""
        token = str(uuid.uuid4())
        now = _now()
        with self._sessions.begin() as session:
            request = session.scalar(
                select(ComparisonRequest)
                .where(or_(ComparisonRequest.status == "pending", ComparisonRequest.status == "partial", and_(ComparisonRequest.status == "scanning", ComparisonRequest.lease_expires_at < now)))
                .order_by(ComparisonRequest.id)
                .limit(1)
            )
            if request is None:
                return None
            query = session.get(FeatureArtifact, request.query_artifact_id)
            if query is None or query.status != "ready" or query.feature_json is None:
                return None
            candidates = list(session.scalars(
                self._unscored_candidates(request)
                .limit(self._settings.comparison_page_size)
            ))
            if not candidates:
                return None
            result = session.execute(
                update(ComparisonRequest)
                .where(
                    ComparisonRequest.id == request.id,
                    or_(ComparisonRequest.status == "pending", ComparisonRequest.status == "partial", and_(ComparisonRequest.status == "scanning", ComparisonRequest.lease_expires_at < now)),
                    # Prevent claiming a stale page after another owner completed it.
                    ComparisonRequest.candidates_scored_count == request.candidates_scored_count,
                )
                .values(status="scanning", claim_token=token, lease_expires_at=self._lease_until())
                .execution_options(synchronize_session=False)
            )
            if result.rowcount != 1:
                return None
            return request.id, token, query.feature_json, [(item.id, item.feature_json) for item in candidates]

    def _finish_comparison_page(self, request_id: int, token: str, scores: list[tuple[int, float]]) -> None:
        with self._sessions.begin() as session:
            # Acquire write protection before touching any aggregate rows. The
            # ownership transition and all result changes commit or roll back together.
            result = session.execute(
                update(ComparisonRequest)
                .where(ComparisonRequest.id == request_id, ComparisonRequest.status == "scanning", ComparisonRequest.claim_token == token)
                .values(status="partial", claim_token=None, lease_expires_at=None)
                .execution_options(synchronize_session=False)
            )
            if result.rowcount != 1:
                return
            request = session.get(ComparisonRequest, request_id)
            previous = list(session.scalars(select(TopComparison).where(TopComparison.request_id == request_id)))
            combined = [(row.candidate_artifact_id, row.score) for row in previous] + scores
            best_by_id = {artifact_id: score for artifact_id, score in combined}
            best = sorted(best_by_id.items(), key=lambda pair: pair[1], reverse=True)[:request.retained_max_k]
            session.execute(delete(TopComparison).where(TopComparison.request_id == request_id))
            session.add_all(TopComparison(request_id=request_id, candidate_artifact_id=artifact_id, score=score) for artifact_id, score in best)
            session.add_all(
                ScoredCandidate(request_id=request_id, candidate_artifact_id=artifact_id)
                for artifact_id, _ in scores
            )
            # Make completion records visible to the remaining-work query.
            session.flush()
            request.candidates_scored_count += len(scores)
            has_more_ready = session.scalar(self._unscored_candidates(request).limit(1)) is not None
            has_pending_build = session.scalar(select(FeatureArtifact.id).where(FeatureArtifact.workspace_id == request.workspace_id, FeatureArtifact.status.in_(["pending", "building"])).limit(1)) is not None
            request.status = "partial" if has_more_ready or has_pending_build else "ready"
            request.claim_token = None
            request.lease_expires_at = None

    def _renew_comparison_leases(self, active_pages: dict[Future, tuple[int, str]]) -> None:
        if not active_pages:
            return
        with self._sessions.begin() as session:
            for request_id, token in active_pages.values():
                session.execute(update(ComparisonRequest).where(ComparisonRequest.id == request_id, ComparisonRequest.status == "scanning", ComparisonRequest.claim_token == token).values(lease_expires_at=self._lease_until()))

    def _run_comparisons(self) -> None:
        active_pages: dict[Future, tuple[int, str]] = {}
        next_renewal = time.monotonic()
        while not self._stop.is_set():
            while len(active_pages) < self._settings.comparison_workers:
                claim = self._claim_comparison_page()
                if claim is None:
                    break
                request_id, token, query, candidates = claim
                active_pages[self._comparison_pool.submit(score_feature_pairs, query, candidates)] = (request_id, token)
            for future in list(active_pages):
                if not future.done():
                    continue
                request_id, token = active_pages.pop(future)
                try:
                    self._finish_comparison_page(request_id, token, future.result())
                except Exception:
                    logger.exception("Comparison page failed for request %s", request_id)
            if time.monotonic() >= next_renewal:
                self._renew_comparison_leases(active_pages)
                next_renewal = time.monotonic() + self._settings.claim_lease_seconds / 3
            self._stop.wait(self._settings.poll_seconds)
