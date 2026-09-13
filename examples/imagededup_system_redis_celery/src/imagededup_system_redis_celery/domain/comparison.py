"""Paged exhaustive comparison, top-K results and an application-owned ledger."""

from sqlalchemy import delete, exists, or_, select, update
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import Select
from sqlalchemy.sql.selectable import Exists

from imagededup_system_redis_celery.db.models import (
    ComparisonRequest, ExecutionStatus, FeatureArtifact, ScoredCandidate, TopComparison,
)
from imagededup_system_redis_celery.outbox import enqueue


def ready_candidates(request: ComparisonRequest) -> Select[tuple[FeatureArtifact]]:
    completed = exists().where(
        ScoredCandidate.request_id == request.id,
        ScoredCandidate.candidate_artifact_id == FeatureArtifact.id,
    )
    return select(FeatureArtifact).where(
        FeatureArtifact.workspace_id == request.workspace_id,
        FeatureArtifact.id != request.query_artifact_id,
        FeatureArtifact.hash_value.is_not(None), ~completed,
    ).order_by(FeatureArtifact.id)


def unfinished_builds(workspace_id: int) -> Exists:
    return exists().where(
        FeatureArtifact.workspace_id == workspace_id, FeatureArtifact.hash_value.is_(None),
        or_(FeatureArtifact.execution_status.is_(None), FeatureArtifact.execution_status.not_in(
            (ExecutionStatus.FINISHED, ExecutionStatus.FAILED),
        )),
    )


def compare_page(request_id: int, revision: int, session_factory: sessionmaker[Session],
                 *, page_size: int, dependency_wait: float) -> str | None:
    with session_factory.begin() as session:
        request = session.get(ComparisonRequest, request_id)
        if request is None or request.revision != revision or request.execution_status in (
            ExecutionStatus.FINISHED, ExecutionStatus.FAILED,
        ):
            return None
        query_artifact = session.get(FeatureArtifact, request.query_artifact_id)
        if query_artifact is None or query_artifact.execution_status is ExecutionStatus.FAILED:
            raise ValueError("Comparison query image build failed or no longer exists")
        query_hash = query_artifact.hash_value
        candidates = [(artifact.id, artifact.hash_value)
                      for artifact in session.scalars(ready_candidates(request).limit(page_size))] if query_hash else []
        session.execute(update(ComparisonRequest).where(
            ComparisonRequest.id == request_id, ComparisonRequest.revision == revision,
        ).values(execution_status=ExecutionStatus.WORKING))
    # Exact 64-bit Hamming metric used by imagededup, without loading its image
    # and ML dependencies in comparison processes or creating nested pools.
    query_number = int(query_hash, 16) if query_hash else 0
    distances = [(key, (query_number ^ int(value, 16)).bit_count())
                 for key, value in candidates if value is not None]
    with session_factory.begin() as session:
        changed = session.execute(update(ComparisonRequest).where(
            ComparisonRequest.id == request_id, ComparisonRequest.revision == revision,
        ).values(revision=revision + 1))
        if changed.rowcount != 1:  # type: ignore[attr-defined]
            return None  # Duplicate/stale delivery: no writes and no continuation.
        current = session.get(ComparisonRequest, request_id)
        assert current is not None
        if distances:
            previous = session.scalars(select(TopComparison).where(TopComparison.request_id == request_id))
            combined = {row.candidate_artifact_id: row.distance for row in previous}
            combined.update((key, distance) for key, distance in distances if distance <= current.max_distance)
            best = sorted(combined.items(), key=lambda item: (item[1], item[0]))[:current.retained_max_k]
            session.execute(delete(TopComparison).where(TopComparison.request_id == request_id))
            session.add_all(TopComparison(request_id=request_id, candidate_artifact_id=key, distance=distance)
                            for key, distance in best)
            session.add_all(ScoredCandidate(request_id=request_id, candidate_artifact_id=key)
                            for key, _ in distances)
            current.candidates_scored_count += len(distances)
            session.flush()
        # Observe ready work and unfinished builds together, including a build
        # that finished after the read phase. Waiting tasks never occupy a process.
        query = session.get(FeatureArtifact, current.query_artifact_id)
        if query is None or query.execution_status is ExecutionStatus.FAILED:
            raise ValueError("Comparison query image build failed or no longer exists")
        has_ready, has_builds = session.execute(select(
            ready_candidates(current).order_by(None).exists(), unfinished_builds(current.workspace_id),
        )).one()
        current.error = None
        if query.hash_value is not None and not has_ready and not has_builds:
            current.execution_status = ExecutionStatus.FINISHED
            return None
        current.execution_status = ExecutionStatus.UNFINISHED
        delay = 0 if query.hash_value is not None and has_ready else dependency_wait
        return enqueue(session, "images.compare", request_id, revision + 1, delay=delay)
