"""Comparison eligibility, paged scoring, and application-owned progress."""


from sqlalchemy import and_, delete, exists, or_, select
from sqlalchemy.orm import InstrumentedAttribute, Session
from sqlalchemy.sql import Select
from sqlalchemy.sql.selectable import Exists

from imagededup_system_dbwork.db.models import ComparisonRequest, FeatureArtifact, ScoredCandidate, TopComparison
from dbworker import Coordinator, ExecutionStatus, Finished, Outcome, Unfinished


def ready_candidates(request: ComparisonRequest) -> Select[tuple[FeatureArtifact]]:
    completed = exists().where(
        ScoredCandidate.request_id == request.id,
        ScoredCandidate.candidate_artifact_id == FeatureArtifact.id,
    )
    return select(FeatureArtifact).where(
        FeatureArtifact.workspace_id == request.workspace_id,
        FeatureArtifact.id != request.query_artifact_id,
        FeatureArtifact.hash_value.is_not(None),
        ~completed,
    ).order_by(FeatureArtifact.id)


def unfinished_builds(
    workspace_id: int | InstrumentedAttribute[int], coordinator: Coordinator,
) -> Exists:
    return exists().where(
        FeatureArtifact.workspace_id == workspace_id,
        FeatureArtifact.hash_value.is_(None),
        ~coordinator.has_execution_status(
            worker="artifact_build", source_id=FeatureArtifact.id,
            statuses=(ExecutionStatus.FINISHED, ExecutionStatus.FAILED),
        ),
    )


def compare_artifacts(
    request: ComparisonRequest, session: Session,
    *, page_size: int, coordinator: Coordinator,
) -> Outcome:
    request_id = request.id
    query_artifact = session.get(FeatureArtifact, request.query_artifact_id)
    if query_artifact is None or query_artifact.hash_value is None:
        raise ValueError("Comparison query has no perceptual hash")
    query = query_artifact.hash_value
    candidates: list[tuple[int, str]] = []
    for artifact in session.scalars(ready_candidates(request).limit(page_size)):
        if artifact.hash_value is None:
            raise ValueError("A selected candidate has no perceptual hash")
        candidates.append((artifact.id, artifact.hash_value))
    session.rollback()
    # Same 64-bit Hamming metric as imagededup, without loading its image
    # stack into comparison processes or creating nested process pools.
    query_number = int(query, 16)
    scores = [(artifact_id, (query_number ^ int(hash_value, 16)).bit_count())
              for artifact_id, hash_value in candidates]
    current = session.get(ComparisonRequest, request_id)
    if current is None:
        raise ValueError("Comparison request no longer exists")
    if scores:
        previous = session.scalars(select(TopComparison).where(TopComparison.request_id == request_id))
        combined = {row.candidate_artifact_id: row.distance for row in previous}
        combined.update((key, distance) for key, distance in scores if distance <= current.max_distance)
        best = sorted(combined.items(), key=lambda item: (item[1], item[0]))[:current.retained_max_k]
        session.execute(delete(TopComparison).where(TopComparison.request_id == request_id))
        session.add_all(TopComparison(request_id=request_id, candidate_artifact_id=key, distance=score) for key, score in best)
        session.add_all(ScoredCandidate(request_id=request_id, candidate_artifact_id=key) for key, _ in scores)
        current.candidates_scored_count += len(scores)
        session.flush()
    # One statement observes both sides of a build's transition: a build
    # finishing between separate queries must not make the request look done.
    remaining = session.scalar(select(or_(
        ready_candidates(current).order_by(None).exists(),
        unfinished_builds(current.workspace_id, coordinator),
    )))
    return Unfinished() if remaining else Finished()


def eligible_comparisons(coordinator: Coordinator) -> Select[tuple[ComparisonRequest]]:
    query = FeatureArtifact.__table__.alias("query_artifact")
    candidate = FeatureArtifact.__table__.alias("candidate_artifact")
    completed = exists().where(
        ScoredCandidate.request_id == ComparisonRequest.id,
        ScoredCandidate.candidate_artifact_id == candidate.c.id,
    ).correlate(ComparisonRequest, candidate)
    has_candidates = exists().where(
        candidate.c.workspace_id == ComparisonRequest.workspace_id,
        candidate.c.id != ComparisonRequest.query_artifact_id,
        candidate.c.hash_value.is_not(None),
        ~completed,
    )
    return (
        select(ComparisonRequest)
        .join(query, query.c.id == ComparisonRequest.query_artifact_id)
        .where(or_(
            and_(query.c.hash_value.is_not(None), or_(
                has_candidates, ~unfinished_builds(ComparisonRequest.workspace_id, coordinator),
            )),
            coordinator.has_execution_status(
                worker="artifact_build", source_id=query.c.id,
                statuses=(ExecutionStatus.FAILED,),
            ),
        ))
        .order_by(ComparisonRequest.id)
    )
