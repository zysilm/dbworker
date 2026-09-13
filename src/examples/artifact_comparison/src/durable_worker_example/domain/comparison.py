"""Comparison eligibility, paged scoring, and application-owned progress."""

import math

from sqlalchemy import delete, exists, or_, select
from sqlalchemy.orm import InstrumentedAttribute, Session
from sqlalchemy.sql import Select
from sqlalchemy.sql.selectable import Exists

from durable_worker_example.db.models import ComparisonRequest, FeatureArtifact, ScoredCandidate, TopComparison
from dbworker import Coordinator, ExecutionStatus, Finished, Outcome, Unfinished


def ready_candidates(request: ComparisonRequest) -> Select[tuple[FeatureArtifact]]:
    completed = exists().where(
        ScoredCandidate.request_id == request.id,
        ScoredCandidate.candidate_artifact_id == FeatureArtifact.id,
    )
    return select(FeatureArtifact).where(
        FeatureArtifact.workspace_id == request.workspace_id,
        FeatureArtifact.id != request.query_artifact_id,
        FeatureArtifact.feature_json.is_not(None),
        ~completed,
    ).order_by(FeatureArtifact.id)


def unfinished_builds(
    workspace_id: int | InstrumentedAttribute[int], coordinator: Coordinator,
) -> Exists:
    return exists().where(
        FeatureArtifact.workspace_id == workspace_id,
        FeatureArtifact.feature_json.is_(None),
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
    if query_artifact is None or query_artifact.feature_json is None:
        raise ValueError("Comparison query has no features")
    query = query_artifact.feature_json
    candidates: list[tuple[int, dict[str, int]]] = []
    for artifact in session.scalars(ready_candidates(request).limit(page_size)):
        if artifact.feature_json is None:
            raise ValueError("A selected candidate has no features")
        candidates.append((artifact.id, artifact.feature_json))
    session.rollback()
    scores: list[tuple[int, float]] = []
    query_norm = math.sqrt(sum(value * value for value in query.values()))
    for artifact_id, features in candidates:
        numerator = sum(value * features.get(key, 0) for key, value in query.items())
        candidate_norm = math.sqrt(sum(value * value for value in features.values()))
        score = numerator / (query_norm * candidate_norm) if query_norm and candidate_norm else 0.0
        scores.append((artifact_id, score))
    current = session.get(ComparisonRequest, request_id)
    if current is None:
        raise ValueError("Comparison request no longer exists")
    if scores:
        previous = session.scalars(select(TopComparison).where(TopComparison.request_id == request_id))
        combined = {row.candidate_artifact_id: row.score for row in previous}
        combined.update(scores)
        best = sorted(combined.items(), key=lambda item: (-item[1], item[0]))[:current.retained_max_k]
        session.execute(delete(TopComparison).where(TopComparison.request_id == request_id))
        session.add_all(TopComparison(request_id=request_id, candidate_artifact_id=key, score=score) for key, score in best)
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
        candidate.c.feature_json.is_not(None),
        ~completed,
    )
    return (
        select(ComparisonRequest)
        .join(query, query.c.id == ComparisonRequest.query_artifact_id)
        .where(query.c.feature_json.is_not(None), or_(
            has_candidates, ~unfinished_builds(ComparisonRequest.workspace_id, coordinator),
        ))
        .order_by(ComparisonRequest.id)
    )
