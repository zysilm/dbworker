"""Application handlers and eligibility. The worker knows none of these tables."""

from collections.abc import Callable
from functools import partial

from sqlalchemy import delete, exists, or_, select
from sqlalchemy.orm import InstrumentedAttribute, Session, sessionmaker
from sqlalchemy.sql import Select
from sqlalchemy.sql.selectable import Exists

from durable_worker_example.config import Settings

from durable_worker_example.db.models import ComparisonRequest, Document, FeatureArtifact, ScoredCandidate, TopComparison
from durable_worker_example.domain.execution import build_features, score_feature_pairs
from durable_worker_example.worker.runtime import Finished, Outcome, Unfinished, Worker


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


def build_artifact(
    artifact: FeatureArtifact, session_factory: sessionmaker[Session], save_session: Callable[[], Session],
) -> Finished:
    with session_factory() as session:
        document = session.get(Document, artifact.document_id)
        if document is None:
            raise ValueError("Artifact document no longer exists")
        text = document.text
    features = build_features(text)
    session = save_session()
    current = session.get(FeatureArtifact, artifact.id)
    if current is None:
        raise ValueError("Artifact no longer exists")
    current.feature_json = features
    return Finished()


def unfinished_builds(workspace_id: int | InstrumentedAttribute[int]) -> Exists:
    build_table = FeatureArtifact.metadata.tables["artifact_build_work"]
    terminal_build = exists().where(
        build_table.c.source_id == FeatureArtifact.id,
        build_table.c.status.in_(["finished", "failed"]),
    )
    return exists().where(
        FeatureArtifact.workspace_id == workspace_id,
        FeatureArtifact.feature_json.is_(None),
        ~terminal_build,
    )


def compare_artifacts(
    request: ComparisonRequest, session_factory: sessionmaker[Session], save_session: Callable[[], Session],
    *, page_size: int,
) -> Outcome:
    with session_factory() as session:
        query_artifact = session.get(FeatureArtifact, request.query_artifact_id)
        if query_artifact is None or query_artifact.feature_json is None:
            raise ValueError("Comparison query has no features")
        query = query_artifact.feature_json
        candidates: list[tuple[int, dict[str, int]]] = []
        for artifact in session.scalars(ready_candidates(request).limit(page_size)):
            if artifact.feature_json is None:
                raise ValueError("A selected candidate has no features")
            candidates.append((artifact.id, artifact.feature_json))
    scores = score_feature_pairs(query, candidates) if candidates else []
    # This session checks ownership before any result writes. The child runtime
    # commits all application changes and the returned outcome together.
    session = save_session()
    current = session.get(ComparisonRequest, request.id)
    if current is None:
        raise ValueError("Comparison request no longer exists")
    if scores:
        previous = session.scalars(select(TopComparison).where(TopComparison.request_id == request.id))
        combined = {row.candidate_artifact_id: row.score for row in previous}
        combined.update(scores)
        best = sorted(combined.items(), key=lambda item: (-item[1], item[0]))[:current.retained_max_k]
        session.execute(delete(TopComparison).where(TopComparison.request_id == request.id))
        session.add_all(TopComparison(request_id=request.id, candidate_artifact_id=key, score=score) for key, score in best)
        session.add_all(ScoredCandidate(request_id=request.id, candidate_artifact_id=key) for key, _ in scores)
        current.candidates_scored_count += len(scores)
        session.flush()
    # One statement observes both sides of a build's transition: a build
    # finishing between separate queries must not make the request look done.
    remaining = session.scalar(select(or_(
        ready_candidates(current).order_by(None).exists(),
        unfinished_builds(current.workspace_id),
    )))
    return Unfinished() if remaining else Finished()


def create_workers(settings: Settings) -> tuple[Worker, Worker]:
    builds = Worker(
        name="artifact_build", source=FeatureArtifact, handler=build_artifact,
        eligible=lambda: select(FeatureArtifact).where(FeatureArtifact.feature_json.is_(None)).order_by(FeatureArtifact.id),
        concurrency=settings.build_workers,
    )

    def eligible_comparisons() -> Select[tuple[ComparisonRequest]]:
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
                has_candidates, ~unfinished_builds(ComparisonRequest.workspace_id),
            ))
            .order_by(ComparisonRequest.id)
        )

    comparisons = Worker(
        name="comparison", source=ComparisonRequest, eligible=eligible_comparisons,
        handler=partial(compare_artifacts, page_size=settings.comparison_page_size), concurrency=settings.comparison_workers,
    )
    return builds, comparisons
