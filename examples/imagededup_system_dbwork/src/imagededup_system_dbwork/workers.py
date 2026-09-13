"""Shared worker definitions; importing this module starts no processing."""

from sqlalchemy import select
from sqlalchemy.orm import Session

from dbworker import Coordinator, Finished, Outcome
from imagededup_system_dbwork.config import settings
from imagededup_system_dbwork.domain import artifact_build, comparison
from imagededup_system_dbwork.db.models import ComparisonRequest, FeatureArtifact
from imagededup_system_dbwork.db.engine import create_engine_and_session_factory


engine, session_factory = create_engine_and_session_factory(settings.database_url)
coordinator = Coordinator(
    session_factory, database_url=settings.database_url, lease_seconds=settings.claim_lease_seconds,
    poll_seconds=settings.poll_seconds, max_poll_seconds=settings.max_poll_seconds,
)


@coordinator.transactional_worker(
    name="artifact_build", source=FeatureArtifact,
    eligible=lambda: select(FeatureArtifact).where(FeatureArtifact.hash_value.is_(None)).order_by(FeatureArtifact.id),
    concurrency=settings.build_workers,
)
def build_artifact(artifact: FeatureArtifact, session: Session) -> Finished:
    return artifact_build.build_artifact(artifact, session)


@coordinator.transactional_worker(
    name="comparison", source=ComparisonRequest,
    eligible=lambda: comparison.eligible_comparisons(coordinator),
    concurrency=settings.comparison_workers,
)
def compare_artifacts(request: ComparisonRequest, session: Session) -> Outcome:
    return comparison.compare_artifacts(
        request, session, page_size=settings.comparison_page_size,
        coordinator=coordinator,
    )
