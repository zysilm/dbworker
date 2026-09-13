from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.orm import Session

from durable_worker_example.api.routes import router
from durable_worker_example.config import settings
from durable_worker_example.domain import workflows
from durable_worker_example.db.models import ComparisonRequest, FeatureArtifact
from durable_worker_example.db.engine import Base, create_engine_and_session_factory
from dbworker import Coordinator, Finished, Outcome


engine, session_factory = create_engine_and_session_factory(settings.database_url)
coordinator = Coordinator(
    session_factory, database_url=settings.database_url, lease_seconds=settings.claim_lease_seconds,
    poll_seconds=settings.poll_seconds, max_poll_seconds=settings.max_poll_seconds,
)


@coordinator.transactional_worker(
    name="artifact_build", source=FeatureArtifact,
    eligible=lambda: select(FeatureArtifact).where(FeatureArtifact.feature_json.is_(None)).order_by(FeatureArtifact.id),
    concurrency=settings.build_workers,
)
def build_artifact(artifact: FeatureArtifact, session: Session) -> Finished:
    return workflows.build_artifact(artifact, session)


@coordinator.transactional_worker(
    name="comparison", source=ComparisonRequest,
    eligible=workflows.eligible_comparisons,
    concurrency=settings.comparison_workers,
)
def compare_artifacts(request: ComparisonRequest, session: Session) -> Outcome:
    return workflows.compare_artifacts(request, session, page_size=settings.comparison_page_size)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    Base.metadata.create_all(engine)
    coordinator.create_worker_tables()
    app.state.build_worker = coordinator.workers["artifact_build"]
    app.state.comparison_worker = coordinator.workers["comparison"]
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.coordinator = coordinator
    coordinator.start()
    try:
        yield
    finally:
        coordinator.stop()
        engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="Durable worker example", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()


def run() -> None:
    uvicorn.run("durable_worker_example.main:app", host="127.0.0.1", port=8001, reload=False)
