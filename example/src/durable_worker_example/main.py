from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from durable_worker_example.api.routes import router
from durable_worker_example.config import settings
from durable_worker_example.domain.workflows import create_workers
from durable_worker_example.db.engine import Base, create_engine_and_session_factory
from durable_worker_example.worker.runtime import Coordinator


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    engine, session_factory = create_engine_and_session_factory(settings.database_url)
    Base.metadata.create_all(engine)
    coordinator = Coordinator(
        session_factory, lease_seconds=settings.claim_lease_seconds,
        poll_seconds=settings.poll_seconds, max_poll_seconds=settings.max_poll_seconds,
    )
    builds, comparisons = create_workers(settings)
    coordinator.register(builds)
    coordinator.register(comparisons)
    coordinator.create_worker_tables()
    app.state.build_worker = builds
    app.state.comparison_worker = comparisons
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
