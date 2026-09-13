from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from imagededup_system_dbwork.api.routes import router
from imagededup_system_dbwork.db.engine import Base
from imagededup_system_dbwork.workers import coordinator, engine, session_factory


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    Base.metadata.create_all(engine)
    coordinator.create_worker_tables()
    app.state.build_worker = coordinator.workers["artifact_build"]
    app.state.comparison_worker = coordinator.workers["comparison"]
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.coordinator = coordinator
    try:
        yield
    finally:
        engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="imagededup_system_dbwork", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()


def run() -> None:
    uvicorn.run("imagededup_system_dbwork.main_fastapi:app", host="127.0.0.1", port=8001, reload=False)
