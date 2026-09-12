from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from durable_worker_example.api.routes import router
from durable_worker_example.config import settings
from durable_worker_example.db.engine import Base, create_engine_and_sessions
from durable_worker_example.worker.runtime import Coordinator


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine, sessions = create_engine_and_sessions(settings.database_url)
    Base.metadata.create_all(engine)
    coordinator = Coordinator(sessions, settings)
    coordinator.start()
    app.state.engine = engine
    app.state.sessions = sessions
    app.state.coordinator = coordinator
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
