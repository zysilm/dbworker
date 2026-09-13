from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from imagededup_system_redis_celery.api.routes import router
from imagededup_system_redis_celery.db.engine import Base, dispose_engine, get_engine, get_session_factory


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    Base.metadata.create_all(get_engine())
    app.state.session_factory = get_session_factory()
    try:
        yield
    finally:
        dispose_engine()


def create_app() -> FastAPI:
    app = FastAPI(title="imagededup_system_redis_celery", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()


def run() -> None:
    uvicorn.run("imagededup_system_redis_celery.main:app", host="127.0.0.1", port=8002)
