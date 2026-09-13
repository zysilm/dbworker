import logging
from typing import Any

from celery import Task  # type: ignore[import-untyped]
from sqlalchemy import update
from sqlalchemy.exc import OperationalError

from imagededup_system_redis_celery.celery_app import app
from imagededup_system_redis_celery.config import settings
from imagededup_system_redis_celery.db.engine import get_session_factory
from imagededup_system_redis_celery.db.models import ComparisonRequest, ExecutionStatus, FeatureArtifact
from imagededup_system_redis_celery.domain.artifact_build import build_artifact
from imagededup_system_redis_celery.domain.comparison import compare_page
from imagededup_system_redis_celery.outbox import publish_pending

logger = logging.getLogger(__name__)


class ImageTask(Task):  # type: ignore[misc]
    """Record terminal errors only for the revision this delivery owns."""

    autoretry_for = (OperationalError,)
    retry_backoff = True
    retry_backoff_max = 10
    retry_jitter = True
    max_retries = 5

    def on_failure(self, exc: BaseException, task_id: str, args: Any, kwargs: Any, einfo: Any) -> None:
        model = FeatureArtifact if self.name == "images.build" else ComparisonRequest
        try:
            with get_session_factory().begin() as session:
                session.execute(update(model).where(
                    model.id == args[0], model.revision == args[1],
                ).values(execution_status=ExecutionStatus.FAILED, error=str(exc), revision=args[1] + 1))
        except Exception:
            logger.exception("Could not persist terminal task failure for %s", task_id)


@app.task(name="images.build", base=ImageTask)  # type: ignore[untyped-decorator]
def build(artifact_id: int, revision: int) -> None:
    build_artifact(artifact_id, revision, get_session_factory())


@app.task(name="images.compare", base=ImageTask)  # type: ignore[untyped-decorator]
def compare(request_id: int, revision: int) -> None:
    session_factory = get_session_factory()
    continuation = compare_page(request_id, revision, session_factory,
                                page_size=settings.comparison_page_size,
                                dependency_wait=settings.dependency_wait_seconds)
    if continuation:
        publish_pending(session_factory, [continuation])


@app.task(name="images.dispatch")  # type: ignore[untyped-decorator]
def dispatch() -> None:
    publish_pending(get_session_factory())
