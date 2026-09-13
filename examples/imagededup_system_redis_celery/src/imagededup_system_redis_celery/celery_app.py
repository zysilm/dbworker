from typing import Any

from celery import Celery  # type: ignore[import-untyped]
from celery.signals import worker_process_init, worker_process_shutdown  # type: ignore[import-untyped]

from imagededup_system_redis_celery.config import settings
from imagededup_system_redis_celery.db.engine import dispose_engine

app = Celery("imagededup_system_redis_celery", broker=settings.broker_url,
             include=["imagededup_system_redis_celery.tasks"])
app.conf.update(
    task_serializer="json", accept_content=["json"], task_ignore_result=True,
    task_acks_late=True, task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1, worker_cancel_long_running_tasks_on_connection_loss=True,
    broker_connection_retry_on_startup=True, broker_connection_timeout=3,
    broker_transport_options={"visibility_timeout": 3600, "global_keyprefix": "imagededup:"},
    task_soft_time_limit=110, task_time_limit=120,
    task_routes={
        "images.build": {"queue": "image_build"},
        "images.compare": {"queue": "image_compare"},
        "images.dispatch": {"queue": "image_control"},
    },
    beat_schedule={"recover-pending-publications": {"task": "images.dispatch", "schedule": 1.0}},
)


def reset_connections(**kwargs: Any) -> None:
    dispose_engine()


worker_process_init.connect(reset_connections, weak=False)
worker_process_shutdown.connect(reset_connections, weak=False)
