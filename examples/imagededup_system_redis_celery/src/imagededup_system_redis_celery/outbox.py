"""Bridge SQL commits and Redis publication without losing committed work."""

import logging
from datetime import datetime, timedelta

from kombu.exceptions import OperationalError  # type: ignore[import-untyped]
from sqlalchemy import delete, select
from sqlalchemy.orm import Session, sessionmaker

from imagededup_system_redis_celery.celery_app import app
from imagededup_system_redis_celery.config import settings
from imagededup_system_redis_celery.db.models import OutboxMessage

logger = logging.getLogger(__name__)


def enqueue(session: Session, task_name: str, source_id: int, revision: int = 0, *, delay: float = 0) -> str:
    message = OutboxMessage(task_name=task_name, source_id=source_id, source_revision=revision,
                            available_at=datetime.utcnow() + timedelta(seconds=delay))
    session.add(message)
    session.flush()
    return message.id


def publish_pending(session_factory: sessionmaker[Session], message_ids: list[str] | None = None) -> int:
    with session_factory() as session:
        query = select(OutboxMessage).order_by(OutboxMessage.available_at, OutboxMessage.id)
        if message_ids is not None:
            if not message_ids:
                return 0
            query = query.where(OutboxMessage.id.in_(message_ids))
        messages = [(message.id, message.task_name, message.source_id, message.source_revision, message.available_at)
                    for message in session.scalars(query.limit(settings.outbox_batch_size))]
    if not messages:
        return 0
    sent: list[str] = []
    try:
        # Reuse a single producer/connection for the batch. Never hold a SQL
        # transaction open while performing broker I/O.
        with app.producer_or_acquire() as producer:
            for message_id, task_name, source_id, revision, available_at in messages:
                app.send_task(task_name, args=[source_id, revision], task_id=message_id, producer=producer,
                              countdown=max(0, (available_at - datetime.utcnow()).total_seconds()), retry=False)
                sent.append(message_id)
    except (OperationalError, OSError):
        logger.warning("Redis publication failed; committed messages remain for the dispatcher", exc_info=True)
    if sent:
        with session_factory.begin() as session:
            session.execute(delete(OutboxMessage).where(OutboxMessage.id.in_(sent)))
    # A crash after sending but before deletion can duplicate delivery; handlers
    # fence their final writes with the SQL revision, not a Celery task ID.
    return len(sent)
