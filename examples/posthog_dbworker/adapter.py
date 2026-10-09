"""Preserve native notification construction and split its delivery publication."""
from contextlib import contextmanager


def initialize():
    from examples.posthog_dbworker.bootstrap import initialize as bootstrap
    return bootstrap()


@contextmanager
def route_delivery(submit):
    """Replace only the selected publication seam inside one DBWorker process."""
    from posthog.email import _send_email
    original = _send_email.apply_async
    def capture(args=None, kwargs=None, **options):
        if args:
            raise ValueError("The reviewed notification publishes keyword arguments")
        if options:
            raise ValueError("Unreviewed delivery publication options")
        return submit(dict(kwargs or {}))
    _send_email.apply_async = capture
    try:
        yield
    finally:
        _send_email.apply_async = original


def original_body(task):
    # Celery stores the original decorated callable before adding autoretry.
    # Retry/backoff is supplied by the DBWorker durable job lifecycle instead.
    return getattr(task, "_orig_run", task.run)


def notify(user_id, submit):
    from posthog.tasks.email import send_two_factor_auth_enabled_email
    with route_delivery(submit):
        return original_body(send_two_factor_auth_enabled_email)(user_id)


def deliver(payload):
    from posthog.email import _send_email
    return original_body(_send_email)(**payload)
