"""Guard against broker publication inside a DBWorker business operation."""

from contextlib import contextmanager

class UnexpectedTaskDispatch(RuntimeError):
    pass


@contextmanager
def forbid_task_dispatch():
    """Reject nested broker publication and eager Celery execution in a handler.

    A handler process executes one business operation at a time. This guard is
    process-local and deliberately does not modify the upstream checkout.
    """
    from celery.app.task import Task
    from kombu import Producer

    def reject(*args, **kwargs):
        raise UnexpectedTaskDispatch("The DBWorker operation attempted Celery publication or eager execution")

    publish, apply = Producer.publish, Task.apply
    Producer.publish, Task.apply = reject, reject
    try:
        yield
    finally:
        Producer.publish, Task.apply = publish, apply
