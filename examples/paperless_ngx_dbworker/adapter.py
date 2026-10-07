"""Initialize the pristine Paperless application for its DBWorker variation."""
from functools import cache


@cache
def initialize():
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "paperless_ngx" / "src"))
    import django
    django.setup()
    from paperless.parsers.registry import init_builtin_parsers
    init_builtin_parsers()
    # Task modules are unchanged and provide the bodies and lifecycle helpers.
    import documents.tasks
    import documents.workflows.webhooks
