"""Initialize the complete pristine upstream application; never project modules."""
import os
import sys
from functools import cache
from pathlib import Path

SOURCE_ROOT = Path(__file__).resolve().parents[1] / "posthog"

@cache
def initialize():
    sys.path.insert(0, str(SOURCE_ROOT))
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "examples.posthog_dbworker.settings")
    import django
    django.setup()
    from posthog.celery import app
    app.loader.import_default_modules()
    return app
