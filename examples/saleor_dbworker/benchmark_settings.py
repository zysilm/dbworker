"""Isolated infrastructure overrides for the unmodified Saleor application."""

import os

from saleor.settings import *  # noqa: F403

SECRET_KEY = "isolated-benchmark-only-secret"
MEDIA_ROOT = os.environ["SALEOR_BENCHMARK_MEDIA"]
MEDIA_URL = "/media/"
STORAGES = {
    "default": {"BACKEND": "django.core.files.storage.FileSystemStorage"},
    "staticfiles": {"BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage"},
}
CACHES = {"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}}
CELERY_TASK_ALWAYS_EAGER = os.environ.get("SALEOR_BENCHMARK_SETUP") == "1"
CELERY_TASK_EAGER_PROPAGATES = True
