"""Dedicated real notification model application for paired measurements."""

import os
from pathlib import Path

SECRET_KEY = "isolated-posthog-benchmark-only-secret"
INSTALLED_APPS = ["examples.posthog_dbworker.apps.NotificationConfig"]
DATABASES = {"default": {"ENGINE": "django.db.backends.postgresql",
                         "NAME": os.environ["POSTHOG_BENCHMARK_DATABASE"], "USER": os.environ.get("POSTHOG_BENCHMARK_PG_USER", "benchmark"),
                         "HOST": "127.0.0.1", "PORT": os.environ["POSTHOG_BENCHMARK_PG_PORT"]}}
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
TIME_ZONE = "UTC"
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
CUSTOMER_IO_API_KEY = ""
SITE_URL = "https://benchmark.invalid"
STATIC_URL = "/static/"
MESSAGING_HASH_SALT = "deterministic-benchmark-messaging-salt"
MESSAGING_HASH_SALT_FALLBACKS = []
TEMPLATES = [{"BACKEND": "django.template.backends.django.DjangoTemplates",
              "DIRS": [str(Path(__file__).resolve().parents[1] / "posthog/posthog/templates")],
              "APP_DIRS": False, "OPTIONS": {"builtins": ["posthog.templatetags.posthog_assets",
                                                          "posthog.templatetags.posthog_filters"]}}]
CELERY_BROKER_URL = os.environ.get("BENCHMARK_REDIS_URL", "redis://127.0.0.1:6379/0")
