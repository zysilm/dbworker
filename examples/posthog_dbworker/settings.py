"""Native settings with only invocation-owned service and transport overrides.

The native app registry, task imports, queues, retry decorators and worker hooks
are inherited unchanged. Missing native dependencies fail admission.
"""
import os
from posthog.settings import *  # noqa: F403

SECRET_KEY = "isolated-posthog-native-benchmark-only-secret"
DATABASES = {"default": {"ENGINE": "django.db.backends.postgresql",
    "NAME": os.environ["POSTHOG_BENCHMARK_DATABASE"], "USER": "benchmark",
    "HOST": "127.0.0.1", "PORT": os.environ["POSTHOG_BENCHMARK_PG_PORT"], "CONN_MAX_AGE": 0}}
CELERY_BROKER_URL = os.environ["BENCHMARK_REDIS_URL"]
CELERY_RESULT_BACKEND = CELERY_BROKER_URL
CELERY_TASK_ALWAYS_EAGER = False
EMAIL_BACKEND = "django.core.mail.backends.smtp.EmailBackend"
CUSTOMER_IO_API_KEY = ""
SITE_URL = "https://benchmark.invalid"
MESSAGING_HASH_SALT = "deterministic-benchmark-messaging-salt"
MESSAGING_HASH_SALT_FALLBACKS = []
