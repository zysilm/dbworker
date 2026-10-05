"""Register actual upstream notification models only."""

from django.apps import AppConfig
from pathlib import Path


class NotificationConfig(AppConfig):
    name = "posthog"
    label = "posthog"
    path = str(Path(__file__).resolve().parent)

    def import_models(self):
        from examples.posthog_dbworker.bootstrap import load_model_support
        self.models = self.apps.all_models[self.label]
        load_model_support()
        from posthog.models import messaging, instance_setting
        self.models_module = messaging
