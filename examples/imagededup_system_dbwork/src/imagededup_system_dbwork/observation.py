"""Optional external observation; ordinary example deployments need no observer."""

import importlib
import os
from contextlib import nullcontext


def observe_handler(stage, source_id, session):
    module = os.environ.get("DBWORKER_OBSERVER_MODULE")
    if not module:
        return nullcontext()
    return importlib.import_module(module).observe_handler(stage, source_id, session)
