"""Keep the historical operation in its own interpreter, with no second queue."""

import json
import os
import subprocess
from pathlib import Path


def initialize():
    interpreter = Path(os.environ["SENTRY_UPSTREAM_PYTHON"])
    if not interpreter.is_file():
        raise FileNotFoundError(f"Missing historical Sentry interpreter: {interpreter}")


def execute(payload: dict) -> dict:
    command = [os.environ["SENTRY_UPSTREAM_PYTHON"], str(Path(__file__).with_name("legacy_send.py"))]
    environment = os.environ.copy()
    # The legacy interpreter imports the pinned checkout, not the modern queue
    # environment's site-packages or repository PYTHONPATH.
    environment["PYTHONPATH"] = os.environ["SENTRY_SOURCE_PATH"]
    completed = subprocess.run(command, input=json.dumps(payload), text=True, capture_output=True,
                               timeout=120, env=environment)
    if completed.returncode:
        raise RuntimeError(f"Historical Sentry bridge failed: {completed.stderr[-6000:]}")
    # The historical app may log to stdout; the last line is the protocol result.
    result = json.loads(completed.stdout.splitlines()[-1])
    if result.get("accepted") != len(payload["to"]):
        raise RuntimeError("The historical utility did not accept every message")
    return result
