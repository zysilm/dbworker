"""Exercise separate worker readiness, shutdown, and scheduler failure."""

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class PostHogWorkerServiceTests(unittest.TestCase):
    def launch(self, directory, *, fail=False):
        arguments = ["--database-url", f"sqlite:///{directory / 'jobs.db'}",
                     "--ready-file", str(directory / "ready.json"), "--concurrency", "8"]
        if fail:
            script = """
from examples.posthog_dbworker import worker
original = worker.coordinator
def create(*args, **kwargs):
    runtime = original(*args, **kwargs)
    def fail(*unused):
        raise RuntimeError('injected scheduler failure')
    runtime._run = fail
    return runtime
worker.coordinator = create
worker.main()
"""
            command = [sys.executable, "-c", script, *arguments]
        else:
            command = [sys.executable, "-m", "examples.posthog_dbworker.worker", *arguments]
        environment = dict(os.environ, PYTHONPATH=os.pathsep.join((str(ROOT), str(ROOT / "src"))))
        child = subprocess.Popen(command, cwd=ROOT, env=environment,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.addCleanup(self.close, child)
        return child

    @staticmethod
    def close(child):
        if child.poll() is None:
            child.kill()
        child.communicate(timeout=10)

    def test_idle_service_receipt_and_graceful_shutdown(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            child = self.launch(directory)
            deadline = time.monotonic() + 10
            while not (directory / "ready.json").exists():
                if child.poll() is not None or time.monotonic() > deadline:
                    self.fail(f"Worker readiness failed: {child.communicate(timeout=10)!r}")
                time.sleep(.02)
            receipt = json.loads((directory / "ready.json").read_text())
            self.assertEqual(receipt["pid"], child.pid)
            self.assertEqual(receipt["concurrency"], 8)
            self.assertIn("application warmup is separate", receipt["readiness_scope"])
            child.terminate()
            output = child.communicate(timeout=10)
            self.assertEqual(child.returncode, 0, output)

    def test_scheduler_failure_terminates_service(self):
        with tempfile.TemporaryDirectory() as temporary:
            child = self.launch(Path(temporary), fail=True)
            _, error = child.communicate(timeout=10)
            self.assertNotEqual(child.returncode, 0)
            self.assertIn(b"injected scheduler failure", error)
            self.assertIn(b"coordinator scheduling thread failed", error)
