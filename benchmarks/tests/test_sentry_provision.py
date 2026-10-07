"""Check the source-build CLI contract without installing historical dependencies."""

import unittest
from unittest.mock import patch

from benchmarks.upstream import sentry_provision


class SentryProvisionTest(unittest.TestCase):
    def test_both_application_arms_use_supported_uv_source_build_flags(self):
        with patch.object(sentry_provision.subprocess, "run") as run:
            sentry_provision.provision("uv")
        commands = [call.args[0] for call in run.call_args_list]
        builds = [command for command in commands if "--no-binary" in command]
        self.assertEqual(len(builds), 2)
        for role, command in zip(("celery", "dbworker"), builds):
            interpreter = str(sentry_provision.ROOT / "benchmarks/environments/sentry" / role / ".venv/bin/python")
            self.assertEqual(command, ["uv", "pip", "install", "--python", interpreter,
                "--no-cache", "--no-deps", "--no-binary", "lxml", "--no-binary", "xmlsec",
                "--reinstall-package", "lxml", "--reinstall-package", "xmlsec",
                "--build-constraint", "benchmarks/locks/sentry-build.txt",
                "lxml==4.9.3", "xmlsec==1.3.14"])
        lock = (sentry_provision.ROOT / "benchmarks/locks/sentry-native.txt").read_text().splitlines()
        self.assertIn("lxml==4.9.3", lock)
        self.assertIn("xmlsec==1.3.14", lock)
        self.assertFalse(any("--no-binary-package" in command for command in commands))


if __name__ == "__main__":
    unittest.main()
