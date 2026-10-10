"""Check independent SMTP ownership, exact envelopes, and service failure."""

import importlib.util
import os
import smtplib
import socket
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from benchmarks.common.smtp_receiver import SMTPReceiver


@unittest.skipUnless(importlib.util.find_spec("aiosmtpd"), "Requires the native SMTP environment")
class SMTPReceiverTest(unittest.TestCase):
    def setUp(self):
        with socket.socket() as connection:
            connection.bind(("127.0.0.1", 0))
            self.port = connection.getsockname()[1]
        self.receiver = SMTPReceiver(self.port, timeout=5)
        self.addCleanup(self.receiver.stop)
        self.receiver.start()

    def test_independent_process_preserves_envelopes_and_duplicate_counts(self):
        self.assertNotEqual(self.receiver.process.pid, os.getpid())
        content = b"From: sender@benchmark.invalid\r\nSubject: same native envelope\r\n\r\nBody.\r\n"
        with smtplib.SMTP("127.0.0.1", self.port, timeout=5) as client:
            for recipient in ("first@benchmark.invalid", "second@benchmark.invalid", "first@benchmark.invalid"):
                client.sendmail("sender@benchmark.invalid", [recipient], content)
        self.assertEqual(self.receiver.accepted_count({"first@benchmark.invalid"}), 2)
        self.assertEqual(self.receiver.accepted_count({"second@benchmark.invalid"}), 1)
        self.assertEqual(self.receiver.accepted_count({"missing@benchmark.invalid"}), 0)
        messages = self.receiver.messages
        self.assertEqual(len(messages), 3)
        self.assertTrue(all(message["content"] == content for message in messages))
        self.assertTrue(all(message["mail_from"] == "sender@benchmark.invalid" for message in messages))
        self.assertEqual([message["rcpt_tos"] for message in messages],
                         [["first@benchmark.invalid"], ["second@benchmark.invalid"], ["first@benchmark.invalid"]])

    def test_dead_receiver_fails_instead_of_reporting_empty_progress(self):
        self.receiver.process.terminate()
        self.receiver.process.join(5)
        with self.assertRaisesRegex(RuntimeError, "receiver exited"):
            self.receiver.accepted_count({"first@benchmark.invalid"})

    def test_stop_releases_owned_process_and_port(self):
        self.receiver.stop()
        self.assertFalse(self.receiver.process.is_alive())
        with socket.socket() as connection:
            connection.bind(("127.0.0.1", self.port))

    def test_failed_shutdown_send_still_reaps_the_live_receiver(self):
        with patch.object(self.receiver.channel, "send", side_effect=BrokenPipeError("Controlled shutdown race")):
            self.receiver.stop()
        self.assertFalse(self.receiver.process.is_alive())
        with socket.socket() as connection:
            connection.bind(("127.0.0.1", self.port))


if __name__ == "__main__":
    unittest.main()
