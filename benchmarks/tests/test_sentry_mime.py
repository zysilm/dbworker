"""Independent SMTP oracle checks for recipient fan-out and rendered content."""

import unittest
from email.message import EmailMessage

from benchmarks.upstream.sentry_backend import recipients, validate_messages


class SentryMimeTest(unittest.TestCase):
    def messages(self, *, inline=True):
        rows = []
        for index, recipient in enumerate(recipients("0")):
            message = EmailMessage()
            message["From"] = "sender@benchmark.invalid"
            message["To"] = recipient
            message["Reply-To"] = recipients("0")[1 - index]
            message["Subject"] = "Historical Sentry fixture 0"
            message["Message-Id"] = f"<fixture-{index}@benchmark.invalid>"
            message["X-Benchmark"] = "0"
            message.set_content("Plain body 0 with unicode: café")
            style = ' style="color: red"' if inline else ""
            message.add_alternative(f'<html><body><p class="fixture"{style}>HTML body 0: café</p></body></html>', subtype="html")
            rows.append(("sender@benchmark.invalid", [recipient], message.as_bytes()))
        return rows

    def test_exact_distinct_recipient_messages_and_rendered_content(self):
        rows = validate_messages(self.messages(), ["0"])
        self.assertEqual(len(rows), 2)
        self.assertEqual([row["to"][0] for row in rows], recipients("0"))

    def test_missing_and_duplicate_smtp_receipts_are_rejected(self):
        rows = self.messages()
        with self.assertRaisesRegex(AssertionError, "missing distinct"):
            validate_messages(rows[:1], ["0"])
        with self.assertRaisesRegex(AssertionError, "duplicate delivery"):
            validate_messages([*rows, rows[0]], ["0"])

    def test_css_and_envelope_are_business_outputs(self):
        with self.assertRaisesRegex(AssertionError, "CSS was not inlined"):
            validate_messages(self.messages(inline=False), ["0"])
        rows = self.messages()
        sender, _, raw = rows[0]
        rows[0] = (sender, ["other@benchmark.invalid"], raw)
        with self.assertRaisesRegex(AssertionError, "envelope"):
            validate_messages(rows, ["0"])
