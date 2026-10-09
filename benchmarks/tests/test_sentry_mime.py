"""Independent SMTP oracle checks for recipient fan-out and rendered content."""

import ast
import json
import tempfile
import time
import unittest
from email.message import EmailMessage
from email import policy
from email.parser import BytesParser
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import Mock, patch

from benchmarks.upstream.sentry_backend import SOURCE, delivery_completion, message_id_evidence, recipients, validate_messages
from benchmarks.common.workflow_graph import SuccessTraceCursor, validate_graph


class SentryMimeTest(unittest.TestCase):
    def test_completion_defers_full_graph_but_keeps_missing_and_duplicate_gates(self):
        rows = [{'schema_version': 1, 'backend': 'celery', 'operation_id': '0',
                 'node_id': f'job-{index}', 'stage': 'delivery', 'parent_id': None,
                 'event': phase, 'timestamp_ns': timestamp}
                for index in range(2)
                for phase, timestamp in [('submitted', 10), ('started', 20), ('succeeded', 30)]]
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'workflow.jsonl'
            def write(values):
                path.write_text(''.join(json.dumps(row) + '\n' for row in values))
            write(rows[:3])
            cursor = SuccessTraceCursor(path, ['0'])
            with patch('benchmarks.upstream.sentry_backend.validate_graph', wraps=validate_graph) as graph:
                for _ in range(10):
                    self.assertIsNone(delivery_completion(cursor, ['0'], 2, []))
                self.assertEqual(graph.call_count, 0)
                with path.open('a') as stream:
                    stream.write(''.join(json.dumps(row) + '\n' for row in rows[3:]))
                self.assertIsNone(delivery_completion(cursor, ['0'], 1, []))
                self.assertEqual(graph.call_count, 0)
                self.assertTrue(delivery_completion(cursor, ['0'], 2, []))
                self.assertEqual(graph.call_count, 1)
            write(rows + [rows[-1]])
            with self.assertRaisesRegex(ValueError, 'Duplicate task phase'):
                delivery_completion(SuccessTraceCursor(path, ['0']), ['0'], 2, [])
            for invalid in (rows[1:], rows + [{**rows[-1], 'node_id': 'unexpected', 'operation_id': 'unexpected'}]):
                write(invalid)
                self.assertIsNone(delivery_completion(SuccessTraceCursor(path, ['0']), ['0'], 2, []))

    def messages(self, *, inline=True, operation_id="0"):
        rows = []
        for index, recipient in enumerate(recipients(operation_id)):
            message = EmailMessage()
            message["From"] = "sender@benchmark.invalid"
            message["To"] = recipient
            message["Reply-To"] = recipients(operation_id)[1 - index]
            message["Subject"] = f"Historical Sentry fixture {operation_id}"
            message["Message-Id"] = f"<20260102030405.1234.{index}@benchmark.invalid>"
            message["X-Benchmark"] = operation_id
            message.set_content(f"Plain body {operation_id} with unicode: café")
            style = ' style="color: red"' if inline else ""
            message.add_alternative(f'<html><body><p class="fixture"{style}>HTML body {operation_id}: café</p></body></html>', subtype="html")
            rows.append(("sender@benchmark.invalid", [recipient], message.as_bytes()))
        return rows

    def test_large_operation_identity_retains_exact_recipient_granularity(self):
        rows = self.messages(operation_id="19999")
        normalized = validate_messages(rows, ["19999"])
        self.assertEqual(len(normalized), 2)
        self.assertEqual([item["to"][0] for item in normalized], recipients("19999"))
        with self.assertRaisesRegex(AssertionError, "unknown or duplicate"):
            validate_messages(rows, ["19998"])

    def test_original_native_id_collision_does_not_duplicate_distinct_deliveries(self):
        # Execute the pristine upstream function without booting unrelated Sentry
        # services. Fixed native time/PID/random inputs expose its finite range.
        source = SOURCE / "sentry/utils/email/message_builder.py"
        definition = next(node for node in ast.parse(source.read_text()).body
                          if isinstance(node, ast.FunctionDef) and node.name == "make_msgid")
        random = Mock(return_value=42)
        namespace = {"time": SimpleNamespace(time=lambda: 1700000000,
            strftime=time.strftime, gmtime=time.gmtime),
            "os": SimpleNamespace(getpid=lambda: 1234), "randrange": random}
        exec(compile(ast.Module(body=[definition], type_ignores=[]), str(source), "exec"), namespace)
        first = namespace["make_msgid"]("benchmark.invalid")
        second = namespace["make_msgid"]("benchmark.invalid")
        self.assertEqual(first, second)
        self.assertEqual(random.call_args_list[0].args, (100000,))
        rows = self.messages()
        collided = []
        for sender, destinations, raw in rows:
            message = BytesParser(policy=policy.default).parsebytes(raw)
            message.replace_header("Message-Id", first)
            collided.append((sender, destinations, message.as_bytes()))
        self.assertEqual(validate_messages(collided, ["0"]), validate_messages(rows, ["0"]))
        self.assertEqual(message_id_evidence(collided)["collisions"], 1)
        with self.assertRaisesRegex(AssertionError, "duplicate delivery"):
            validate_messages([*collided, collided[0]], ["0"])
        with self.assertRaisesRegex(AssertionError, "missing distinct"):
            validate_messages(collided[:1], ["0"])

    def test_missing_malformed_or_multiple_generated_headers_are_rejected(self):
        for replacement in (None, "<20260102030405.1234.1@wrong.invalid>",
                            "<malformed@benchmark.invalid>", "<20260102030405.1234.100000@benchmark.invalid>"):
            with self.subTest(header=replacement):
                rows = self.messages()
                sender, destinations, raw = rows[0]
                message = BytesParser(policy=policy.default).parsebytes(raw)
                del message["Message-Id"]
                if replacement is not None:
                    message["Message-Id"] = replacement
                rows[0] = (sender, destinations, message.as_bytes())
                with self.assertRaisesRegex(AssertionError, "absent or malformed"):
                    validate_messages(rows, ["0"])
        rows = self.messages()
        sender, destinations, raw = rows[0]
        rows[0] = (sender, destinations, b"Message-Id: <20260102030405.1234.2@benchmark.invalid>\n" + raw)
        with self.assertRaisesRegex(AssertionError, "absent or malformed"):
            validate_messages(rows, ["0"])

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
