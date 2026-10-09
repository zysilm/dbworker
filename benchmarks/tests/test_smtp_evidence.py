"""Adversarial replay of actual serialized SMTP messages, without application boot."""
import copy
import hashlib
import json
import tempfile
import unittest
from email.message import EmailMessage
from pathlib import Path

from benchmarks.common.smtp_evidence import (business_digest, observe_messages,
    recipients, replay_messages, validate_smtp_evidence)


class SmtpEvidenceTest(unittest.TestCase):
    def setUp(self):
        messages = []
        for operation in map(str, range(100)):
            for index, recipient in enumerate(recipients(operation)):
                message = EmailMessage()
                message['From'] = 'sender@benchmark.invalid'
                message['To'] = recipient
                message['Reply-To'] = recipients(operation)[1 - index]
                message['Subject'] = f'Historical Sentry fixture {operation}'
                message['X-Benchmark'] = operation
                message['Message-Id'] = '<20260102030405.1234.42@benchmark.invalid>'
                message['Date'] = 'Fri, 02 Jan 2026 03:04:05 +0000'
                message.set_content(f'Plain body {operation} with unicode: café')
                message.add_alternative(f'<html><body><p class="fixture" style="color: red">HTML body {operation}: café</p></body></html>', subtype='html')
                messages.append(('sender@benchmark.invalid', [recipient], message.as_bytes()))
        self.manifest = observe_messages(messages)
        self.operations = list(map(str, range(100)))

    def test_full_observed_receipts_replay_and_collisions_are_not_duplicate_deliveries(self):
        normalized, ids = replay_messages(self.manifest, self.operations)
        self.assertEqual(len(normalized), 200)
        self.assertEqual(ids['collisions'], 199)
        self.assertEqual(ids['distinct'], 1)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, 'smtp-evidence.json')
            raw = json.dumps(self.manifest).encode()
            path.write_bytes(raw)
            row = {'smtp_evidence': {'schema_version': 1, 'path': path.name,
                'sha256': hashlib.sha256(raw).hexdigest()}, 'validation': {
                'messages': 200, 'output_digest': business_digest(normalized), 'generated_message_ids': ids}}
            self.assertEqual(validate_smtp_evidence(row, directory)['messages'], 200)
            for field, replacement in [('output_digest', '0' * 64), ('messages', 198), ('generated_message_ids', {})]:
                forged = copy.deepcopy(row)
                forged['validation'][field] = replacement
                with self.assertRaises(ValueError):
                    validate_smtp_evidence(forged, directory)
            path.write_bytes(raw + b' ')
            with self.assertRaisesRegex(ValueError, 'SHA-256'):
                validate_smtp_evidence(row, directory)

    def test_wrong_content_identity_envelope_and_header_occurrences_are_rejected(self):
        modifications = [
            lambda m: m['deliveries'].pop(),
            lambda m: m['deliveries'].append(copy.deepcopy(m['deliveries'][0])),
            lambda m: m['deliveries'][0]['headers']['Subject'].__setitem__(0, 'Forged subject'),
            lambda m: m['deliveries'][0]['headers']['X-Benchmark'].__setitem__(0, 'warmup:-2'),
            lambda m: m['deliveries'][0].__setitem__('destinations', ['other@benchmark.invalid']),
            lambda m: m['deliveries'][0]['headers']['Message-Id'].append('<20260102030405.1234.2@benchmark.invalid>'),
            lambda m: m['deliveries'][0]['headers']['Date'].__setitem__(0, 'invalid'),
            lambda m: m['deliveries'][0]['parts'][0].__setitem__('content', 'Forged body'),
            lambda m: m['deliveries'][0]['parts'][1].__setitem__('content', '<p class="fixture" style="background-color:red">HTML body 0: café</p>'),
            lambda m: m['deliveries'][0]['parts'][1].__setitem__('content', '<p class="fixture" style="color:red;color:blue">HTML body 0: café</p>'),
            lambda m: m['deliveries'][0]['parts'][1].__setitem__('content', '<p class="fixture" style="color:red">HTML body 0: café</p>Forged extra body'),
            lambda m: m['deliveries'][0]['headers']['Message-Id'].__setitem__(0, '<20269902030405.1234.42@benchmark.invalid>'),
            lambda m: m.__setitem__('schema_version', True),
        ]
        for index, modify in enumerate(modifications):
            with self.subTest(case=index):
                forged = copy.deepcopy(self.manifest)
                modify(forged)
                with self.assertRaises(ValueError):
                    replay_messages(forged, self.operations)

    def test_expected_operations_are_not_derived_from_reported_fixture(self):
        normalized, ids = replay_messages(self.manifest, self.operations)
        manifest = copy.deepcopy(self.manifest)
        manifest['deliveries'] = manifest['deliveries'][:2]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, 'smtp-evidence.json')
            raw = json.dumps(manifest).encode()
            path.write_bytes(raw)
            row = {'dataset': {'operations': 1}, 'smtp_evidence': {'schema_version': 1,
                'path': path.name, 'sha256': hashlib.sha256(raw).hexdigest()}, 'validation': {
                'messages': 200, 'output_digest': business_digest(normalized), 'generated_message_ids': ids}}
            with self.assertRaisesRegex(ValueError, 'missing distinct'):
                validate_smtp_evidence(row, directory)
            row['smtp_evidence']['path'] = '../smtp-evidence.json'
            with self.assertRaisesRegex(ValueError, 'escapes'):
                validate_smtp_evidence(row, directory)


class VariableTrustedSmtpCountTests(unittest.TestCase):
    setUp = SmtpEvidenceTest.setUp

    def test_nonhistorical_trusted_count_and_wrong_count_rejection(self):
        manifest = copy.deepcopy(self.manifest)
        manifest["deliveries"] = manifest["deliveries"][:6]
        normalized, ids = replay_messages(manifest, ["0", "1", "2"])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory, "smtp-evidence.json")
            raw = json.dumps(manifest).encode()
            path.write_bytes(raw)
            row = {"dataset": {"operations": 3}, "smtp_evidence": {
                "schema_version": 1, "path": path.name, "sha256": hashlib.sha256(raw).hexdigest()},
                "validation": {"messages": 6, "output_digest": business_digest(normalized), "generated_message_ids": ids}}
            self.assertEqual(validate_smtp_evidence(row, directory, expected_requests=3)["messages"], 6)
            with self.assertRaisesRegex(ValueError, "missing distinct"):
                validate_smtp_evidence(row, directory, expected_requests=4)
            with self.assertRaisesRegex(ValueError, "missing distinct"):
                validate_smtp_evidence(row, directory)
            for count in (0, -1, True, 3.0):
                with self.assertRaisesRegex(ValueError, "trusted SMTP request count"):
                    validate_smtp_evidence(row, directory, expected_requests=count)
