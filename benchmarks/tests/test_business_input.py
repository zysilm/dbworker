"""Semantic evidence follows actual inputs across publication and execution."""
import hashlib
import tempfile
import unittest
from dataclasses import dataclass
from pathlib import Path

from benchmarks.common.business_input import business_input


@dataclass
class Upload:
    original_file: Path


class BusinessInputTests(unittest.TestCase):
    def test_sql_positional_and_keyword_arguments(self):
        expected = {'query_id': 21, 'sql_sha256': hashlib.sha256(b'SELECT 9').hexdigest()}
        self.assertEqual(business_input('sql_lab.get_sql_results', (21, 'SELECT 9'), {}), expected)
        self.assertEqual(business_input('sql_lab.get_sql_results', (),
                                       {'query_id': 21, 'rendered_query': 'SELECT 9'}), expected)
        self.assertNotEqual(business_input('sql_lab.get_sql_results', (22, 'SELECT 9'), {}), expected)
        self.assertNotEqual(business_input('sql_lab.get_sql_results', (21, 'SELECT 8'), {}), expected)

    def test_sql_missing_duplicate_and_invalid_values_fail_closed(self):
        for args, kwargs in [((21,), {}), ((21, 'SELECT 9'), {'query_id': 21}),
                             ((True, 'SELECT 9'), {}), ((0, 'SELECT 9'), {}), ((21, b'SQL'), {})]:
            with self.subTest(args=args), self.assertRaises(ValueError):
                business_input('sql_lab.get_sql_results', args, kwargs)

    def test_actual_upload_bytes_not_file_name_or_fixture_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'scan.png'
            path.write_bytes(b'first scan bytes')
            document = Upload(path)
            first = business_input('documents.tasks.consume_file', (), {'input_doc': document})
            self.assertEqual(first, {'input_sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
            self.assertEqual(first, business_input('documents.tasks.consume_file', (document,), {}))
            path.write_bytes(b'different scan bytes')
            self.assertNotEqual(first, business_input('documents.tasks.consume_file', (), {'input_doc': document}))
            path.unlink()
            with self.assertRaises(FileNotFoundError):
                business_input('documents.tasks.consume_file', (document,), {})

    def test_other_original_tasks_have_no_semantic_receipt(self):
        self.assertEqual(business_input('sentry.tasks.email.send_email', (), {}), {})


if __name__ == '__main__':
    unittest.main()
