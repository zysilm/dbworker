"""Adversarial durable SQL Lab observations, independently replayed offline."""
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from benchmarks.common.superset_output import digest, fixture_digest, validate_superset_output


class SupersetOutputTests(unittest.TestCase):
    def fixture(self, count=100):
        columns = [{'column_name': name, 'name': name, 'type': 'BIGINT', 'type_generic': 0, 'is_dttm': False}
                   for name in ('category', 'total')]
        observed, bindings = [], []
        for i in range(count):
            operation_id = f'query-{i}'
            result = {'data': [{'category': k, 'total': sum(range(k, 10000, 10)) + i} for k in range(10)],
                      'columns': copy.deepcopy(columns), 'status': 'success'}
            sql = f'SELECT category, SUM(value) + {i} AS total FROM facts GROUP BY category ORDER BY category'
            binding = {'operation_id': operation_id, 'query_id': i + 1, 'node_id': f'job-{i}',
                       'results_key': f'result-{i}', 'sql_sha256': hashlib.sha256(sql.encode()).hexdigest(),
                       'argument_sha256': format(i + 1, '064x'), 'output_sha256': digest(result)}
            bindings.append(binding)
            observed.append({key: value for key, value in binding.items() if key != 'output_sha256'}
                            | {'retrieved_timestamp_ns': 1500, 'http_status': 200, 'result': result})
        receipt = {'schema_version': 1, 'fixture': {'rows': 10000, 'ordered_input_sha256': fixture_digest()},
                   'results': observed}
        row = {'business_bindings': bindings, 'warmup_operations': ['warmup:0', 'warmup:1'],
               'metrics': {'wall_seconds': .000001},
               'measurement_window': {'schema_version': 1, 'clock_domain': 'unix_time_ns',
                   'start': {'timestamp_ns': 1000, 'monotonic_ns': 1000, 'uncertainty_ns': 0},
                   'end': {'timestamp_ns': 2000, 'monotonic_ns': 2000, 'uncertainty_ns': 0}},
               'validation': {'queries': count, 'retrieved_results': count, 'rows_per_query': 10,
                              'output_digest': digest([entry['result'] for entry in observed])}}
        return row, receipt

    def admit(self, row, receipt, count=100):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'sample' / 'sql-results-evidence.json'
            path.parent.mkdir()
            raw = json.dumps(receipt).encode()
            path.write_bytes(raw)
            row['sql_result_evidence'] = {'schema_version': 1, 'path': 'sample/sql-results-evidence.json',
                                         'sha256': hashlib.sha256(raw).hexdigest()}
            return validate_superset_output(row, directory, count)

    def test_all_100_actual_results_reconstruct_original_output_digest(self):
        row, receipt = self.fixture()
        replayed = self.admit(row, receipt)
        self.assertEqual(replayed, {'queries': 100, 'rows_per_query': 10,
                                   'output_digest': row['validation']['output_digest']})

    def test_common_wrong_output_with_all_hashes_recomputed_is_rejected(self):
        row, receipt = self.fixture()
        receipt['results'][0]['result']['data'][0]['total'] += 1
        row['business_bindings'][0]['output_sha256'] = digest(receipt['results'][0]['result'])
        row['validation']['output_digest'] = digest([entry['result'] for entry in receipt['results']])
        with self.assertRaisesRegex(ValueError, 'independent oracle'):
            self.admit(row, receipt)

    def test_changed_bindings_sql_results_and_measured_receipts_are_rejected(self):
        mutations = [
            lambda r, e: e['results'][0]['result']['data'].pop(),
            lambda r, e: e['results'].reverse(),
            lambda r, e: e['results'][0].update(operation_id='query-1'),
            lambda r, e: e['results'][0].update(query_id=999),
            lambda r, e: e['results'][0].update(node_id='wrong-job'),
            lambda r, e: e['results'][0].update(results_key='wrong-cache-key'),
            lambda r, e: e['results'][0].update(argument_sha256='0' * 64),
            lambda r, e: e['results'][0].update(sql_sha256='0' * 64),
            lambda r, e: e['results'][0].update(http_status=500),
            lambda r, e: e['results'][0].update(retrieved_timestamp_ns=2001),
            lambda r, e: e['results'][0].update(retrieved_timestamp_ns=True),
            lambda r, e: e['results'][0]['result'].update(status='failed'),
            lambda r, e: e['results'][0]['result']['data'].reverse(),
            lambda r, e: e['results'][0]['result']['data'][0].update(category=False),
            lambda r, e: e['results'][0]['result']['columns'][0].update(name='wrong'),
            lambda r, e: e['results'][0]['result']['columns'][0].update(type='TEXT'),
            lambda r, e: e['fixture'].update(rows=100),
            lambda r, e: e['fixture'].update(ordered_input_sha256='0' * 64),
            lambda r, e: r['validation'].update(output_digest='0' * 64),
            lambda r, e: r['business_bindings'][0].update(output_sha256='0' * 64),
            lambda r, e: r['validation'].update(rows_per_query=1),
            lambda r, e: r['business_bindings'].pop(),
            lambda r, e: r.update(warmup_operations=['warmup:0', 'query-0']),
        ]
        for index, mutation in enumerate(mutations):
            with self.subTest(mutation=index):
                row, receipt = self.fixture()
                mutation(row, receipt)
                with self.assertRaises(ValueError):
                    self.admit(row, receipt)

    def test_ci_package_combine_and_publication_preserve_sql_receipts(self):
        from benchmarks.ci_results import combine, package, publication_evidence
        from benchmarks.tests.test_ci_results import MatrixResultsTests
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            incoming, registry = MatrixResultsTests().fixtures(root)
            receipt = incoming / 'first/private/sql-results-evidence.json'
            receipt.parent.mkdir()
            receipt.write_text('{"observed_native_results": []}')
            artifact = root / 'artifact'
            package(incoming / 'first', artifact, 'first')
            self.assertEqual((artifact / 'private/sql-results-evidence.json').read_bytes(), receipt.read_bytes())
            output = root / 'results'
            combine(incoming, output, run_id='shared', commit='revision', registry=registry)
            copied = output / 'private/sql-results-evidence.json'
            self.assertEqual(copied.read_bytes(), receipt.read_bytes())
            self.assertIn(copied.absolute(), publication_evidence(output))

    def test_receipt_path_and_file_checksum_are_enforced(self):
        row, receipt = self.fixture()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'sql-results-evidence.json'
            path.write_text(json.dumps(receipt))
            for location in ('../sql-results-evidence.json', str(path), 'sample.json'):
                row['sql_result_evidence'] = {'schema_version': 1, 'path': location, 'sha256': '0' * 64}
                with self.assertRaises(ValueError):
                    validate_superset_output(row, directory, 100)
            row['sql_result_evidence']['path'] = path.name
            with self.assertRaisesRegex(ValueError, 'checksum'):
                validate_superset_output(row, directory, 100)


if __name__ == '__main__':
    unittest.main()
