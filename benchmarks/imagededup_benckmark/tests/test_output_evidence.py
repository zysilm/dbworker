"""Independent replay of public image outputs, including adversarial receipts."""
import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from imagededup_benckmark import output_evidence, run


class OutputEvidenceTests(unittest.TestCase):
    def fixture(self, count=3):
        ids = list(range(1, count + 1))
        manifest = {'schema_version': 1, 'workspace': 1, 'top_k': 10, 'max_distance': 10,
            'artifacts': [{'artifact_id': identity, 'hash_value': '0000000000000000'} for identity in ids],
            'comparisons': [{'request_id': count + query, 'query_artifact_id': query,
                'results': [{'candidate_artifact_id': candidate, 'distance': 0}
                            for candidate in ids if candidate != query][:10]}
                for query in ids]}
        trace = {'workspace': 1, 'artifact_ids': ids, 'request_ids': [count + query for query in ids]}
        base = output_evidence.replay(manifest, trace, count, 'mixed')
        row = {'scenario': 'mixed', 'operation_trace': trace,
            'validation': {**base, 'output_digest': hashlib.sha256(json.dumps(base, sort_keys=True).encode()).hexdigest()}}
        return manifest, row

    def save(self, root, manifest, row):
        path = root / 'output-evidence.json'
        path.write_text(json.dumps(manifest))
        row['output_evidence'] = {'path': path.name, 'schema_version': 1,
            'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}

    def test_all_thousand_outputs_replayed_and_native_id_tie_break(self):
        manifest, row = self.fixture(1000)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.save(root, manifest, row)
            self.assertEqual(output_evidence.validate(row, root, 1000), row['validation'])
            manifest['comparisons'][0]['results'].reverse()
            self.save(root, manifest, row)
            with self.assertRaisesRegex(ValueError, 'Hamming oracle'):
                output_evidence.validate(row, root, 1000)

    def test_nonzero_hamming_threshold_and_database_id_tie_break(self):
        identities = [7, 3, 9, 10]
        manifest = {'schema_version': 1, 'workspace': 1, 'top_k': 10, 'max_distance': 10,
            'artifacts': [{'artifact_id': identity, 'hash_value': value} for identity, value in
                          zip(identities, ('0000000000000000', '0000000000000001',
                                           '0000000000000003', 'ffffffffffffffff'), strict=True)],
            'comparisons': [{'request_id': index + 11, 'query_artifact_id': identity,
                'results': [{'candidate_artifact_id': candidate, 'distance': distance}
                            for candidate, distance in values]}
                for index, (identity, values) in enumerate(zip(identities,
                    ([(3, 1), (9, 2)], [(7, 1), (9, 1)], [(3, 1), (7, 2)], []), strict=True))]}
        trace = {'workspace': 1, 'artifact_ids': identities, 'request_ids': [11, 12, 13, 14]}
        result = output_evidence.replay(manifest, trace, 4, 'comparison')
        self.assertEqual(result['top_k_digests'][1],
            hashlib.sha256(json.dumps([(0, 1), (2, 1)]).encode()).hexdigest())
        self.assertEqual(result['top_k_digests'][3], hashlib.sha256(b'[]').hexdigest())

    def test_self_reports_cannot_override_observed_outputs(self):
        for key in ('hashes_digest', 'top_k_digests', 'output_digest', 'artifacts', 'requests', 'scored_pairs', 'passed'):
            with self.subTest(key=key), tempfile.TemporaryDirectory() as temporary:
                manifest, row = self.fixture()
                root = Path(temporary)
                self.save(root, manifest, row)
                row['validation'][key] = 'changed'
                with self.assertRaisesRegex(ValueError, 'Reported image output'):
                    output_evidence.validate(row, root, 3)

    def test_forged_receipts_even_with_updated_checksum_and_digest_reject(self):
        for mutation in ('missing_hash', 'duplicate_hash', 'wrong_order', 'bad_hash', 'missing_request',
                         'wrong_query', 'wrong_request', 'duplicate_request', 'distance', 'missing_top_k',
                         'duplicate_result', 'extra_result', 'self_candidate', 'bool_distance', 'bool_id',
                         'top_k', 'max_distance', 'workspace', 'version'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                manifest, row = self.fixture()
                if mutation == 'missing_hash': manifest['artifacts'].pop()
                elif mutation == 'duplicate_hash': manifest['artifacts'][1] = manifest['artifacts'][0]
                elif mutation == 'wrong_order': manifest['artifacts'].reverse()
                elif mutation == 'bad_hash': manifest['artifacts'][0]['hash_value'] = '-1'
                elif mutation == 'missing_request': manifest['comparisons'].pop()
                elif mutation == 'wrong_query': manifest['comparisons'][0]['query_artifact_id'] = 2
                elif mutation == 'wrong_request': manifest['comparisons'][0]['request_id'] = 999
                elif mutation == 'duplicate_request': manifest['comparisons'][1]['request_id'] = 4
                elif mutation == 'distance': manifest['comparisons'][0]['results'][0]['distance'] = 1
                elif mutation == 'missing_top_k': manifest['comparisons'][0]['results'].pop()
                elif mutation == 'duplicate_result': manifest['comparisons'][0]['results'][1] = manifest['comparisons'][0]['results'][0]
                elif mutation == 'extra_result': manifest['comparisons'][0]['results'].append({'candidate_artifact_id': 2, 'distance': 0})
                elif mutation == 'self_candidate': manifest['comparisons'][0]['results'][0]['candidate_artifact_id'] = 1
                elif mutation == 'bool_distance': manifest['comparisons'][0]['results'][0]['distance'] = False
                elif mutation == 'bool_id': manifest['artifacts'][0]['artifact_id'] = True
                elif mutation == 'version': manifest['schema_version'] = True
                else: manifest[mutation] = 2
                root = Path(temporary)
                self.save(root, manifest, row)
                with self.assertRaises(ValueError):
                    output_evidence.validate(row, root, 3)

    def test_path_and_checksum_are_bound(self):
        for mutation in ('missing', 'checksum', 'traversal', 'absolute', 'symlink', 'version'):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                manifest, row = self.fixture()
                root = Path(temporary)
                self.save(root, manifest, row)
                if mutation == 'missing': row.pop('output_evidence')
                elif mutation == 'checksum': row['output_evidence']['sha256'] = '0' * 64
                elif mutation == 'traversal': row['output_evidence']['path'] = '../output-evidence.json'
                elif mutation == 'absolute': row['output_evidence']['path'] = str(root / 'output-evidence.json')
                elif mutation == 'version': row['output_evidence']['schema_version'] = True
                else:
                    (root / 'linked').symlink_to(root, target_is_directory=True)
                    row['output_evidence']['path'] = 'linked/output-evidence.json'
                with self.assertRaises(ValueError):
                    output_evidence.validate(row, root, 3)

    def test_build_has_complete_hashes_without_comparison_rows(self):
        manifest, row = self.fixture()
        manifest['comparisons'] = []
        row['operation_trace']['request_ids'] = []
        result = output_evidence.replay(manifest, row['operation_trace'], 3, 'build')
        self.assertEqual((result['requests'], result['scored_pairs'], result['top_k_digests']), (0, 0, []))

    def test_runtime_retains_actual_sql_hashes_and_public_http_rows(self):
        manifest, row = self.fixture()
        with tempfile.TemporaryDirectory() as temporary:
            database = Path(temporary) / 'test.db'
            with sqlite3.connect(database) as connection:
                connection.executescript('CREATE TABLE feature_artifact(id INTEGER,hash_value TEXT,workspace_id INTEGER);'
                    'CREATE TABLE scored_candidate(request_id INTEGER);')
                connection.executemany('INSERT INTO feature_artifact VALUES (?, ?, 1)',
                    [(item['artifact_id'], item['hash_value']) for item in manifest['artifacts']])
                connection.executemany('INSERT INTO scored_candidate VALUES (?)', [(key,) for key in (4, 4, 5, 5, 6, 6)])
            def request(method, path):
                if '/artifacts?' in path:
                    return [{'execution_status': 'finished', 'error': None}] * 3
                identity = int(path.split('/')[2])
                if path.endswith('/results'):
                    return copy.deepcopy(manifest['comparisons'][identity - 4]['results'])
                return {'execution_status': 'finished', 'candidates_scored_count': 2}
            observed = {}
            actual = run.validate(SimpleNamespace(database=database, request=request), 1, [1, 2, 3], [4, 5, 6],
                top_k=10, max_distance=10, output_manifest=observed)
            self.assertEqual(observed, manifest)
            self.assertEqual(actual, output_evidence.replay(observed, row['operation_trace'], 3, 'mixed'))


if __name__ == '__main__':
    unittest.main()
