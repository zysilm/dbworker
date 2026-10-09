"""Public inputs and effect receipts must bind to actual execution evidence."""
import copy
import hashlib
import json
import unittest

from benchmarks.common.argument_evidence import argument_digest
from benchmarks.common.business_evidence import (
    POSTHOG_API_SHA256, expected_operations, validate_business_binding,
)
from benchmarks.common.workflow_graph import WorkflowMismatch


def sha(value):
    return hashlib.sha256(str(value).encode()).hexdigest()


class BusinessEvidenceTests(unittest.TestCase):
    def fixture(self, suite, count=2):
        measured, warmups = expected_operations(suite, count)
        operations = warmups + measured
        row = {'warmup_operations': warmups, 'configuration': {}}
        events = []
        stage = {'superset': 'sql_lab', 'saleor': 'export', 'paperless_ngx': 'ingestion',
                 'posthog': 'notification', 'sentry': 'delivery'}[suite]
        bindings, identities, outcomes = [], [], []
        public_input = {'product_ids': ['21', '22'], 'fields': ['name', 'product type', 'variant sku'], 'file_type': 'csv'}
        for index, op in enumerate(operations):
            node = f'root-{index}'
            details = {'argument_sha256': sha(index)}
            if suite == 'superset':
                marker = index - 2 if index >= 2 else index + 10000
                sql = f'SELECT category, SUM(value) + {marker} AS total FROM facts GROUP BY category ORDER BY category'
                details['business_input'] = {'query_id': index + 1, 'sql_sha256': sha(sql)}
                if index >= 2:
                    bindings.append({'operation_id': op, 'stage': stage, 'query_id': index + 1,
                        'sql_sha256': sha(sql), 'username': 'benchmark', 'node_id': node,
                        'argument_sha256': details['argument_sha256'], 'results_key': f'result-{index}',
                        'output_sha256': sha('output-' + str(index))})
            elif suite == 'saleor':
                details['argument_sha256'] = argument_digest((index + 1, {'ids': public_input['product_ids']},
                    {'fields': public_input['fields']}, 'csv'), {})
                bindings.append({'operation_id': op, 'export_file_id': index + 1,
                    'user_email': f'export-{index}@example.test', 'root_node_id': node,
                    'root_argument_sha256': details['argument_sha256']})
            elif suite == 'paperless_ngx':
                details['business_input'] = {'input_sha256': sha('file-' + str(index))}
                if index >= 2:
                    bindings.append({'operation_id': op, 'task_id': node,
                        'input_sha256': details['business_input']['input_sha256'],
                        'argument_sha256': details['argument_sha256']})
            elif suite == 'posthog':
                identity = {'operation_id': op, 'user_id': index + 1,
                    'recipient_sha256': sha(f'recipient-{index:04d}@benchmark.invalid')}
                identities.append({**identity, 'sha256': sha(json.dumps(identity, sort_keys=True))})
                details['argument_sha256'] = argument_digest((index + 1,), {})
                outcomes.append({'operation_id': op, 'status_code': 200, 'response': {'success': True},
                    'effects': {'user_id': index + 1, 'totp_device_id': index + 11,
                    'totp_verified': True, 'session_verified': True, 'otp_device_matches': True,
                    'setup_cache_and_session_keys_removed': True, 'other_session_revoked': True,
                    'remaining_sessions': 1}})
            for phase in ('submitted', 'started', 'succeeded'):
                events.append({'operation_id': op, 'node_id': node, 'parent_id': None,
                               'stage': stage, 'event': phase, 'details': copy.deepcopy(details)})
        if suite == 'superset':
            row.update(business_bindings=bindings)
            row['configuration']['submission'] = 'authenticated original SQL Lab REST API'
        elif suite == 'saleor':
            row['operation_binding'] = {'producer': 'saleor.graphql.csv.mutations.export_products.ExportProducts',
                'entrypoint': 'authenticated POST /graphql/ exportProducts', 'bindings': bindings,
                'public_input': public_input}
            row['dataset'] = {'products': 2}
        elif suite == 'paperless_ngx':
            row['submitted_inputs'] = bindings
            row['configuration']['submission'] = 'authenticated POST /api/documents/post_document/ via native APIClient routing'
        elif suite == 'posthog':
            api = 'posthog.api.user.UserViewSet.two_factor_validate'
            row.update(producer_identities=identities, producer_api_outcomes=outcomes,
                producer_execution={'passed': True, 'api': api, 'source_file': 'posthog/api/user.py',
                    'sha256': POSTHOG_API_SHA256, 'measured_calls': count, 'warmup_calls': 2,
                    'root_jobs_per_call': 1, 'delivery_jobs_per_call': 1,
                    'effects': ['verified_totp_device', 'persistent_session_flags', 'setup_cache_cleanup', 'other_session_revocation']})
            row['configuration'] = {'producer_api': api, 'producer_api_timed': True,
                                     'producer_api_effects_validated': True}
        return row, events

    def test_all_suite_fixtures(self):
        for suite in ('superset', 'saleor', 'paperless_ngx', 'posthog', 'sentry'):
            with self.subTest(suite=suite):
                validate_business_binding(*self.arguments(suite))

    def arguments(self, suite):
        row, events = self.fixture(suite)
        return row, suite, events, 2

    def test_known_operation_identities_not_result_graph(self):
        for suite in ('superset', 'saleor', 'paperless_ngx', 'posthog', 'sentry'):
            row, events = self.fixture(suite)
            for event in events:
                if event['operation_id'] == expected_operations(suite, 2)[0][0]:
                    event['operation_id'] = 'substituted-operation'
            with self.subTest(suite=suite), self.assertRaises(WorkflowMismatch):
                validate_business_binding(row, suite, events, 2)

    def test_unknown_warmup_and_invalid_count_or_evidence_types(self):
        row, events = self.fixture('sentry')
        row['warmup_operations'] = ['warmup:-2', 'warmup:999']
        with self.assertRaises(WorkflowMismatch):
            validate_business_binding(row, 'sentry', events, 2)
        for count in (True, 0, '2', None):
            with self.subTest(count=count), self.assertRaises(WorkflowMismatch):
                validate_business_binding({}, 'posthog', [], count)
        for events in ([None], [{'operation_id': []}]):
            with self.assertRaises(WorkflowMismatch):
                validate_business_binding({}, 'posthog', events, 2)

    def test_api_shortcuts_rejected(self):
        for suite in ('superset', 'paperless_ngx', 'posthog', 'saleor'):
            row, events = self.fixture(suite)
            if suite == 'saleor':
                row['operation_binding']['entrypoint'] = 'export_products_task.delay'
            elif suite == 'posthog':
                row['configuration']['producer_api'] = 'send_two_factor_auth_enabled_email.delay'
            else:
                row['configuration']['submission'] = 'direct original Task.delay'
            with self.subTest(suite=suite), self.assertRaises(WorkflowMismatch):
                validate_business_binding(row, suite, events, 2)

    def test_receipt_omission_duplicate_and_root_substitution(self):
        keys = {'superset': 'business_bindings', 'paperless_ngx': 'submitted_inputs', 'posthog': 'producer_identities'}
        for suite, key in keys.items():
            for mutation in ('missing', 'duplicate', 'wrong_root'):
                row, events = self.fixture(suite)
                if mutation == 'missing':
                    row[key].pop()
                elif mutation == 'duplicate':
                    row[key][-1] = copy.deepcopy(row[key][0])
                else:
                    event = next(event for event in events if event['operation_id'] == expected_operations(suite, 2)[0][0] and event['event'] == 'started')
                    event['details']['argument_sha256'] = sha('substituted')
                with self.subTest(suite=suite, mutation=mutation), self.assertRaises(WorkflowMismatch):
                    validate_business_binding(row, suite, events, 2)

    def test_superset_actual_sql_and_query_identity(self):
        for mutation in ('query', 'sql', 'omitted_semantics', 'same_results'):
            row, events = self.fixture('superset')
            binding = row['business_bindings'][0]
            if mutation == 'query':
                binding['query_id'] += 500
            elif mutation == 'sql':
                binding['sql_sha256'] = sha('different SQL')
            elif mutation == 'omitted_semantics':
                next(event for event in events if event['operation_id'] == 'query-0')['details'].pop('business_input')
            else:
                row['business_bindings'][1]['results_key'] = binding['results_key']
            with self.subTest(mutation=mutation), self.assertRaises(WorkflowMismatch):
                validate_business_binding(row, 'superset', events, 2)

    def test_paperless_actual_bytes_and_api_returned_task(self):
        for key in ('input_sha256', 'task_id', 'argument_sha256'):
            row, events = self.fixture('paperless_ngx')
            row['submitted_inputs'][0][key] = sha('substituted')
            with self.subTest(key=key), self.assertRaises(WorkflowMismatch):
                validate_business_binding(row, 'paperless_ngx', events, 2)

    def test_saleor_export_id_independently_reconstructs_arguments(self):
        for mutation in ('export', 'recipient', 'products', 'bool_id'):
            row, events = self.fixture('saleor')
            binding = row['operation_binding']['bindings'][0]
            if mutation == 'products':
                row['operation_binding']['public_input']['product_ids'] = ['90', '91']
            else:
                binding[{'export': 'export_file_id', 'recipient': 'user_email', 'bool_id': 'export_file_id'}[mutation]] = {
                    'export': 900, 'recipient': 'unrelated@example.test', 'bool_id': True}[mutation]
            with self.subTest(mutation=mutation), self.assertRaises(WorkflowMismatch):
                validate_business_binding(row, 'saleor', events, 2)

    def test_posthog_source_counts_receipts_and_actual_effects(self):
        for mutation in ('source', 'warmup_count', 'missing_outcome', 'status_bool', 'false_effect', 'remaining_sessions', 'device_reuse', 'identity_hash', 'recipient'):
            row, events = self.fixture('posthog')
            outcome = row['producer_api_outcomes'][0]
            if mutation == 'source':
                row['producer_execution']['sha256'] = sha('other source')
            elif mutation == 'warmup_count':
                row['producer_execution']['warmup_calls'] = True
            elif mutation == 'missing_outcome':
                row['producer_api_outcomes'].pop()
            elif mutation == 'status_bool':
                outcome['status_code'] = True
            elif mutation == 'false_effect':
                outcome['effects']['session_verified'] = False
            elif mutation == 'remaining_sessions':
                outcome['effects']['remaining_sessions'] = True
            elif mutation == 'device_reuse':
                row['producer_api_outcomes'][1]['effects']['totp_device_id'] = outcome['effects']['totp_device_id']
            elif mutation == 'identity_hash':
                row['producer_identities'][0]['sha256'] = sha('other user')
            else:
                row['producer_identities'][0]['recipient_sha256'] = sha('other@example.test')
            with self.subTest(mutation=mutation), self.assertRaises(WorkflowMismatch):
                validate_business_binding(row, 'posthog', events, 2)


if __name__ == '__main__':
    unittest.main()
