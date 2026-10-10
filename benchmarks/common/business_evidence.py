"""Replay public-producer inputs against independently observed business jobs.

This successful-work contract does not establish fault or retry equivalence.
Opaque fingerprints alone cannot establish a file or SQL input's semantics.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from benchmarks.common.argument_evidence import argument_digest
from benchmarks.common.workflow_graph import WorkflowMismatch

POSTHOG_API_SHA256 = '4b7b7b5c24d8a5fa5014672060d3b0c1b66b20773e0fa0f88178b38af85579a9'


def _require(condition, message):
    if not condition:
        raise WorkflowMismatch('Business binding: ' + message)


def _digest(value):
    _require(isinstance(value, str) and re.fullmatch('[0-9a-f]{64}', value) is not None,
             'missing or malformed fingerprint')
    return value


def _positive(value):
    _require(type(value) is int and value > 0, 'invalid persistent business identity')
    return value


def trusted_warmup_requests(suite, expected_profile=None):
    """Accept diagnostic warmup only from the caller's trusted profile."""
    count = (expected_profile or {}).get('warmup_requests', 2)
    _require(type(count) is int and count in (2, 8), 'invalid trusted warmup count')
    _require(suite == 'posthog' or count == 2, 'diagnostic warmup is PostHog only')
    return count


def expected_operations(suite, count, *, expected_profile=None):
    """Fixture identities are a trusted contract, never inferred from results."""
    _require(type(count) is int and count > 0, 'invalid request count')
    warmup_count = trusted_warmup_requests(suite, expected_profile)
    if suite == 'superset':
        return [f'query-{i}' for i in range(count)], ['warmup:0', 'warmup:1']
    if suite == 'posthog':
        return [f'notification-{i:04d}' for i in range(count)], [f'warmup:notification-{i:04d}' for i in range(warmup_count)]
    if suite in ('saleor', 'paperless_ngx'):
        return [str(i) for i in range(count)], ['warmup:0', 'warmup:1']
    if suite == 'sentry':
        return [str(i) for i in range(count)], ['warmup:-2', 'warmup:-1']
    raise WorkflowMismatch('Business binding: unsupported suite')


def _indexed(values, operations):
    _require(isinstance(values, list) and len(values) == len(operations), 'missing or duplicate input receipts')
    _require(all(isinstance(value, dict) for value in values), 'invalid input receipt')
    _require(all(isinstance(value.get('operation_id'), str) for value in values), 'invalid receipt operation identity')
    indexed = {value.get('operation_id'): value for value in values}
    _require(len(indexed) == len(values) and set(indexed) == set(operations), 'input receipt operation identities differ')
    return indexed


def _roots(events, stage, operations):
    indexed = {}
    for event in events:
        if event.get('stage') == stage and event.get('event') in ('submitted', 'started'):
            indexed.setdefault((event.get('operation_id'), event['event']), []).append(event)
    roots = {}
    for op in operations:
        phases = {}
        for phase in ('submitted', 'started'):
            rows = indexed.get((op, phase), [])
            _require(len(rows) == 1, 'missing or duplicate root publication/execution')
            event = rows[0]
            _require(event.get('parent_id') is None and isinstance(event.get('details'), dict), 'invalid root task')
            _require(isinstance(event.get('node_id'), str) and bool(event['node_id']), 'missing root identity')
            _digest(event['details'].get('argument_sha256'))
            phases[phase] = event
        _require(phases['submitted']['node_id'] == phases['started']['node_id'], 'root identity changed')
        _require(phases['submitted']['details']['argument_sha256'] == phases['started']['details']['argument_sha256'],
                 'root input changed between publication and execution')
        roots[op] = phases
    _require(len({value['submitted']['node_id'] for value in roots.values()}) == len(operations), 'root reused across operations')
    return roots


def _unique(values, label):
    _require(len(set(values)) == len(values), label + ' reused across operations')


def _trusted_profile(suite, expected_requests, expected_profile):
    if expected_profile is None:
        registry = json.loads((Path(__file__).resolve().parents[1] / 'registry.json').read_text())
        declaration = next(item for item in registry['suites'] if item['suite_id'] == suite)
        expected_profile = declaration['profiles']['full']
    _require(isinstance(expected_profile, dict), 'missing trusted workload profile')
    _require(type(expected_profile.get('requests')) is int
             and expected_profile['requests'] == expected_requests, 'trusted request count differs')
    return expected_profile


def _counts(values, expected):
    _require(isinstance(values, dict), 'missing business workload counts')
    for name, count in expected.items():
        _require(type(values.get(name)) is int and values[name] == count,
                 'trusted business workload count differs: ' + name)


def validate_posthog_delivery_records(users, records, email_hash):
    """Validate the native ledger in linear time without changing its oracle."""
    if len(records) != len(users) or any(record.sent_at is None for record in records):
        raise AssertionError("Native delivery ledger is incomplete")
    by_email_hash = {}
    for record in records:
        by_email_hash.setdefault(record.email_hash, []).append(record)
    for user in users:
        matching = by_email_hash.get(email_hash(user.email), [])
        if len(matching) != 1 or not matching[0].campaign_key.startswith(f"2fa_enabled_{user.uuid}-"):
            raise AssertionError("Native user/campaign mapping differs")


def validate_business_binding(row, suite, events, expected_requests, *, expected_profile=None):
    """Reject missing, substituted or shortcut producer evidence before scoring."""
    _require(isinstance(row, dict) and isinstance(events, list) and all(isinstance(event, dict) and isinstance(event.get('operation_id'), str) for event in events), 'invalid evidence types')
    measured, warmups = expected_operations(suite, expected_requests, expected_profile=expected_profile)
    all_ops = warmups + measured
    _require(row.get('warmup_operations') == warmups, 'warmup fixture identities differ')
    _require({event.get('operation_id') for event in events} == set(all_ops), 'observed fixture identities differ')
    configuration = row.get('configuration')
    _require(isinstance(configuration, dict), 'missing public API configuration')
    if suite == 'sentry':
        # MIME/content admission is implemented by the independent SMTP oracle.
        return
    if suite == 'superset':
        receipt = row.get('sql_result_evidence')
        _require(isinstance(receipt, dict) and set(receipt) == {'schema_version', 'path', 'sha256'}
                 and type(receipt['schema_version']) is int and receipt['schema_version'] == 1
                 and isinstance(receipt['path'], str) and bool(receipt['path'])
                 and not Path(receipt['path']).is_absolute() and '..' not in Path(receipt['path']).parts
                 and Path(receipt['path']).name == 'sql-results-evidence.json',
                 'missing independently replayable SQL output receipt')
        _digest(receipt['sha256'])
        _digest(row.get('validation', {}).get('output_digest'))
        _counts(row.get('validation'), {'queries': expected_requests, 'retrieved_results': expected_requests, 'rows_per_query': 10})
        _require(configuration.get('submission') == 'authenticated original SQL Lab REST API', 'SQL Lab public API shortcut')
        bindings = _indexed(row.get('business_bindings'), measured)
        roots = _roots(events, 'sql_lab', measured)
        for op, binding in bindings.items():
            query_id = _positive(binding.get('query_id'))
            marker = int(op.split('-')[-1])
            sql = f'SELECT category, SUM(value) + {marker} AS total FROM facts GROUP BY category ORDER BY category'
            sql_sha = hashlib.sha256(sql.encode()).hexdigest()
            _require(binding.get('stage') == 'sql_lab' and binding.get('username') == 'benchmark', 'SQL Lab request context differs')
            _require(binding.get('sql_sha256') == sql_sha, 'SQL fixture differs')
            _require(binding.get('node_id') == roots[op]['submitted']['node_id'], 'query task binding differs')
            _require(binding.get('argument_sha256') == roots[op]['submitted']['details']['argument_sha256'], 'query argument binding differs')
            for event in roots[op].values():
                _require(event['details'].get('business_input') == {'query_id': query_id, 'sql_sha256': sql_sha},
                         'observed query identity or rendered SQL differs')
            _require(isinstance(binding.get('results_key'), str) and bool(binding['results_key']), 'missing stored query result')
            _digest(binding.get('output_sha256'))
        for key in ('query_id', 'node_id', 'sql_sha256', 'results_key', 'output_sha256'):
            _unique([value[key] for value in bindings.values()], 'SQL Lab ' + key)
        return
    if suite == 'saleor':
        profile = _trusted_profile(suite, expected_requests, expected_profile)
        product_count = profile.get('products')
        _require(type(product_count) is int and product_count > 0, 'missing trusted product count')
        _counts(row.get('dataset'), {'products': product_count, 'variants': product_count, 'exports': expected_requests})
        _counts(row.get('validation'), {'products_per_export': product_count, 'exports': expected_requests,
                                      'email_messages': expected_requests, 'business_jobs': expected_requests * 2})
        evidence = row.get('operation_binding')
        _require(isinstance(evidence, dict) and evidence.get('producer') == 'saleor.graphql.csv.mutations.export_products.ExportProducts'
                 and evidence.get('entrypoint') == 'authenticated POST /graphql/ exportProducts', 'Saleor GraphQL producer shortcut')
        public_input = evidence.get('public_input')
        _require(isinstance(public_input, dict), 'missing normalized GraphQL public input')
        products = public_input.get('product_ids')
        _require(isinstance(products, list) and len(products) == product_count and all(isinstance(value, str) and value.isdecimal() and int(value) > 0 for value in products), 'invalid exported product identities')
        _unique(products, 'exported product')
        _require(public_input.get('fields') == ['name', 'product type', 'variant sku'] and public_input.get('file_type') == 'csv', 'GraphQL field or format input differs')
        _require(isinstance(row.get('dataset'), dict) and type(row['dataset'].get('products')) is int and row['dataset']['products'] == len(products), 'export product count differs')
        bindings = _indexed(evidence.get('bindings'), all_ops)
        roots = _roots(events, 'export', all_ops)
        for index, op in enumerate(all_ops):
            binding = bindings[op]
            export_id = _positive(binding.get('export_file_id'))
            expected_digest = argument_digest((export_id, {'ids': products}, {'fields': public_input['fields']}, 'csv'), {})
            _require(binding.get('root_argument_sha256') == expected_digest, 'returned export ID or public GraphQL input differs')
            _require(binding.get('user_email') == f'export-{index}@example.test', 'export authenticated recipient differs')
            _require(binding.get('root_node_id') == roots[op]['submitted']['node_id'], 'export task binding differs')
            _require(_digest(binding.get('root_argument_sha256')) == roots[op]['submitted']['details']['argument_sha256'],
                     'normalized export publication differs')
        for key in ('export_file_id', 'user_email', 'root_node_id', 'root_argument_sha256'):
            _unique([value[key] for value in bindings.values()], 'export ' + key)
        return
    if suite == 'paperless_ngx':
        _require(configuration.get('submission') == 'authenticated POST /api/documents/post_document/ via native APIClient routing',
                 'Paperless upload API shortcut')
        bindings = _indexed(row.get('submitted_inputs'), measured)
        roots = _roots(events, 'ingestion', measured)
        for op, binding in bindings.items():
            input_sha = _digest(binding.get('input_sha256'))
            _require(binding.get('task_id') == roots[op]['submitted']['node_id'], 'API-returned task identity differs')
            _require(binding.get('argument_sha256') == roots[op]['submitted']['details']['argument_sha256'], 'upload argument binding differs')
            for event in roots[op].values():
                _require(event['details'].get('business_input') == {'input_sha256': input_sha}, 'observed upload bytes differ')
        for key in ('task_id', 'input_sha256'):
            _unique([value[key] for value in bindings.values()], 'upload ' + key)
        return
    _require(configuration.get('producer_api') == 'posthog.api.user.UserViewSet.two_factor_validate'
             and configuration.get('producer_api_timed') is True and configuration.get('producer_api_effects_validated') is True,
             'PostHog API shortcut or untimed producer')
    if len(warmups) == 8:
        _require(row.get('validation', {}).get('warmup_messages') == 8
                 and type(row.get('validation', {}).get('warmup_messages')) is int
                 and row.get('dataset', {}).get('warmup_requests') == 8
                 and type(row.get('dataset', {}).get('warmup_requests')) is int,
                 'diagnostic warmup result counts differ')
    execution = row.get('producer_execution')
    _require(isinstance(execution, dict) and execution.get('passed') is True
             and execution.get('api') == configuration['producer_api']
             and execution.get('source_file') == 'posthog/api/user.py' and execution.get('sha256') == POSTHOG_API_SHA256,
             'original 2FA API source differs')
    for field, expected in (('measured_calls', expected_requests), ('warmup_calls', len(warmups)), ('root_jobs_per_call', 1), ('delivery_jobs_per_call', 1)):
        _require(type(execution.get(field)) is int and execution[field] == expected, 'API call/job counts differ')
    _require(execution.get('effects') == ['verified_totp_device', 'persistent_session_flags', 'setup_cache_cleanup', 'other_session_revocation'], 'API effects contract differs')
    bindings = _indexed(row.get('producer_identities'), all_ops)
    outcomes = _indexed(row.get('producer_api_outcomes'), all_ops)
    roots = _roots(events, 'notification', all_ops)
    devices = []
    for index, op in enumerate(all_ops):
        binding, outcome = bindings[op], outcomes[op]
        user_id = _positive(binding.get('user_id'))
        if index < len(warmups):
            fixture_index = index if index < 2 else expected_requests + index
        else:
            fixture_index = index - len(warmups) + 2
        recipient_sha = hashlib.sha256(f'recipient-{fixture_index:04d}@benchmark.invalid'.encode()).hexdigest()
        identity = {'operation_id': op, 'user_id': user_id, 'recipient_sha256': recipient_sha}
        _require(binding == {**identity, 'sha256': hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()}, 'user/recipient receipt differs')
        _require(roots[op]['submitted']['details']['argument_sha256'] == argument_digest((user_id,), {}), 'notification user input differs')
        _require(type(outcome.get('status_code')) is int and outcome['status_code'] == 200
                 and outcome.get('response') == {'success': True}, 'original API response differs')
        effects = outcome.get('effects')
        _require(isinstance(effects, dict) and type(effects.get('user_id')) is int and effects['user_id'] == user_id, 'API effects user differs')
        for name in ('totp_verified', 'session_verified', 'otp_device_matches', 'setup_cache_and_session_keys_removed', 'other_session_revoked'):
            _require(effects.get(name) is True, 'missing actual API effect: ' + name)
        _require(type(effects.get('remaining_sessions')) is int and effects['remaining_sessions'] == 1, 'API session revocation differs')
        devices.append(_positive(effects.get('totp_device_id')))
    _unique([value['user_id'] for value in bindings.values()], '2FA user')
    _unique(devices, 'TOTP device')
