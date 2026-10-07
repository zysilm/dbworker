"""Require native source evidence and replayable business-work parity for results."""
from __future__ import annotations

import hashlib
import ast
import json
import importlib.util
from collections import Counter
from pathlib import Path

from benchmarks.common.native_admission import APPLICATIONS, NativeAdmissionError
from benchmarks.common.workflow_graph import WorkflowMismatch, compare_graphs, read_trace, validate_graph

CONTRACTS = {
    'superset': ({'sql_lab': 1}, []),
    'saleor': ({'export': 1, 'email': 1}, [('export', 'email')]),
    'paperless_ngx': ({'ingestion': 1}, []),
    'posthog': ({'notification': 1, 'delivery': 1}, [('notification', 'delivery')]),
    'sentry': ({'delivery': 2}, []),
}
ROOT = Path(__file__).resolve().parents[2]
TASK_STAGES = {
    'superset': {'sql_lab.get_sql_results': 'sql_lab'},
    'saleor': {'export-products': 'export',
              'saleor.plugins.admin_email.tasks.send_email_with_link_to_download_file_task': 'email'},
    'paperless_ngx': {'documents.tasks.consume_file': 'ingestion'},
    'posthog': {'posthog.tasks.email.send_two_factor_auth_enabled_email': 'notification',
                'posthog.email._send_email': 'delivery'},
    'sentry': {'sentry.tasks.email.send_email': 'delivery', 'sentry.tasks.email.send_email_control': 'delivery'},
    'imagededup': {'images.build': 'build', 'images.compare': 'comparison', 'images.dispatch': 'dispatch'},
}
SCENARIOS = {'superset': {'sql_lab_group_by'}, 'saleor': {'product_csv_export'},
             'paperless_ngx': {'native_unsplit_scan_ingestion'},
             'posthog': {'native_two_factor_notification'}, 'sentry': {'historical_native_email_fanout'},
             'imagededup': {'build', 'comparison', 'mixed'}}


def _safe_relative(root, value):
    if not isinstance(value, str) or not value or Path(value).is_absolute() or '..' in Path(value).parts:
        raise WorkflowMismatch('Missing or unsafe relative evidence artifact')
    root = Path(root).resolve()
    path = (root / value).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise WorkflowMismatch('Missing or unsafe evidence artifact')
    return path


def validate_native_sample(row, suite):
    if row.get('backend') != 'celery':
        return
    evidence = row.get('native_execution', {})
    if evidence.get('passed') is not True or evidence.get('worker_app') != APPLICATIONS[suite]:
        raise NativeAdmissionError(f'{suite}: missing original Celery application admission')
    if evidence.get('check') != 'live_task_origin_and_ast' or not evidence.get('tasks'):
        raise NativeAdmissionError(f'{suite}: missing live native task body origin checks')
    if evidence.get('observer_only') is not True:
        raise NativeAdmissionError(f'{suite}: baseline observation must not replace tasks')
    expected = set(TASK_STAGES[suite])
    if set(evidence['tasks']) != expected or set(evidence.get('task_names', [])) != expected or len(evidence.get('task_names', [])) != len(expected):
        raise NativeAdmissionError(f'{suite}: native task declaration is incomplete or changed')
    source = ROOT / 'examples' / suite
    if suite == 'sentry':
        source /= 'src'
    if suite == 'imagededup':
        source = ROOT / 'examples/imagededup_system_redis_celery/src'
    for name, task in evidence['tasks'].items():
        path = _safe_relative(source, task.get('source'))
        if hashlib.sha256(path.read_bytes()).hexdigest() != task.get('source_sha256'):
            raise NativeAdmissionError(f'{suite}: native source checksum differs: {name}')
        functions = {node.name for node in ast.walk(ast.parse(path.read_text()))
                     if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
        if task.get('function') not in functions:
            raise NativeAdmissionError(f'{suite}: native function evidence absent: {name}')


def replay_graph(row, suite, directory, expected_requests):
    contract_stages, contract_edges = CONTRACTS[suite]
    trace = row.get('workflow_trace', {})
    path = _safe_relative(directory, trace.get('path'))
    if hashlib.sha256(path.read_bytes()).hexdigest() != trace.get('sha256'):
        raise WorkflowMismatch('Workflow trace checksum mismatch')
    graph = row.get('workflow_graph', {})
    operations = graph.get('operations', [])
    if len(operations) != expected_requests or any(not isinstance(op, str) or op.startswith('warmup:') for op in operations):
        raise WorkflowMismatch('Operation count differs from configured workload')
    events = read_trace(path)
    warmups = {event.get('operation_id') for event in events
               if isinstance(event.get('operation_id'), str) and event['operation_id'].startswith('warmup:')}
    if len(warmups) != 2:
        raise WorkflowMismatch('Persisted native trace must contain exactly two complete warmup operations')
    if any(event.get('backend') != row['backend'] for event in events):
        raise WorkflowMismatch('Trace backend differs from sample backend')
    if row['backend'] == 'celery':
        for event in events:
            task_name = event.get('details', {}).get('task_name')
            if TASK_STAGES[suite].get(task_name) != event.get('stage'):
                raise WorkflowMismatch('Observed Celery task name differs from declared native stage')
    replayed = validate_graph(events, operations, contract_stages, contract_edges)
    if replayed != graph:
        raise WorkflowMismatch('Reported workflow differs from replayed events')
    return replayed


def validate_image_workload(row, images):
    evidence = row.get('operation_evidence', {})
    scenario = row['scenario']
    builds = 0 if scenario == 'comparison' else images
    comparisons = 0 if scenario == 'build' else images
    counts = {'submitted_builds': builds, 'completed_builds': builds,
              'submitted_comparisons': comparisons, 'completed_comparisons': comparisons,
              'scored_pairs': comparisons * (images - 1)}
    for key, value in counts.items():
        if type(evidence.get(key)) is not int or evidence[key] != value:
            raise WorkflowMismatch(f'Image work differs from trusted profile: {key}')
    detail = evidence.get('comparisons')
    if not isinstance(detail, list) or len(detail) != comparisons:
        raise WorkflowMismatch('Missing individual image comparison evidence')
    page_items, ordinals = [], []
    positive_pages = [250] * ((images - 1) // 250)
    if (images - 1) % 250:
        positive_pages.append((images - 1) % 250)
    for item in detail:
        widths = item.get('page_sizes')
        if not isinstance(widths, list) or any(type(width) is not int or not 0 <= width <= 250 for width in widths):
            raise WorkflowMismatch('Invalid image scoring page details')
        if sorted(width for width in widths if width) != sorted(positive_pages) or item.get('scored_pairs') != images - 1:
            raise WorkflowMismatch('Image scoring page work was omitted, duplicated or collapsed')
        ordinals.append(item.get('input_ordinal'))
        page_items.extend(widths)
    if any(type(ordinal) is not int for ordinal in ordinals) or sorted(ordinals) != list(range(comparisons)):
        raise WorkflowMismatch('Image comparison identities are incomplete or duplicated')
    if evidence.get('maximum_page_items') != max(page_items, default=0):
        raise WorkflowMismatch('Image maximum page count differs from observed pages')
    if evidence.get('empty_comparison_attempts') != page_items.count(0) or evidence.get('business_attempts') != builds + len(page_items):
        raise WorkflowMismatch('Image business attempt accounting differs from observed work')
    normalized = {'build_inputs': list(range(builds)), 'comparison_inputs': list(range(comparisons)),
                  'scored_pairs': counts['scored_pairs'], 'page_bound': 250}
    digest = hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()
    if evidence.get('workload_digest') != digest:
        raise WorkflowMismatch('Image workload digest does not match trusted workload')
    validation = row['validation']
    for key, value in {'artifacts': images, 'requests': comparisons, 'scored_pairs': counts['scored_pairs']}.items():
        if validation.get(key) != value:
            raise WorkflowMismatch(f'Image output validation omitted configured work: {key}')


def replay_image_workload(row, directory, images):
    trace = row.get('operation_trace', {})
    path = _safe_relative(directory, trace.get('path'))
    if hashlib.sha256(path.read_bytes()).hexdigest() != trace.get('sha256'):
        raise WorkflowMismatch('Image observation checksum mismatch')
    if trace.get('record_offset') != 0 or type(trace.get('record_offset')) is not int or trace.get('page_size') != 250:
        raise WorkflowMismatch('Image observation snapshot scope or page bound changed')
    if type(trace.get('workspace')) is not int or type(trace.get('new_builds')) is not bool:
        raise WorkflowMismatch('Image observation submission identities are invalid')
    if trace['new_builds'] != (row['scenario'] != 'comparison'):
        raise WorkflowMismatch('Image scenario submission mode differs')
    expected_requests = 0 if row['scenario'] == 'build' else images
    for key, count in (('artifact_ids', images), ('request_ids', expected_requests)):
        values = trace.get(key)
        if (not isinstance(values, list) or len(values) != count
                or any(type(value) is not int or value <= 0 for value in values)
                or len(set(values)) != count):
            raise WorkflowMismatch(f'Image submitted source identities differ from profile: {key}')
    records = read_trace(path)
    backend = 'dbwork' if row['backend'] == 'dbworker' else 'celery'
    attempts = {}
    task_ids = []
    for record in records:
        if record.get('backend') != backend:
            raise WorkflowMismatch('Image snapshot backend differs from sample')
        if record.get('stage') == 'dispatch':
            continue  # Control polls are retained separately from business work.
        if record.get('stage') not in ('build', 'comparison') or record.get('workspace_id') != trace['workspace']:
            raise WorkflowMismatch('Unexpected business work in image scenario snapshot')
        identity = record.get('attempt_id')
        if not isinstance(identity, str) or not identity:
            raise WorkflowMismatch('Missing image business attempt identity')
        attempts.setdefault(identity, []).append(record)
    for identity, phases in attempts.items():
        if Counter(record.get('event') for record in phases) != Counter({'started': 1, 'finished': 1}):
            raise WorkflowMismatch(f'Missing or duplicate image business attempt phases: {identity}')
        start = next(record for record in phases if record['event'] == 'started')
        end = next(record for record in phases if record['event'] == 'finished')
        if any(start.get(key) != end.get(key) for key in ('task_id', 'source_id', 'workspace_id', 'stage', 'backend')):
            raise WorkflowMismatch('Image business attempt identity changed')
        if (type(start.get('items')) is not int or type(end.get('items_after')) is not int
                or type(end.get('page_items')) is not int):
            raise WorkflowMismatch('Image observed work counts must be integers')
        if start['stage'] == 'build':
            if end['page_items'] != end['items_after'] - start['items']:
                raise WorkflowMismatch('Image build state differs from its source counters')
        elif (end.get('page_accounting') != 'transaction_committed_orm_inserts'
              or not isinstance(end.get('scored_rows'), list)
              or len(end['scored_rows']) != end['page_items']):
            raise WorkflowMismatch('Image page lacks transaction-attributed committed candidates')
        if backend == 'celery':
            if not isinstance(start.get('task_id'), str) or not start['task_id']:
                raise WorkflowMismatch('Missing original Celery image task identity')
            task_ids.append(start['task_id'])
    if len(set(task_ids)) != len(task_ids):
        raise WorkflowMismatch('Image native business task was delivered more than once')
    verifier_path = ROOT / 'benchmarks/imagededup_benckmark/src/imagededup_benckmark/evidence.py'
    spec = importlib.util.spec_from_file_location('benchmark_image_artifact_verifier', verifier_path)
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    try:
        replayed = verifier.verify(records, workspace=trace['workspace'], artifact_ids=trace['artifact_ids'],
            request_ids=trace['request_ids'], new_builds=trace['new_builds'], page_size=250, record_offset=0)
    except (AssertionError, ValueError, TypeError, KeyError) as error:
        raise WorkflowMismatch(f'Image persisted work evidence is invalid: {error}') from error
    if replayed != row.get('operation_evidence'):
        raise WorkflowMismatch('Reported image work differs from persisted observations')
    validate_image_workload(row, images)
    return replayed


def validate_native_report(report, directory, *, expected_profile=None):
    # Admission is computed here; never preserve a child's asserted verdict.
    report['admission'] = {'passed': False, 'contract': 'successful_business_workflow',
                           'reason': 'native source and persisted business work have not passed replay'}
    if report.get('status') != 'passed':
        return
    suite = report['suite_id']
    if expected_profile is None:
        registry = json.loads((ROOT / 'benchmarks/registry.json').read_text())
        declaration = next(item for item in registry['suites'] if item['suite_id'] == suite)
        expected_profile = declaration['profiles'][report['profile']]
    profile = report['configuration'] if suite == 'imagededup' else report['configuration'].get('profile', {})
    for name, value in expected_profile.items():
        if profile.get(name) != value or type(profile.get(name)) is not type(value):
            raise WorkflowMismatch(f'Reported profile differs from trusted workload: {name}')
    if not report.get('runs'):
        raise WorkflowMismatch('Passed report has no measured runs')
    groups = {}
    for row in report['runs']:
        if row.get('status') != 'passed' or row.get('validation', {}).get('passed') is not True:
            raise WorkflowMismatch('Passed report contains an unvalidated run')
        if row.get('scenario') not in SCENARIOS[suite] or row.get('backend') not in ('celery', 'dbworker'):
            raise WorkflowMismatch('Unexpected scenario or backend')
        repetition = row.get('repetition')
        if type(repetition) is not int or not 1 <= repetition <= expected_profile['repetitions']:
            raise WorkflowMismatch('Repetition differs from trusted workload')
        validate_native_sample(row, suite)
        pair = groups.setdefault((row['scenario'], repetition), {})
        if row['backend'] in pair:
            raise WorkflowMismatch('Duplicate backend sample cannot overwrite another run')
        pair[row['backend']] = row
    expected_groups = {(scenario, repetition) for scenario in SCENARIOS[suite]
                       for repetition in range(1, expected_profile['repetitions'] + 1)}
    if set(groups) != expected_groups:
        raise WorkflowMismatch('Missing configured scenarios or repetitions')
    parity = []
    for (_, repetition), pair in sorted(groups.items()):
        if set(pair) != {'celery', 'dbworker'}:
            raise WorkflowMismatch('Native scenario lacks a paired backend')
        if suite == 'imagededup':
            for row in pair.values():
                replay_image_workload(row, directory, expected_profile['images'])
                evidence = row.get('operation_evidence', {})
                if evidence.get('passed') is not True or evidence.get('quiescence_verified') is not True:
                    raise WorkflowMismatch('Missing image business-work evidence or quiescence')
                if evidence.get('page_bound') != 250 or evidence.get('maximum_page_items', 251) > 250:
                    raise WorkflowMismatch('Image task page granularity changed')
                if evidence.get('failed_attempts') or evidence.get('missing_attempts') or evidence.get('duplicate_build_deliveries'):
                    raise WorkflowMismatch('Image task attempts contain failures or duplicates')
            names = ('submitted_builds', 'completed_builds', 'submitted_comparisons', 'completed_comparisons', 'scored_pairs', 'page_bound', 'workload_digest')
            for name in names:
                if pair['celery']['operation_evidence'].get(name) != pair['dbworker']['operation_evidence'].get(name):
                    raise WorkflowMismatch(f'Image business workload mismatch: {name}')
            parity.append({'repetition': repetition, 'passed': True, 'contract': 'image_successful_business_work'})
        else:
            count = expected_profile['requests']
            left = replay_graph(pair['celery'], suite, directory, count)
            right = replay_graph(pair['dbworker'], suite, directory, count)
            parity.append({'repetition': repetition, **compare_graphs(left, right)})
    report['admission'] = {'passed': True, 'native_baseline': True, 'workflow_parity': parity,
                           'contract': 'successful_business_workflow',
                           'fault_lifecycle_parity': 'not established by throughput measurement'}
