"""Require native source evidence and replayable business-work parity for results."""
from __future__ import annotations

import hashlib
import json
import importlib.util
import math
from collections import Counter
from pathlib import Path

from benchmarks.common.native_admission import APPLICATIONS, NativeAdmissionError
from benchmarks.common.native_binding import validate_task_binding
from benchmarks.common.business_evidence import validate_business_binding as _validate_business_binding
from benchmarks.common.smtp_evidence import validate_smtp_evidence
from benchmarks.common.superset_output import validate_superset_output
from benchmarks.common.timing_evidence import elapsed_seconds, validate_timing_window
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


def _validate_origin(evidence, suite, expected):
    if not isinstance(evidence, dict):
        raise NativeAdmissionError(f'{suite}: missing native worker source evidence')
    if evidence.get('passed') is not True or evidence.get('worker_app') != APPLICATIONS[suite]:
        raise NativeAdmissionError(f'{suite}: missing original Celery application admission')
    if (evidence.get('check') != 'live_task_origin_and_ast'
            or not isinstance(evidence.get('tasks'), dict) or not evidence['tasks']):
        raise NativeAdmissionError(f'{suite}: missing live native task body origin checks')
    if evidence.get('observer_only') is not True:
        raise NativeAdmissionError(f'{suite}: baseline observation must not replace tasks')
    expected = set(expected)
    if set(evidence['tasks']) != expected or set(evidence.get('task_names', [])) != expected or len(evidence.get('task_names', [])) != len(expected):
        raise NativeAdmissionError(f'{suite}: native task declaration is incomplete or changed')
    source = ROOT / 'examples' / suite
    if suite == 'sentry':
        source /= 'src'
    if suite == 'imagededup':
        source = ROOT / 'examples/imagededup_system_redis_celery/src'
    for name, task in evidence['tasks'].items():
        if not isinstance(task, dict):
            raise NativeAdmissionError(f'{suite}: malformed native task body evidence')
        path = _safe_relative(source, task.get('source'))
        if hashlib.sha256(path.read_bytes()).hexdigest() != task.get('source_sha256'):
            raise NativeAdmissionError(f'{suite}: native source checksum differs: {name}')
        validate_task_binding(suite, name, task, source)


def validate_native_sample(row, suite):
    if row.get('backend') == 'celery':
        _validate_origin(row.get('native_execution', {}), suite, TASK_STAGES[suite])


def _validate_worker_and_arguments(events, suite):
    published = {}
    for event in events:
        details = event.get('details', {})
        name = details.get('task_name')
        if ((event.get('event') in ('submitted', 'started') or name is not None)
                and TASK_STAGES[suite].get(name) != event.get('stage')):
            raise WorkflowMismatch('Observed task name differs from the original business stage')
        if event.get('event') in ('submitted', 'started'):
            fingerprint = details.get('argument_sha256')
            if (not isinstance(fingerprint, str) or len(fingerprint) != 64
                    or any(character not in '0123456789abcdef' for character in fingerprint)):
                raise WorkflowMismatch('Missing original task argument fingerprint')
            key = event.get('node_id')
            if event['event'] == 'submitted':
                published[key] = fingerprint
            else:
                _validate_origin(details.get('native_worker_origin', {}), suite, [name])
    for event in events:
        if event.get('event') == 'started':
            if event['details']['argument_sha256'] != published.get(event.get('node_id')):
                raise WorkflowMismatch('Worker arguments differ from original publication intent')


def replay_graph(row, suite, directory, expected_requests, *, expected_profile=None):
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
    declared_warmup = row.get('warmup_operations')
    if (not isinstance(declared_warmup, list) or len(declared_warmup) != 2
            or len(set(declared_warmup)) != 2 or set(declared_warmup) != warmups):
        raise WorkflowMismatch('Missing or changed explicit warmup identities')
    _validate_worker_and_arguments(events, suite)
    replayed = validate_graph(events, operations, contract_stages, contract_edges,
                              warmup_operations=declared_warmup)
    validate_timing_window(row.get('measurement_window'), row.get('metrics', {}).get('wall_seconds'),
                           events, operations)
    _validate_business_binding(row, suite, events, expected_requests, expected_profile=expected_profile)
    if suite == 'superset':
        try:
            validate_superset_output(row, directory, expected_requests, events=events)
        except (ValueError, TypeError, KeyError, OSError) as error:
            raise WorkflowMismatch(f'Invalid persisted SQL Lab output evidence: {error}') from error
    if suite == 'sentry':
        try:
            validate_smtp_evidence(row, directory)
        except (ValueError, TypeError, KeyError, OSError) as error:
            raise WorkflowMismatch(f'Invalid persisted SMTP business evidence: {error}') from error
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
        if sum(widths) != images - 1 or item.get('scored_pairs') != images - 1:
            raise WorkflowMismatch('Image scoring page work was omitted or duplicated')
        if scenario == 'comparison' and sorted(width for width in widths if width) != sorted(positive_pages):
            raise WorkflowMismatch('Image ready-candidate scoring page granularity changed')
        ordinals.append(item.get('input_ordinal'))
        page_items.extend(widths)
    if any(type(ordinal) is not int for ordinal in ordinals) or sorted(ordinals) != list(range(comparisons)):
        raise WorkflowMismatch('Image comparison identities are incomplete or duplicated')
    if evidence.get('maximum_page_items') != max(page_items, default=0):
        raise WorkflowMismatch('Image maximum page count differs from observed pages')
    duplicates = evidence.get('duplicate_build_deliveries', 0)
    if type(duplicates) is not int or duplicates < 0:
        raise WorkflowMismatch('Invalid duplicate image build delivery accounting')
    if evidence.get('empty_comparison_attempts') != page_items.count(0) or evidence.get('business_attempts') != builds + duplicates + len(page_items):
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
    backend = row['backend']
    if any(record.get('backend') != backend for record in records):
        raise WorkflowMismatch('Image snapshot backend differs from sample')
    verifier_path = ROOT / 'benchmarks/imagededup_benckmark/src/imagededup_benckmark/evidence.py'
    spec = importlib.util.spec_from_file_location('benchmark_image_artifact_verifier', verifier_path)
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    try:
        if sum(record.get('event') == 'quiescence_barrier' for record in records) != 1:
            raise WorkflowMismatch('Image snapshot must contain one final live idle receipt')
        if verifier.validate_quiescence(records) is not True:
            raise WorkflowMismatch('Image snapshot lacks a live quiescence receipt')
    except (AssertionError, ValueError, TypeError, KeyError) as error:
        raise WorkflowMismatch(f'Invalid image live quiescence receipt: {error}') from error
    window = row.get('measurement_window')
    duration = elapsed_seconds(window)
    metrics = row.get('metrics', {})
    wall = metrics.get('wall_seconds')
    if (type(wall) not in (int, float) or not math.isfinite(wall)
            or abs(wall - duration) > 1e-9):
        raise WorkflowMismatch('Image duration differs from captured monotonic boundaries')
    for name in ('submission_seconds', 'business_finished_seconds'):
        value = metrics.get(name)
        if (type(value) not in (int, float) or not math.isfinite(value)
                or not 0 <= value <= duration):
            raise WorkflowMismatch('Image measured window excludes submission or completion barrier')
    lower = window['start']['timestamp_ns'] - window['start']['uncertainty_ns']
    upper = window['end']['timestamp_ns'] + window['end']['uncertainty_ns']
    attempts = {}
    for record in records:
        if record.get('backend') != backend:
            raise WorkflowMismatch('Image snapshot backend differs from sample')
        if record.get('event') == 'quiescence_barrier':
            if record.get('stage') != 'barrier' or record.get('workspace_id') is not None:
                raise WorkflowMismatch('Image barrier has invalid record scope')
            continue  # Only the independently validated typed idle receipt.
        if record.get('stage') == 'dispatch':
            if backend == 'celery' and record.get('event') == 'started':
                _validate_origin(record.get('native_worker_origin', {}), 'imagededup', ['images.dispatch'])
            continue  # Control polls are retained separately from business work.
        if record.get('stage') not in ('build', 'comparison') or record.get('workspace_id') != trace['workspace']:
            raise WorkflowMismatch('Unexpected business work in image scenario snapshot')
        identity = record.get('attempt_id')
        if not isinstance(identity, str) or not identity:
            raise WorkflowMismatch('Missing image business attempt identity')
        attempts.setdefault(identity, []).append(record)
    barrier = next(record for record in records if record.get('event') == 'quiescence_barrier')
    # This receipt counts all completed business attempts since stack startup;
    # a scenario snapshot deliberately excludes previous scenarios and warmup.
    if (type(barrier.get('observed_attempt_count')) is not int
            or barrier['observed_attempt_count'] < len(attempts)):
        raise WorkflowMismatch('Image idle receipt business-attempt scope differs')
    for identity, phases in attempts.items():
        if Counter(record.get('event') for record in phases) != Counter({'started': 1, 'finished': 1}):
            raise WorkflowMismatch(f'Missing or duplicate image business attempt phases: {identity}')
        start = next(record for record in phases if record['event'] == 'started')
        end = next(record for record in phases if record['event'] == 'finished')
        if any(start.get(key) != end.get(key) for key in ('task_id', 'source_id', 'workspace_id', 'stage', 'backend')):
            raise WorkflowMismatch('Image business attempt identity changed')
        started_ns, finished_ns = start.get('timestamp_ns'), end.get('timestamp_ns')
        if (type(started_ns) is not int or type(finished_ns) is not int
                or started_ns <= 0 or finished_ns < started_ns):
            raise WorkflowMismatch('Image attempt has missing or reversed actual timestamps')
        if (type(start.get('items')) is not int or type(end.get('items_after')) is not int
                or type(end.get('page_items')) is not int):
            raise WorkflowMismatch('Image observed work counts must be integers')
        if start['stage'] == 'build':
            expected = [start['source_id']] if end['page_items'] == 1 else []
            if (end['page_items'] not in (0, 1)
                    or end.get('build_accounting') != 'transaction_committed_hash_writes'
                    or end.get('built_artifact_ids') != expected):
                raise WorkflowMismatch('Image build lacks transaction-attributed committed hash writes')
        elif (end.get('page_accounting') != 'transaction_committed_orm_inserts'
              or not isinstance(end.get('scored_rows'), list)
              or len(end['scored_rows']) != end['page_items']):
            raise WorkflowMismatch('Image page lacks transaction-attributed committed candidates')
        if backend == 'celery':
            if not isinstance(start.get('task_id'), str) or not start['task_id']:
                raise WorkflowMismatch('Missing original Celery image task identity')
            name = 'images.build' if start['stage'] == 'build' else 'images.compare'
            _validate_origin(start.get('native_worker_origin', {}), 'imagededup', [name])
            if type(start.get('source_revision')) is not int or start['source_revision'] < 0:
                raise WorkflowMismatch('Image worker lacks an original source revision')
        if end['page_items'] > 0:
            committed_ns = end.get('business_committed_timestamp_ns')
            if (type(committed_ns) is not int or committed_ns < started_ns
                    or committed_ns > finished_ns or not lower <= started_ns <= upper
                    or not lower <= committed_ns <= upper):
                raise WorkflowMismatch('Positive image business commit lies outside measured boundaries')
            # Native task_postrun can occur after the measured SQL commit and
            # final API response. Its later diagnostic time is not completion.
    try:
        replayed = verifier.verify(records, workspace=trace['workspace'], artifact_ids=trace['artifact_ids'],
            request_ids=trace['request_ids'], new_builds=trace['new_builds'], page_size=250, record_offset=0)
    except (AssertionError, ValueError, TypeError, KeyError) as error:
        raise WorkflowMismatch(f'Image persisted work evidence is invalid: {error}') from error
    if replayed != row.get('operation_evidence'):
        raise WorkflowMismatch('Reported image work differs from persisted observations')
    validate_image_workload(row, images)
    output_path = ROOT / "benchmarks/imagededup_benckmark/src/imagededup_benckmark/output_evidence.py"
    output_spec = importlib.util.spec_from_file_location("benchmark_image_output_verifier", output_path)
    output_verifier = importlib.util.module_from_spec(output_spec)
    output_spec.loader.exec_module(output_verifier)
    try:
        output_verifier.validate(row, directory, images)
    except (ValueError, TypeError, KeyError, OSError) as error:
        raise WorkflowMismatch(f"Invalid image output receipt: {error}") from error
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
    image_outputs = {}
    image_hashes = None
    image_comparisons = None
    if suite == "imagededup":
        # These are the reviewed official suite runner defaults; child-reported
        # settings never select the oracle used by aggregate admission.
        for setting, expected in (("top_k", 10), ("max_distance", 10), ("page_size", 250)):
            if type(profile.get(setting)) is not int or profile[setting] != expected:
                raise WorkflowMismatch(f"Image setting differs from official workload: {setting}")
    for (scenario, repetition), pair in sorted(groups.items()):
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
                if evidence.get('failed_attempts') or evidence.get('missing_attempts'):
                    raise WorkflowMismatch('Image task attempts contain failures or missing completions')
            names = ('submitted_builds', 'completed_builds', 'submitted_comparisons', 'completed_comparisons', 'scored_pairs', 'page_bound', 'workload_digest')
            for name in names:
                if pair['celery']['operation_evidence'].get(name) != pair['dbworker']['operation_evidence'].get(name):
                    raise WorkflowMismatch(f'Image business workload mismatch: {name}')
            outputs = [row["validation"] for row in pair.values()]
            if outputs[0] != outputs[1]:
                raise WorkflowMismatch("Replayed image outputs differ between backends")
            if scenario in image_outputs and image_outputs[scenario] != outputs[0]:
                raise WorkflowMismatch("Replayed image outputs differ between repetitions")
            image_outputs[scenario] = outputs[0]
            hashes = outputs[0].get("hashes_digest")
            if image_hashes is not None and image_hashes != hashes:
                raise WorkflowMismatch("Replayed image hashes differ across identical-input scenarios")
            image_hashes = hashes
            if scenario != "build":
                comparisons = outputs[0].get("top_k_digests")
                if image_comparisons is not None and image_comparisons != comparisons:
                    raise WorkflowMismatch("Replayed image top-K differs across identical-input scenarios")
                image_comparisons = comparisons
            parity.append({'repetition': repetition, 'passed': True, 'contract': 'image_successful_business_work'})
        else:
            count = expected_profile['requests']
            left = replay_graph(pair['celery'], suite, directory, count, expected_profile=expected_profile)
            right = replay_graph(pair['dbworker'], suite, directory, count, expected_profile=expected_profile)
            parity.append({'repetition': repetition, **compare_graphs(left, right)})
    report['admission'] = {'passed': True, 'native_baseline': True, 'workflow_parity': parity,
                           'contract': 'successful_business_workflow',
                           'fault_lifecycle_parity': 'not established by throughput measurement'}
