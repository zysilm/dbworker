"""Replay admission rejects altered artifacts and reduced measured workloads."""
import copy
import hashlib
import json
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from benchmarks.common.native_admission import APPLICATIONS, NativeAdmissionError
from benchmarks.common.native_binding import BINDINGS, SOURCE_SHA256
from benchmarks.common.argument_evidence import argument_digest
from benchmarks.common.performance_admission import (
    ROOT, TASK_STAGES, replay_graph, replay_image_workload, validate_image_workload,
    validate_native_report, validate_native_sample,
)
from benchmarks.common.workflow_graph import WorkflowMismatch, validate_graph


class PerformanceAdmissionTests(unittest.TestCase):
    def setUp(self):
        # These synthetic receipts isolate graph, origin, argument and timing gates.
        # Public API/business identity validation has independent adversarial tests.
        binding = patch('benchmarks.common.performance_admission._validate_business_binding')
        binding.start()
        self.addCleanup(binding.stop)

    @staticmethod
    def origin(suite, name):
        source, module, function, factory = BINDINGS[suite][name]
        return {'passed': True, 'worker_app': APPLICATIONS[suite],
                'check': 'live_task_origin_and_ast', 'observer_only': True,
                'task_names': [name], 'tasks': {name: {'source': source,
                    'function': function, 'source_sha256': SOURCE_SHA256[(suite, source)]}}}

    def graph_fixture(self, directory, backend='celery', warmup_count=2):
        events = []
        warmups = [f'warmup:{i}' for i in range(warmup_count)]
        for index, op in enumerate(warmups + ['0', '1']):
            for task, stage in TASK_STAGES['posthog'].items():
                parent = f'{backend}-{op}-notification' if stage == 'delivery' else None
                for phase in ('submitted', 'started', 'succeeded'):
                    events.append({'schema_version': 1, 'backend': backend, 'operation_id': op,
                                   'node_id': f'{backend}-{op}-{stage}', 'stage': stage,
                                   'parent_id': parent, 'event': phase,
                                   'timestamp_ns': 1_000_000_000 + index * 100 +
                                       (10 if stage == 'delivery' else 0) +
                                       {'submitted': 1, 'started': 2, 'succeeded': 3}[phase],
                                   'details': {'task_name': task, 'argument_sha256': argument_digest([op], {}),
                                               'native_worker_origin': self.origin('posthog', task)}})
        path = Path(directory) / f'{backend}.jsonl'
        path.write_text(''.join(json.dumps(event) + '\n' for event in events))
        return {'backend': backend, 'scenario': 'native_two_factor_notification', 'status': 'passed',
                'repetition': 1, 'validation': {'passed': True},
                'warmup_operations': warmups,
                'measurement_window': {'schema_version': 1, 'clock_domain': 'unix_time_ns',
                    'start': {'timestamp_ns': 1_000_000_000 + warmup_count * 100, 'monotonic_ns': 1_000_000_000 + warmup_count * 100, 'uncertainty_ns': 0},
                    'end': {'timestamp_ns': 2_000_000_000 + warmup_count * 100, 'monotonic_ns': 2_000_000_000 + warmup_count * 100, 'uncertainty_ns': 0}},
                'metrics': {'wall_seconds': 1.0},
                'workflow_trace': {'path': path.name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()},
                'workflow_graph': validate_graph(events, ['0', '1'], {'notification': 1, 'delivery': 1},
                                                 [('notification', 'delivery')], warmup_operations=warmups)}

    def test_diagnostic_graph_requires_trusted_eight_warmups(self):
        with tempfile.TemporaryDirectory() as temporary:
            row = self.graph_fixture(temporary, warmup_count=8)
            self.assertEqual(replay_graph(row, 'posthog', temporary, 2,
                expected_profile={'warmup_requests': 8})['nodes'], 4)
            with self.assertRaises(WorkflowMismatch):
                replay_graph(row, 'posthog', temporary, 2)
            changed = copy.deepcopy(row)
            changed['warmup_operations'] = changed['warmup_operations'][:2]
            with self.assertRaises(WorkflowMismatch):
                replay_graph(changed, 'posthog', temporary, 2, expected_profile={'warmup_requests': 8})
            events = [json.loads(line) for line in (Path(temporary) / 'celery.jsonl').read_text().splitlines()]
            events = [event for event in events if not (event['operation_id'] == 'warmup:7'
                      and event['stage'] == 'delivery' and event['event'] == 'succeeded')]
            path = Path(temporary) / 'celery.jsonl'
            path.write_text(''.join(json.dumps(event) + '\n' for event in events))
            row['workflow_trace']['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaises(WorkflowMismatch):
                replay_graph(row, 'posthog', temporary, 2, expected_profile={'warmup_requests': 8})

    def test_real_artifact_replay_and_wrong_count_backend_or_checksum(self):
        with tempfile.TemporaryDirectory() as temporary:
            row = self.graph_fixture(temporary)
            self.assertEqual(replay_graph(row, 'posthog', temporary, 2)['nodes'], 4)
            with self.assertRaises(WorkflowMismatch):
                replay_graph(row, 'posthog', temporary, 100)
            for update in ({'backend': 'dbworker'}, {'workflow_trace': {**row['workflow_trace'], 'sha256': '0' * 64}},
                           {'workflow_trace': {**row['workflow_trace'], 'path': str(Path(temporary) / 'celery.jsonl')}},
                           {'workflow_trace': {**row['workflow_trace'], 'path': '../celery.jsonl'}}):
                with self.subTest(update=update), self.assertRaises(WorkflowMismatch):
                    replay_graph({**row, **update}, 'posthog', temporary, 2)

    def test_graph_metadata_and_task_name_cannot_override_trace(self):
        with tempfile.TemporaryDirectory() as temporary:
            row = self.graph_fixture(temporary)
            modified = copy.deepcopy(row)
            modified['workflow_graph']['stage_counts']['delivery'] = 1
            with self.assertRaises(WorkflowMismatch):
                replay_graph(modified, 'posthog', temporary, 2)
            path = Path(temporary) / row['workflow_trace']['path']
            events = [json.loads(line) for line in path.read_text().splitlines()]
            events[0]['details']['task_name'] = 'benchmark.synthetic'
            path.write_text(''.join(json.dumps(event) + '\n' for event in events))
            row['workflow_trace']['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaises(WorkflowMismatch):
                replay_graph(row, 'posthog', temporary, 2)

    def test_worker_origin_argument_and_timing_proofs_are_required(self):
        mutations = ('arguments', 'missing_arguments', 'wrong_function', 'missing_worker_origin',
                     'out_of_window', 'reverse_phases', 'missing_warmups', 'different_wall')
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                row = self.graph_fixture(temporary)
                path = Path(temporary) / row['workflow_trace']['path']
                events = [json.loads(line) for line in path.read_text().splitlines()]
                started = next(event for event in events if event['event'] == 'started'
                               and event['operation_id'] == '0')
                if mutation == 'arguments':
                    started['details']['argument_sha256'] = 'f' * 64
                elif mutation == 'missing_arguments':
                    started['details'].pop('argument_sha256')
                elif mutation == 'wrong_function':
                    origin = started['details']['native_worker_origin']
                    origin['tasks'][started['details']['task_name']]['function'] = 'other_genuine_function'
                elif mutation == 'missing_worker_origin':
                    started['details'].pop('native_worker_origin')
                elif mutation == 'out_of_window':
                    row['measurement_window']['start']['timestamp_ns'] += 50
                    row['measurement_window']['start']['monotonic_ns'] += 50
                    row['measurement_window']['end']['timestamp_ns'] += 50
                    row['measurement_window']['end']['monotonic_ns'] += 50
                elif mutation == 'reverse_phases':
                    started['timestamp_ns'] -= 2
                elif mutation == 'missing_warmups':
                    row.pop('warmup_operations')
                else:
                    row['metrics']['wall_seconds'] = 2.0
                path.write_text(''.join(json.dumps(event) + '\n' for event in events))
                row['workflow_trace']['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
                with self.assertRaises((WorkflowMismatch, NativeAdmissionError)):
                    replay_graph(row, 'posthog', temporary, 2)

    def test_empty_duplicate_and_missing_repetition_cannot_pass(self):
        with tempfile.TemporaryDirectory() as temporary:
            rows = [self.graph_fixture(temporary), self.graph_fixture(temporary, 'dbworker')]
            report = {'status': 'passed', 'suite_id': 'posthog', 'profile': 'full',
                      'configuration': {'profile': {'requests': 2, 'repetitions': 1}}, 'runs': rows}
            # Native origin admission is separately exercised against real Celery;
            # these fixtures test orchestration/profile/trace replay only.
            with patch('benchmarks.common.performance_admission.validate_native_sample'):
                validate_native_report(report, temporary, expected_profile={'requests': 2, 'repetitions': 1})
                for runs in ([], rows + [rows[0]], rows[:1]):
                    with self.subTest(runs=runs), self.assertRaises(WorkflowMismatch):
                        validate_native_report({**report, 'runs': runs}, temporary,
                                               expected_profile={'requests': 2, 'repetitions': 1})
                with self.assertRaises(WorkflowMismatch):
                    validate_native_report(report, temporary, expected_profile={'requests': 100, 'repetitions': 1})
                with self.assertRaises(WorkflowMismatch):
                    validate_native_report(report, temporary, expected_profile={'requests': 2, 'repetitions': 5})

    def test_superset_replay_reads_observed_results_and_rejects_common_wrong_output(self):
        from benchmarks.tests.test_business_evidence import BusinessEvidenceTests
        from benchmarks.tests.test_superset_output import SupersetOutputTests
        from benchmarks.common.business_evidence import validate_business_binding
        from benchmarks.common.superset_output import digest as sql_digest
        with tempfile.TemporaryDirectory() as temporary:
            row, events = BusinessEvidenceTests().fixture('superset', count=2)
            output_row, receipt = SupersetOutputTests().fixture(count=2)
            row.update(backend='celery', measurement_window=output_row['measurement_window'],
                       metrics=output_row['metrics'])
            row['validation']['output_digest'] = output_row['validation']['output_digest']
            for index, (binding, observation) in enumerate(zip(row['business_bindings'], receipt['results'])):
                binding['output_sha256'] = output_row['business_bindings'][index]['output_sha256']
                for key in ('query_id', 'node_id', 'results_key', 'sql_sha256', 'argument_sha256'):
                    observation[key] = binding[key]
            task = next(iter(TASK_STAGES['superset']))
            for event in events:
                event.update(schema_version=1, backend='celery', timestamp_ns=
                    (500 if event['operation_id'].startswith('warmup:') else 1100) +
                    {'submitted': 1, 'started': 2, 'succeeded': 3}[event['event']])
                event['details'].update(task_name=task, native_worker_origin=self.origin('superset', task))
            trace = Path(temporary) / 'trace.jsonl'
            trace.write_text(''.join(json.dumps(event) + '\n' for event in events))
            row['workflow_trace'] = {'path': trace.name, 'sha256': hashlib.sha256(trace.read_bytes()).hexdigest()}
            row['workflow_graph'] = validate_graph(events, ['query-0', 'query-1'], {'sql_lab': 1}, [],
                                                   warmup_operations=['warmup:0', 'warmup:1'])
            path = Path(temporary) / 'sql-results-evidence.json'

            def save_receipt():
                path.write_text(json.dumps(receipt))
                row['sql_result_evidence'] = {'schema_version': 1, 'path': path.name,
                                             'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}

            save_receipt()
            with patch('benchmarks.common.performance_admission._validate_business_binding',
                       side_effect=validate_business_binding):
                self.assertEqual(replay_graph(row, 'superset', temporary, 2)['nodes'], 2)
                # A common wrong observation plus self-consistent hashes cannot
                # be admitted by either arm: the independent fixture rejects it.
                receipt['results'][0]['result']['data'][0]['total'] += 1
                row['business_bindings'][0]['output_sha256'] = sql_digest(receipt['results'][0]['result'])
                row['validation']['output_digest'] = sql_digest([item['result'] for item in receipt['results']])
                save_receipt()
                with self.assertRaisesRegex(WorkflowMismatch, 'independent oracle'):
                    replay_graph(row, 'superset', temporary, 2)

    def test_saleor_report_passes_trusted_profile_to_each_business_replay(self):
        profile = {'requests': 3, 'products': 4, 'repetitions': 1}
        rows = [{'backend': backend, 'scenario': 'product_csv_export', 'repetition': 1,
                 'status': 'passed', 'validation': {'passed': True}}
                for backend in ('celery', 'dbworker')]
        report = {'status': 'passed', 'suite_id': 'saleor', 'profile': 'full',
                  'configuration': {'profile': dict(profile)}, 'runs': rows}
        graph = {'passed': True, 'operations': ['0', '1', '2'], 'nodes': 6,
                 'stage_counts': {'export': 3, 'email': 3},
                 'edge_counts': {'export->email': 3}}
        # This isolates orchestration; production business replay is tested with
        # self-consistent reduced native root arguments in test_business_evidence.
        with patch('benchmarks.common.performance_admission.validate_native_sample'), \
                patch('benchmarks.common.performance_admission.replay_graph', return_value=graph) as replay:
            validate_native_report(report, '.', expected_profile=profile)
        self.assertEqual(replay.call_count, 2)
        for call, row in zip(replay.call_args_list, rows):
            self.assertEqual(call.args, (row, 'saleor', '.', 3))
            self.assertEqual(call.kwargs, {'expected_profile': profile})

    def test_empty_task_sets_cannot_forge_native_admission(self):
        row = {'backend': 'celery', 'native_execution': {'passed': True,
               'worker_app': 'posthog.celery:app', 'check': 'live_task_origin_and_ast',
               'observer_only': True, 'tasks': {'irrelevant': {}}, 'task_names': ['irrelevant']}}
        with self.assertRaises(NativeAdmissionError):
            validate_native_sample(row, 'posthog')

    def image_fixture(self, images=3, scenario='mixed', duplicates=0):
        builds = 0 if scenario == 'comparison' else images
        comparisons = 0 if scenario == 'build' else images
        normalized = {'build_inputs': list(range(builds)), 'comparison_inputs': list(range(comparisons)),
                      'scored_pairs': comparisons * (images - 1), 'page_bound': 250}
        evidence = {'submitted_builds': builds, 'completed_builds': builds,
                    'submitted_comparisons': comparisons, 'completed_comparisons': comparisons,
                    'scored_pairs': comparisons * (images - 1), 'maximum_page_items': images - 1 if comparisons else 0,
                    'empty_comparison_attempts': 0, 'business_attempts': builds + duplicates + comparisons,
                    'duplicate_build_deliveries': duplicates,
                    'passed': True, 'quiescence_verified': True, 'page_bound': 250,
                    'failed_attempts': 0, 'missing_attempts': 0,
                    'comparisons': [{'input_ordinal': n, 'page_sizes': [images - 1],
                                     'scored_pairs': images - 1} for n in range(comparisons)],
                    'workload_digest': hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()}
        return {'scenario': scenario, 'operation_evidence': evidence,
                'validation': {'passed': True, 'artifacts': images, 'requests': comparisons,
                               'scored_pairs': comparisons * (images - 1)}}

    def test_complete_paired_image_report_retains_zero_write_redeliveries(self):
        rows = []
        for scenario in ('build', 'comparison', 'mixed'):
            for backend in ('celery', 'dbworker'):
                duplicates = int(backend == 'celery' and scenario != 'comparison')
                row = self.image_fixture(scenario=scenario, duplicates=duplicates)
                row.update(backend=backend, status='passed', repetition=1)
                rows.append(row)
        report = {'suite_id': 'imagededup', 'status': 'passed', 'profile': 'full',
                  'configuration': {'images': 3, 'repetitions': 1, 'top_k': 10, 'max_distance': 10, 'page_size': 250}, 'runs': rows}

        def replay_summary(row, directory, images):
            # Source origin and actual trace replay have separate direct tests.
            # Preserve trusted workload/page/attempt checks in this report-level
            # regression, including the extra zero-write delivery's overhead.
            validate_image_workload(row, images)
            return row['operation_evidence']

        with patch('benchmarks.common.performance_admission.validate_native_sample'), \
                patch('benchmarks.common.performance_admission.replay_image_workload', side_effect=replay_summary):
            validate_native_report(report, '.', expected_profile={'images': 3, 'repetitions': 1})
        self.assertTrue(report['admission']['passed'])
        self.assertEqual(rows[0]['operation_evidence']['duplicate_build_deliveries'], 1)
        self.assertEqual(rows[0]['operation_evidence']['business_attempts'], 4)
        self.assertEqual(rows[1]['operation_evidence']['business_attempts'], 3)

    def test_image_pair_repetition_outputs_and_official_settings_are_enforced(self):
        rows = []
        for repetition in (1, 2):
            for scenario in ('build', 'comparison', 'mixed'):
                for backend in ('celery', 'dbworker'):
                    row = self.image_fixture(scenario=scenario)
                    row.update(backend=backend, status='passed', repetition=repetition)
                    rows.append(row)
        baseline = {'suite_id': 'imagededup', 'status': 'passed', 'profile': 'full',
                    'configuration': {'images': 3, 'repetitions': 2, 'top_k': 10,
                                      'max_distance': 10, 'page_size': 250}, 'runs': rows}
        with patch('benchmarks.common.performance_admission.validate_native_sample'), \
                patch('benchmarks.common.performance_admission.replay_image_workload'):
            validate_native_report(copy.deepcopy(baseline), '.', expected_profile={'images': 3, 'repetitions': 2})
            for mutation in ('pair', 'repetition', 'top_k', 'max_distance', 'page_size'):
                bad = copy.deepcopy(baseline)
                if mutation == 'pair':
                    bad['runs'][1]['validation']['hashes_digest'] = 'changed'
                elif mutation == 'repetition':
                    for row in bad['runs'][6:8]:
                        row['validation']['hashes_digest'] = 'changed'
                else:
                    bad['configuration'][mutation] = 1
                with self.subTest(mutation=mutation), self.assertRaises(WorkflowMismatch):
                    validate_native_report(bad, '.', expected_profile={'images': 3, 'repetitions': 2})

    def test_mixed_ready_candidate_fragmentation_is_valid_but_comparison_fragmentation_is_not(self):
        mixed = self.image_fixture()
        mixed['operation_evidence']['comparisons'][0]['page_sizes'] = [1, 1]
        mixed['operation_evidence']['business_attempts'] += 1
        validate_image_workload(mixed, 3)
        comparison = self.image_fixture(scenario='comparison')
        comparison['operation_evidence']['comparisons'][0]['page_sizes'] = [1, 1]
        comparison['operation_evidence']['business_attempts'] += 1
        with self.assertRaises(WorkflowMismatch):
            validate_image_workload(comparison, 3)

    def test_paired_reduced_image_counts_and_collapsed_page_details_fail(self):
        row = self.image_fixture()
        validate_image_workload(row, 3)
        with self.assertRaises(WorkflowMismatch):
            validate_image_workload(row, 1000)
        for update in ({'submitted_builds': 2}, {'comparisons': []}, {'business_attempts': 1},
                       {'maximum_page_items': 0}, {'workload_digest': 'f' * 64}):
            modified = copy.deepcopy(row)
            modified['operation_evidence'].update(update)
            with self.subTest(update=update), self.assertRaises(WorkflowMismatch):
                validate_image_workload(modified, 3)
        modified = copy.deepcopy(row)
        modified['operation_evidence']['comparisons'][0]['page_sizes'] = [1]
        with self.assertRaises(WorkflowMismatch):
            validate_image_workload(modified, 3)

    def test_image_observations_are_replayed_not_trusted_as_summaries(self):
        with tempfile.TemporaryDirectory() as temporary:
            records = []
            for stage, sources, width in (('build', [1, 2, 3], 1), ('comparison', [11, 12, 13], 2)):
                for identity in sources:
                    start = {'backend': 'celery', 'stage': stage, 'source_id': identity,
                             'workspace_id': 1, 'attempt_id': f'{stage}-{identity}',
                             'task_id': f'original-{stage}-{identity}', 'items': 0, 'event': 'started',
                             'timestamp': 1.1, 'timestamp_ns': 1_100_000_000,
                             'source_revision': 0,
                             'native_worker_origin': self.origin('imagededup',
                                 'images.build' if stage == 'build' else 'images.compare')}
                    if stage == 'comparison':
                        start['query_artifact_id'] = identity - 10
                    finish = {**start, 'event': 'finished', 'state': 'SUCCESS',
                              'items_after': width, 'page_items': width, 'timestamp': 1.3,
                              'timestamp_ns': 1_300_000_000,
                              'business_committed_timestamp_ns': 1_200_000_000}
                    if stage == 'build':
                        finish.update(build_accounting='transaction_committed_hash_writes',
                                      built_artifact_ids=[identity])
                    if stage == 'comparison':
                        # Continuations may finish before the current task's callback.
                        finish['items_after'] = 99
                        finish['query_artifact_id'] = identity - 10
                        finish.update(page_accounting='transaction_committed_orm_inserts',
                                      scored_rows=[[identity, candidate] for candidate in (1, 2, 3)
                                                   if candidate != identity - 10])
                    records.extend([start, finish])
            records.append({'event': 'quiescence_barrier', 'stage': 'barrier',
                'backend': 'celery', 'idle': True, 'observed_at': 2.1, 'timestamp': 2.2,
                'workspace_id': None, 'observed_attempt_count': 6,
                'worker_responses_complete': True, 'pending_business_outbox': 0,
                'worker_states': {phase: {'build-worker': 0, 'comparison-worker': 0}
                                  for phase in ('active', 'reserved', 'scheduled')},
                'redis_priority_steps': [0, 3, 6, 9],
                'redis_lanes': [{'queue': queue, 'priority': priority, 'messages': 0}
                    for queue in ('image_build', 'image_compare') for priority in (0, 3, 6, 9)]})
            path = Path(temporary) / 'image.jsonl'
            path.write_text(''.join(json.dumps(record) + '\n' for record in records))
            spec = importlib.util.spec_from_file_location('test_image_verifier',
                ROOT / 'benchmarks/imagededup_benckmark/src/imagededup_benckmark/evidence.py')
            verifier = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(verifier)
            row = self.image_fixture()
            row['backend'] = 'celery'
            row['measurement_window'] = {'schema_version': 1, 'clock_domain': 'unix_time_ns',
                'start': {'timestamp_ns': 1_000_000_000, 'monotonic_ns': 1_000_000_000, 'uncertainty_ns': 0},
                'end': {'timestamp_ns': 2_000_000_000, 'monotonic_ns': 2_000_000_000, 'uncertainty_ns': 0}}
            row['metrics'] = {'wall_seconds': 1.0, 'submission_seconds': 0.2, 'business_finished_seconds': 0.3}
            row['operation_evidence'] = verifier.verify(records, workspace=1, artifact_ids=[1, 2, 3],
                request_ids=[11, 12, 13], new_builds=True, page_size=250, record_offset=0)
            row['operation_trace'] = {'path': path.name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                'workspace': 1, 'artifact_ids': [1, 2, 3], 'request_ids': [11, 12, 13],
                'new_builds': True, 'page_size': 250, 'record_offset': 0}
            output_spec = importlib.util.spec_from_file_location('test_image_output_verifier',
                ROOT / 'benchmarks/imagededup_benckmark/src/imagededup_benckmark/output_evidence.py')
            output_verifier = importlib.util.module_from_spec(output_spec)
            output_spec.loader.exec_module(output_verifier)
            manifest = {'schema_version': 1, 'workspace': 1, 'top_k': 10, 'max_distance': 10,
                'artifacts': [{'artifact_id': identity, 'hash_value': '0000000000000000'}
                              for identity in (1, 2, 3)],
                'comparisons': [{'request_id': query + 10, 'query_artifact_id': query,
                    'results': [{'candidate_artifact_id': candidate, 'distance': 0}
                                for candidate in (1, 2, 3) if candidate != query]}
                    for query in (1, 2, 3)]}
            output_path = Path(temporary) / 'output-evidence.json'
            output_path.write_text(json.dumps(manifest))
            row['output_evidence'] = {'path': output_path.name, 'schema_version': 1,
                'sha256': hashlib.sha256(output_path.read_bytes()).hexdigest()}
            base_validation = output_verifier.replay(manifest, row['operation_trace'], 3, row['scenario'])
            row['validation'] = {**base_validation, 'output_digest': hashlib.sha256(
                json.dumps(base_validation, sort_keys=True).encode()).hexdigest()}
            self.assertEqual(replay_image_workload(row, temporary, 3)['completed_builds'], 3)
            # Keep the real admitted trace intact while forging only reported
            # outputs. Aggregate replay must reject every old blind spot.
            for output_field in ('hashes_digest', 'top_k_digests', 'output_digest'):
                forged_output = copy.deepcopy(row)
                forged_output['validation'][output_field] = 'changed-output'
                with self.subTest(output_field=output_field), self.assertRaises(WorkflowMismatch):
                    replay_image_workload(forged_output, temporary, 3)

            for mutation in ('missing_origin', 'wrong_origin', 'reverse_phase', 'late_commit',
                             'late_submission', 'wrong_wall', 'missing_barrier', 'missing_priority_lane',
                             'short_attempt_receipt'):
                with self.subTest(image_receipt=mutation):
                    bad_records = copy.deepcopy(records)
                    bad_row = copy.deepcopy(row)
                    if mutation == 'missing_origin':
                        bad_records[0].pop('native_worker_origin')
                    elif mutation == 'wrong_origin':
                        bad_records[0]['native_worker_origin']['tasks']['images.build']['function'] = 'other'
                    elif mutation == 'reverse_phase':
                        bad_records[1]['timestamp_ns'] = 1_050_000_000
                    elif mutation == 'late_commit':
                        bad_records[1]['business_committed_timestamp_ns'] = 2_050_000_000
                    elif mutation == 'late_submission':
                        bad_row['metrics']['submission_seconds'] = 1.5
                    elif mutation == 'wrong_wall':
                        bad_row['metrics']['wall_seconds'] = 2.0
                    elif mutation == 'missing_barrier':
                        bad_records.pop()
                    elif mutation == 'missing_priority_lane':
                        bad_records[-1]['redis_lanes'].pop()
                    else:
                        bad_records[-1]['observed_attempt_count'] = 5
                    bad_path = Path(temporary) / f'{mutation}.jsonl'
                    bad_path.write_text(''.join(json.dumps(record) + '\n' for record in bad_records))
                    bad_row['operation_trace'].update(path=bad_path.name,
                        sha256=hashlib.sha256(bad_path.read_bytes()).hexdigest())
                    with self.assertRaises((WorkflowMismatch, NativeAdmissionError)):
                        replay_image_workload(bad_row, temporary, 3)
            # Actual DBWorker snapshots use the normalized public backend name.
            db_records = copy.deepcopy(records)
            for record in db_records:
                record['backend'] = 'dbworker'
                if record['event'] == 'quiescence_barrier':
                    record['unfinished_work'] = {'artifact_build_work': 0, 'comparison_work': 0}
            db_path = Path(temporary) / 'dbworker-image.jsonl'
            db_path.write_text(''.join(json.dumps(record) + '\n' for record in db_records))
            db_row = copy.deepcopy(row)
            db_row['backend'] = 'dbworker'
            db_row['operation_trace'].update(path=db_path.name,
                sha256=hashlib.sha256(db_path.read_bytes()).hexdigest())
            self.assertEqual(replay_image_workload(db_row, temporary, 3)['completed_builds'], 3)
            for incorrect_backend in ('dbwork', 'celery'):
                with self.subTest(snapshot_backend=incorrect_backend):
                    forged_backend = copy.deepcopy(db_records)
                    for record in forged_backend:
                        record['backend'] = incorrect_backend
                    db_path.write_text(''.join(json.dumps(record) + '\n' for record in forged_backend))
                    db_row['operation_trace']['sha256'] = hashlib.sha256(db_path.read_bytes()).hexdigest()
                    with self.assertRaisesRegex(WorkflowMismatch, 'backend differs|Unknown image barrier backend'):
                        replay_image_workload(db_row, temporary, 3)
            altered = copy.deepcopy(row)
            altered['operation_evidence']['completed_builds'] = 2
            with self.assertRaises(WorkflowMismatch):
                replay_image_workload(altered, temporary, 3)
            with self.assertRaises(WorkflowMismatch):
                replay_image_workload(row, temporary, 1000)
            forged = copy.deepcopy(records)
            # Counts still match, but a query artifact replaces a real candidate.
            forged[7]['scored_rows'] = [[11, 1], [11, 2]]
            path.write_text(''.join(json.dumps(record) + '\n' for record in forged))
            row['operation_trace']['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaises(WorkflowMismatch):
                replay_image_workload(row, temporary, 3)
            # A redelivery sees another attempt's commit but writes nothing itself.
            duplicate = {**records[0], 'attempt_id': 'stale-redelivery'}
            records[-1]['observed_attempt_count'] = 7
            records.extend([duplicate, {**duplicate, 'event': 'finished', 'state': 'SUCCESS',
                'items_after': 1, 'page_items': 0, 'built_artifact_ids': [],
                'build_accounting': 'transaction_committed_hash_writes',
                'business_committed_timestamp_ns': None, 'timestamp_ns': 1_400_000_000,
                'timestamp': 1.4}])
            path.write_text(''.join(json.dumps(record) + '\n' for record in records))
            row['operation_trace']['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            row['operation_evidence'] = verifier.verify(records, workspace=1, artifact_ids=[1, 2, 3],
                request_ids=[11, 12, 13], new_builds=True, page_size=250, record_offset=0)
            self.assertEqual(replay_image_workload(row, temporary, 3)['duplicate_build_deliveries'], 1)
            forged = copy.deepcopy(records)
            forged[-1]['built_artifact_ids'] = [1]
            path.write_text(''.join(json.dumps(record) + '\n' for record in forged))
            row['operation_trace']['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaises(WorkflowMismatch):
                replay_image_workload(row, temporary, 3)
            records.append(records[0])
            path.write_text(''.join(json.dumps(record) + '\n' for record in records))
            row['operation_trace']['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            with self.assertRaises(WorkflowMismatch):
                replay_image_workload(row, temporary, 3)


if __name__ == '__main__':
    unittest.main()
