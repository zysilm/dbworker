"""Replay admission rejects altered artifacts and reduced measured workloads."""
import copy
import hashlib
import json
import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from benchmarks.common.native_admission import NativeAdmissionError
from benchmarks.common.performance_admission import (
    ROOT, TASK_STAGES, replay_graph, replay_image_workload, validate_image_workload,
    validate_native_report, validate_native_sample,
)
from benchmarks.common.workflow_graph import WorkflowMismatch, validate_graph


class PerformanceAdmissionTests(unittest.TestCase):
    def graph_fixture(self, directory, backend='celery'):
        events = []
        for op in ('warmup:0', 'warmup:1', '0', '1'):
            for task, stage in TASK_STAGES['posthog'].items():
                parent = f'{backend}-{op}-notification' if stage == 'delivery' else None
                for phase in ('submitted', 'started', 'succeeded'):
                    events.append({'schema_version': 1, 'backend': backend, 'operation_id': op,
                                   'node_id': f'{backend}-{op}-{stage}', 'stage': stage,
                                   'parent_id': parent, 'event': phase, 'details': {'task_name': task}})
        path = Path(directory) / f'{backend}.jsonl'
        path.write_text(''.join(json.dumps(event) + '\n' for event in events))
        return {'backend': backend, 'scenario': 'native_two_factor_notification', 'status': 'passed',
                'repetition': 1, 'validation': {'passed': True},
                'workflow_trace': {'path': path.name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()},
                'workflow_graph': validate_graph(events, ['0', '1'], {'notification': 1, 'delivery': 1},
                                                 [('notification', 'delivery')])}

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
                  'configuration': {'images': 3, 'repetitions': 1}, 'runs': rows}

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
                             'task_id': f'original-{stage}-{identity}', 'items': 0, 'event': 'started'}
                    finish = {**start, 'event': 'finished', 'state': 'SUCCESS',
                              'items_after': width, 'page_items': width}
                    if stage == 'build':
                        finish.update(build_accounting='transaction_committed_hash_writes',
                                      built_artifact_ids=[identity])
                    if stage == 'comparison':
                        # Continuations may finish before the current task's callback.
                        finish['items_after'] = 99
                        finish.update(page_accounting='transaction_committed_orm_inserts',
                                      scored_rows=[[identity, candidate] for candidate in (1, 2, 3)
                                                   if candidate != identity - 10])
                    records.extend([start, finish])
            path = Path(temporary) / 'image.jsonl'
            path.write_text(''.join(json.dumps(record) + '\n' for record in records))
            spec = importlib.util.spec_from_file_location('test_image_verifier',
                ROOT / 'benchmarks/imagededup_benckmark/src/imagededup_benckmark/evidence.py')
            verifier = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(verifier)
            row = self.image_fixture()
            row['backend'] = 'celery'
            row['operation_evidence'] = verifier.verify(records, workspace=1, artifact_ids=[1, 2, 3],
                request_ids=[11, 12, 13], new_builds=True, page_size=250, record_offset=0)
            row['operation_trace'] = {'path': path.name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                'workspace': 1, 'artifact_ids': [1, 2, 3], 'request_ids': [11, 12, 13],
                'new_builds': True, 'page_size': 250, 'record_offset': 0}
            self.assertEqual(replay_image_workload(row, temporary, 3)['completed_builds'], 3)
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
            records.extend([duplicate, {**duplicate, 'event': 'finished', 'state': 'SUCCESS',
                'items_after': 1, 'page_items': 0, 'built_artifact_ids': [],
                'build_accounting': 'transaction_committed_hash_writes'}])
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
