"""Evidence validation for sampled handoff phase analysis."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

from benchmarks.diagnostics.analyze_handoff import analyze

MILLISECOND = 1_000_000
WALL_OFFSET = 1_000_000_000_000_000_000


def clock(milliseconds):
    return {'monotonic_ns': milliseconds * MILLISECOND,
            'timestamp_ns': WALL_OFFSET + milliseconds * MILLISECOND}


def fixture(source=32, native=False):
    token = f'{source:064x}'
    identity = {'source_id': source, 'claim_token_sha256': token, 'worker': 'jobs'}
    events = []
    for phase, time, pid in [('submit_begin', 10, 100), ('submit_end', 25, 100),
                             ('execution_enter', 20, 200), ('handler_enter', 30, 200),
                             ('handler_exit', 60, 200), ('execution_exit', 100, 200),
                             ('future_done', 110, 100)]:
        row = {'event': phase, 'pid': pid, 'thread_id': pid, **identity, **clock(time)}
        if phase in ('handler_enter', 'handler_exit'):
            row.update(operation_id=f'operation-{source}' if native else None,
                       stage='notification' if native else None)
        if phase in ('handler_exit', 'execution_exit'):
            row['status'] = 'returned'
        if phase == 'future_done':
            row['cancelled'] = False
        if phase == 'execution_exit':
            row['observer_spans'] = []
            if native:
                row['observer_spans'] = [
                    {'stage': 'notification', 'phase': phase, 'start': clock(start),
                     'end': clock(end), 'duration_ns': (end - start) * MILLISECOND}
                    for phase, start, end in [('started', 40, 42), ('succeeded', 70, 72)]]
        events.append(row)
    events.append({'event': 'claim_batch', 'pid': 100, 'thread_id': 100, 'worker': 'jobs',
                   **clock(6), 'batch_sequence': source, 'limit': 2,
                   'claims': [{'source_id': source, 'claim_token_sha256': token}],
                   'start': clock(1), 'end': clock(5), 'duration_ns': 4 * MILLISECOND})
    return events


def workflow_row(operation, event, milliseconds, pid=200):
    return {'backend': 'dbworker', 'operation_id': operation, 'stage': 'notification',
            'node_id': f'notification:{operation}', 'event': event, 'process_id': pid,
            'timestamp_ns': clock(milliseconds)['timestamp_ns']}


class HandoffAnalysisTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.trace = self.directory / 'handoff.jsonl'

    def run_analysis(self, rows, workflow=None):
        self.trace.write_text(''.join(json.dumps(row) + '\n' for row in rows))
        path = None
        if workflow is not None:
            path = self.directory / 'workflow.jsonl'
            path.write_text(''.join(json.dumps(row) + '\n' for row in workflow))
        return analyze(self.trace, path)

    def test_submission_return_may_follow_child_entry(self):
        result = self.run_analysis(fixture())
        self.assertEqual(result['status'], 'valid')
        self.assertEqual(result['sample_count'], 1)
        self.assertEqual(result['parent_pids'], [100])
        self.assertEqual(result['child_pids'], [200])
        self.assertEqual(result['phases']['submit_call']['median_ms'], 15)
        self.assertEqual(result['phases']['submit_to_execution_entry']['median_ms'], 10)
        self.assertEqual(result['phases']['execution_entry_to_handler_entry']['median_ms'], 10)
        self.assertEqual(result['phases']['handler_exit_to_execution_exit']['median_ms'], 40)
        self.assertEqual(result['phases']['execution_exit_to_future_done']['median_ms'], 10)
        self.assertEqual(result['phases']['claim_batch']['median_ms'], 4)
        self.assertIn('handler_prestart', result['samples'][0]['unavailable_phases'])

    def test_workflow_join_uses_full_previous_success_and_excludes_warmup(self):
        rows = fixture(native=True)
        warm = fixture(64, native=True)
        for row in warm:
            if row['event'] in ('handler_enter', 'handler_exit'):
                row['operation_id'] = 'warmup:operation'
        workflow = [workflow_row('previous', 'succeeded', 9),
                    workflow_row('warmup:operation', 'succeeded', 18),
                    workflow_row('operation-32', 'started', 41),
                    workflow_row('operation-32', 'succeeded', 71)]
        result = self.run_analysis(rows + warm, workflow)
        self.assertEqual(result['status'], 'valid')
        self.assertEqual(result['excluded_warmup_samples'], 1)
        self.assertEqual(result['stage_mix'], {'notification': 1})
        self.assertEqual(result['phases']['handler_prestart']['median_ms'], 10)
        self.assertEqual(result['phases']['observer_started_write']['median_ms'], 2)
        self.assertEqual(result['phases']['observer_succeeded_write']['median_ms'], 2)
        self.assertEqual(result['phases']['previous_success_to_execution_entry']['median_ms'], 11)
        self.assertEqual(result['matched_claim_batch_count'], 1)

    def test_incomplete_failed_sample_keeps_explicitly_partial_statistics(self):
        bad = [row for row in fixture(64) if row['event'] != 'future_done']
        result = self.run_analysis(fixture() + bad)
        self.assertEqual(result['status'], 'invalid')
        self.assertEqual(result['sample_count'], 1)
        self.assertEqual(result['invalid_sample_count'], 1)
        self.assertEqual(result['phases']['submit_call']['count'], 1)
        self.assertIn('required phases', result['invalid_samples'][0]['errors'][0])

    def test_negative_sampled_source_ids_preserve_probe_sampling_contract(self):
        rows = fixture()
        for row in rows:
            if row['event'] == 'claim_batch':
                row['claims'][0]['source_id'] = -32
            else:
                row['source_id'] = -32
        result = self.run_analysis(rows)
        self.assertEqual(result['status'], 'valid')
        self.assertEqual(result['samples'][0]['source_id'], -32)

    def test_sampled_batch_identity_without_any_phases_is_invalid(self):
        result = self.run_analysis(fixture() + [fixture(64)[-1]])
        self.assertEqual(result['status'], 'invalid')
        self.assertEqual(result['sample_count'], 1)
        self.assertEqual(result['invalid_sample_count'], 1)
        self.assertEqual(result['invalid_samples'][0]['source_id'], 64)
        self.assertIn('required phases', result['invalid_samples'][0]['errors'][0])

    def test_duplicate_phase_and_wrong_parent_or_child_are_invalid(self):
        for mutation in ('duplicate', 'parent', 'child'):
            with self.subTest(mutation=mutation):
                rows = fixture()
                if mutation == 'duplicate':
                    rows.append(copy.deepcopy(rows[0]))
                elif mutation == 'parent':
                    rows[1]['pid'] = 300
                else:
                    rows[3]['pid'] = 100
                self.assertEqual(self.run_analysis(rows)['status'], 'invalid')

    def test_identity_batch_membership_and_duration_are_required(self):
        for mutation in ('token', 'membership', 'duration', 'limit', 'sequence'):
            with self.subTest(mutation=mutation):
                rows = fixture()
                if mutation == 'token':
                    rows[2]['claim_token_sha256'] = 'not-a-sha256'
                elif mutation == 'membership':
                    rows.pop()
                elif mutation == 'duration':
                    rows[-1]['duration_ns'] += 1
                elif mutation == 'limit':
                    rows[-1]['limit'] = 0
                else:
                    rows[-1]['batch_sequence'] = 0
                self.assertEqual(self.run_analysis(rows)['status'], 'invalid')

    def test_clock_mismatch_and_reversed_execution_are_invalid(self):
        rows = fixture()
        rows[2]['timestamp_ns'] += 20 * MILLISECOND
        self.assertEqual(self.run_analysis(rows)['status'], 'invalid')
        rows = fixture()
        rows[3].update(clock(19))
        self.assertEqual(self.run_analysis(rows)['status'], 'invalid')

    def test_business_errors_and_cancellation_do_not_silently_pass(self):
        for phase, field, value in [('handler_exit', 'status', 'error'),
                                    ('execution_exit', 'status', 'error'),
                                    ('future_done', 'cancelled', True)]:
            rows = fixture()
            next(row for row in rows if row['event'] == phase)[field] = value
            result = self.run_analysis(rows)
            self.assertEqual(result['status'], 'invalid')
            self.assertEqual(result['sample_count'], 0)

    def test_marker_or_stderr_sentinel_invalidates_without_log_disclosure(self):
        marker = Path(str(self.trace) + '.invalid-100')
        marker.write_text('Private marker content')
        result = self.run_analysis(fixture())
        self.assertEqual(result['status'], 'invalid')
        self.assertNotIn('Private marker content', json.dumps(result))
        marker.unlink()
        (self.directory / 'worker.log').write_text('Sensitive prefix DBWORKER_HANDOFF_DIAGNOSTIC_INVALID suffix')
        result = self.run_analysis(fixture())
        self.assertEqual(result['status'], 'invalid')
        self.assertNotIn('Sensitive prefix', json.dumps(result))

    def test_malformed_json_or_span_container_returns_invalid_summary(self):
        self.run_analysis(fixture())
        with self.trace.open('a') as stream:
            stream.write('{broken\n')
        result = analyze(self.trace)
        self.assertEqual(result['status'], 'invalid')
        self.assertEqual(result['sample_count'], 1)
        rows = fixture()
        rows[5]['observer_spans'] = ['malformed']
        self.assertEqual(self.run_analysis(rows)['status'], 'invalid')
        rows = fixture(native=True)
        rows[5]['observer_spans'][0]['start'] = None
        self.assertEqual(self.run_analysis(rows)['status'], 'invalid')

    def test_workflow_missing_or_outside_observer_is_invalid(self):
        for workflow in ([workflow_row('operation-32', 'started', 41)],
                         [workflow_row('operation-32', 'started', 43),
                          workflow_row('operation-32', 'succeeded', 71)]):
            result = self.run_analysis(fixture(native=True), workflow)
            self.assertEqual(result['status'], 'invalid')

    def test_statistics_aggregate_distinct_attempts(self):
        rows = fixture()
        second = fixture(64)
        second[1].update(clock(29))
        result = self.run_analysis(rows + second)
        metric = result['phases']['submit_call']
        self.assertEqual(result['status'], 'valid')
        self.assertEqual(metric['count'], 2)
        self.assertEqual(metric['median_ms'], 17)
        self.assertAlmostEqual(metric['p95_ms'], 18.8)

    def test_missing_trace_is_invalid_and_serializable(self):
        result = analyze(self.trace)
        self.assertEqual(result['status'], 'invalid')
        self.assertEqual(result['sample_count'], 0)
        json.dumps(result, allow_nan=False)


if __name__ == '__main__':
    unittest.main()
