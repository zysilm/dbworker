"""Adversarial admission checks for task count, identity and workflow shape."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from benchmarks.common.workflow_graph import SuccessTraceCursor, WorkflowMismatch, compare_graphs, read_trace, validate_graph


def events(backend='celery', operations=('0', '1')):
    rows = []
    for op in operations:
        for stage, parent in [('notification', None), ('delivery', f'{backend}-{op}-notification')]:
            for phase in ('submitted', 'started', 'succeeded'):
                rows.append({'schema_version': 1, 'backend': backend, 'operation_id': op,
                             'node_id': f'{backend}-{op}-{stage}', 'stage': stage,
                             'parent_id': parent, 'event': phase,
                             'timestamp_ns': {'submitted': 10, 'started': 20, 'succeeded': 50}[phase]
                             + (20 if stage == 'delivery' else 0)})
    return rows


def validate(rows, operations=('0', '1')):
    return validate_graph(rows, operations, {'notification': 1, 'delivery': 1}, [('notification', 'delivery')], warmup_operations=('warmup:0',))


class WorkflowGraphTests(unittest.TestCase):
    def test_incremental_cursor_reads_each_record_once_and_buffers_partial_lines(self):
        rows = events(operations=('0',))
        content = b''.join(json.dumps(row).encode() + b'\n' for row in rows)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'workflow.jsonl'
            cursor = SuccessTraceCursor(path, ['0'])
            cursor.update()
            path.write_bytes(content[:-10])
            with patch('benchmarks.common.workflow_graph.json.loads', wraps=json.loads) as loads:
                cursor.update()
                self.assertEqual(cursor.events, rows[:-1])
                self.assertTrue(cursor.pending)
                for _ in range(100):
                    cursor.update()
                self.assertEqual(loads.call_count, len(rows) - 1)
                with path.open('ab') as stream:
                    stream.write(content[-10:])
                cursor.update()
                self.assertEqual(loads.call_count, len(rows))
            self.assertEqual(cursor.events, rows)
            self.assertFalse(cursor.pending)
            self.assertEqual(cursor.succeeded, 2)
            self.assertEqual(validate(cursor.events, ('0',)), validate(rows, ('0',)))

    def test_incremental_cursor_fails_fast_and_rejects_truncation(self):
        for phase in ('failed', 'retried', 'revoked', 'unknown'):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / 'workflow.jsonl'
                cursor = SuccessTraceCursor(path, ['0'])
                path.write_text(json.dumps({**events()[0], 'event': phase}) + '\n')
                with self.assertRaisesRegex(RuntimeError, 'non-successful'):
                    cursor.update()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'workflow.jsonl'
            path.write_text(json.dumps(events()[0]) + '\n')
            cursor = SuccessTraceCursor(path, ['0'])
            cursor.update()
            path.write_bytes(b'')
            with self.assertRaisesRegex(WorkflowMismatch, 'truncated'):
                cursor.update()
            replacement = Path(temporary) / 'replacement.jsonl'
            replacement.write_text(json.dumps(events()[0]) + '\n')
            path.unlink()
            with self.assertRaisesRegex(WorkflowMismatch, 'disappeared'):
                cursor.update()
            replacement.rename(path)
            with self.assertRaisesRegex(WorkflowMismatch, 'replaced'):
                cursor.update()

    def test_backend_ids_differ_but_business_graph_matches(self):
        self.assertEqual(compare_graphs(validate(events()), validate(events('dbworker')))['nodes_per_backend'], 4)

    def test_two_hundred_jobs_cannot_be_ten_jobs(self):
        operations = tuple(str(n) for n in range(100))
        with self.assertRaises(WorkflowMismatch):
            validate(events(operations=operations[:5]), operations)

    def test_same_outputs_do_not_allow_inline_child_work(self):
        rows = [r for r in events() if r['stage'] == 'notification']
        with self.assertRaises(WorkflowMismatch):
            validate(rows)

    def test_retries_and_duplicate_success_fail_success_only_contract(self):
        rows = events()
        for extra in ('submitted', 'succeeded', 'retried', 'failed'):
            with self.subTest(extra=extra), self.assertRaises(WorkflowMismatch):
                validate(rows + [{**rows[0], 'event': extra}])

    def test_cross_operation_parent_rejected(self):
        rows = events()
        for row in rows:
            if row['operation_id'] == '1' and row['stage'] == 'delivery':
                row['parent_id'] = 'celery-0-notification'
        with self.assertRaises(WorkflowMismatch):
            validate(rows)

    def test_unknown_or_uncorrelated_work_never_disappears(self):
        for override in ({'stage': 'unmapped:business'}, {'operation_id': None}, {'operation_id': 'unexpected'}):
            with self.subTest(override=override), self.assertRaises(WorkflowMismatch):
                validate(events() + [{**events()[0], **override, 'node_id': 'extra'}])

    def test_warmup_exclusion_is_explicit(self):
        self.assertEqual(validate(events() + events(operations=('warmup:0',)))['nodes'], 4)

    def test_malformed_warmup_does_not_hide_missing_or_unknown_work(self):
        for rows in (events(operations=('warmup:0',))[:-1],
                     [{**row, 'stage': 'unmapped:extra'} for row in events(operations=('warmup:0',))]):
            with self.subTest(rows=rows), self.assertRaises(WorkflowMismatch):
                validate(events() + rows)

    def test_mixed_backends_invalid_identity_and_boolean_schema_rejected(self):
        for override in ({'backend': 'dbworker'}, {'operation_id': 0}, {'parent_id': []},
                         {'schema_version': True}, {'node_id': ''}):
            rows = events()
            rows[0].update(override)
            with self.subTest(override=override), self.assertRaises(WorkflowMismatch):
                validate(rows)

    def test_phase_timestamps_are_strict_and_ordered(self):
        for value in (None, True, 0, -1, 1.5, '10'):
            rows = events()
            rows[0]['timestamp_ns'] = value
            with self.subTest(value=value), self.assertRaises(WorkflowMismatch):
                validate(rows)
        rows = events()
        rows[0]['timestamp_ns'] = 21
        with self.assertRaises(WorkflowMismatch):
            validate(rows)

    def test_trace_file_order_is_not_execution_order(self):
        self.assertEqual(validate(list(reversed(events())))['nodes'], 4)

    def test_children_can_run_before_parent_success_but_not_before_start(self):
        rows = events()
        self.assertEqual(validate(rows)['nodes'], 4)
        for row in rows:
            if row['stage'] == 'delivery' and row['event'] == 'submitted':
                row['timestamp_ns'] = 19
        with self.assertRaises(WorkflowMismatch):
            validate(rows)

    def test_warmup_prefix_alone_does_not_allow_exclusion(self):
        with self.assertRaises(WorkflowMismatch):
            validate_graph(events() + events(operations=('warmup:rogue',)),
                           ('0', '1'), {'notification': 1, 'delivery': 1},
                           [('notification', 'delivery')], warmup_operations=('warmup:0',))

    def test_missing_completion_and_truncated_trace_rejected(self):
        with self.assertRaises(WorkflowMismatch):
            validate(events()[:-1])
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'trace.jsonl'
            path.write_text('{"schema_version":1}')
            with self.assertRaises(WorkflowMismatch):
                read_trace(path)

    def test_detached_child_and_cycle_rejected(self):
        rows = events()
        for row in rows:
            if row['stage'] == 'delivery':
                row['parent_id'] = None
        with self.assertRaises(WorkflowMismatch):
            validate(rows)
        rows = events()
        for row in rows:
            if row['stage'] == 'notification':
                row['parent_id'] = f"celery-{row['operation_id']}-delivery"
        with self.assertRaises(WorkflowMismatch):
            validate(rows)


if __name__ == '__main__':
    unittest.main()
