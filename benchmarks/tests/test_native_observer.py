"""Exercise publication correlation before transport and concurrent trace reads."""
import copy
import fcntl
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from benchmarks.common import native_observer as observer
from benchmarks.common.workflow_graph import read_trace, validate_graph


class NativeObserverTests(unittest.TestCase):
    def test_real_receipts_identify_recording_process_without_changing_payload(self):
        with tempfile.TemporaryDirectory() as temporary:
            trace = Path(temporary) / 'trace.jsonl'
            arguments = {'task_name': 'original.delivery', 'argument_sha256': 'unchanged',
                         'native_worker_origin': {'source': 'original'}}
            expected_arguments = copy.deepcopy(arguments)
            with patch.dict(os.environ, self.environment(trace)):
                for phase in ('submitted', 'started', 'succeeded'):
                    observer.record('delivery', 'operation', 'delivery-0', phase,
                                    'notification-0', **arguments)
            events = read_trace(trace)
            self.assertEqual([row['event'] for row in events], ['submitted', 'started', 'succeeded'])
            self.assertEqual(arguments, expected_arguments)
            for row in events:
                self.assertEqual(row['schema_version'], 1)
                self.assertEqual(row['process_id'], os.getpid())
                self.assertIs(type(row['timestamp_ns']), int)
                self.assertGreater(row['timestamp_ns'], 0)
                self.assertEqual(row['details'], expected_arguments)
                self.assertEqual(row['parent_id'], 'notification-0')
            self.assertEqual([row['timestamp_ns'] for row in events],
                             sorted(row['timestamp_ns'] for row in events))

    def environment(self, path):
        return {'BENCHMARK_TRACE_PATH': str(path), 'BENCHMARK_BACKEND': 'celery',
                'BENCHMARK_TASK_STAGES': json.dumps({'original.delivery': 'delivery'})}

    def test_correlated_headers_skip_trace_lookup_and_preserve_native_lifecycle(self):
        for header_parent, request_parent, expected_parent in (
                (None, None, None), ('explicit-parent', 'request-parent', 'explicit-parent'),
                (None, 'request-parent', 'request-parent')):
            with self.subTest(parent=expected_parent), tempfile.TemporaryDirectory() as temporary:
                trace = Path(temporary) / 'trace.jsonl'
                headers = {'benchmark_operation': 'operation'}
                if header_parent is not None:
                    headers['benchmark_parent'] = header_parent
                task = SimpleNamespace(name='original.delivery',
                                       request=SimpleNamespace(headers=headers, parent_id=request_parent))
                arguments, keywords = ([42], {'recipient': 'unit@example.invalid'})
                payload = copy.deepcopy((headers, arguments, keywords))
                origin = {'native_worker_origin': {'source': 'original'}}
                with patch.dict(os.environ, self.environment(trace)), \
                        patch.object(observer, '_context', side_effect=AssertionError('Unnecessary trace lookup')), \
                        patch.object(observer, '_observed_worker_origin', return_value=origin):
                    observer._started(task=task, task_id='delivery-0', args=arguments, kwargs=keywords)
                    self.assertEqual(observer._current.get(), 'operation')
                    self.assertEqual(observer._parent.get(), 'delivery-0')
                    observer._finished(task=task, task_id='delivery-0', state='SUCCESS')
                self.assertEqual((headers, arguments, keywords), payload)
                self.assertIsNone(observer._current.get())
                self.assertIsNone(observer._parent.get())
                events = read_trace(trace)
                self.assertEqual([row['event'] for row in events], ['started', 'succeeded'])
                for row in events:
                    self.assertEqual(row['operation_id'], 'operation')
                    self.assertEqual(row['parent_id'], expected_parent)
                    self.assertEqual(row['details']['task_name'], 'original.delivery')
                self.assertEqual(events[0]['details']['argument_sha256'],
                                 observer.argument_digest(arguments, keywords))
                self.assertEqual(events[0]['details']['native_worker_origin'], origin['native_worker_origin'])
                self.assertEqual(events[1]['details']['state'], 'SUCCESS')

    def test_protocol_one_worker_can_start_inside_publish_before_after_signal(self):
        from celery import Celery, signals
        from kombu import Producer
        app = Celery('publication-race-test', broker='memory://')
        app.conf.update(task_protocol=1, task_serializer='pickle', accept_content=['pickle'])
        @app.task(name='original.delivery', shared=False)
        def fixture(value):
            return value
        delivered, after = [], []

        def published(sender=None, **kwargs):
            after.append(sender)

        def fast_transport(producer, body, *args, **kwargs):
            # A real Celery apply_async path calls this before after_task_publish.
            # Simulate a worker that already received the unchanged protocol-1
            # envelope before the producer returns from transport publication.
            self.assertEqual(len(after), len(delivered))
            unchanged = copy.deepcopy(body)
            task = SimpleNamespace(name=body['task'], request=SimpleNamespace(headers=None, parent_id=None))
            with observer.operation('worker-unrelated-context'):
                observer._started(task_id=body['id'], task=task, args=body['args'], kwargs=body['kwargs'])
                observer._finished(task_id=body['id'], task=task, state='SUCCESS')
            self.assertEqual(body, unchanged)
            self.assertNotIn('benchmark_operation', kwargs.get('headers', {}))
            delivered.append(body['id'])

        signals.after_task_publish.connect(published, weak=False)
        try:
            with tempfile.TemporaryDirectory() as temporary:
                trace = Path(temporary) / 'trace.jsonl'
                with patch.dict(os.environ, self.environment(trace)), patch.object(Producer, 'publish', fast_transport):
                    observer.install()
                    for index in range(2):
                        with observer.operation('operation'):
                            fixture.apply_async(args=(index,), task_id=f'delivery-{index}')
                    graph = validate_graph(read_trace(trace), ['operation'], {'delivery': 2}, [])
                    self.assertEqual(graph['nodes'], 2)
                    events = read_trace(trace)
                    for identity in delivered:
                        phases = [event for event in events if event['node_id'] == identity]
                        self.assertEqual(phases[0]['details']['argument_sha256'],
                                         phases[1]['details']['argument_sha256'])
                    self.assertNotEqual(events[0]['details']['argument_sha256'],
                                        events[3]['details']['argument_sha256'])
                    self.assertEqual(delivered, ['delivery-0', 'delivery-1'])
                    self.assertEqual(len(after), 2)
        finally:
            signals.after_task_publish.disconnect(published)
            app.close()

    def test_context_cache_resets_for_changed_path_inode_and_truncation(self):
        with tempfile.TemporaryDirectory() as temporary:
            first, second = Path(temporary) / 'first.jsonl', Path(temporary) / 'second.jsonl'
            for path, op in ((first, 'first'), (second, 'second')):
                with patch.dict(os.environ, self.environment(path)):
                    observer.record('delivery', op, 'reused-id', 'submitted')
                    self.assertEqual(observer._context('reused-id'), (op, None))
            replacement = second.with_suffix('.replacement')
            with patch.dict(os.environ, self.environment(replacement)):
                observer.record('delivery', 'replacement', 'reused-id', 'submitted')
            replacement.replace(second)
            with patch.dict(os.environ, self.environment(second)):
                self.assertEqual(observer._context('reused-id'), ('replacement', None))
                second.write_bytes(b'')
                self.assertEqual(observer._context('reused-id'), (None, None))

    def test_readers_lock_before_reading_an_in_progress_append(self):
        for reader in ('read_trace', 'context'):
            with self.subTest(reader=reader), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / 'trace.jsonl'
                row = {'schema_version': 1, 'backend': 'celery', 'stage': 'delivery',
                       'operation_id': 'operation', 'node_id': 'delivery-0', 'parent_id': None,
                       'event': 'submitted'}
                payload = json.dumps(row).encode()
                acquired, done = threading.Event(), threading.Event()
                results, errors = [], []
                original = fcntl.flock
                with path.open('wb') as writer:
                    original(writer.fileno(), fcntl.LOCK_EX)
                    writer.write(payload[:-1])
                    writer.flush()

                    def lock(descriptor, mode):
                        if mode == fcntl.LOCK_SH:
                            acquired.set()
                        return original(descriptor, mode)

                    def read():
                        try:
                            results.append(read_trace(path) if reader == 'read_trace' else observer._context('delivery-0'))
                        except BaseException as error:
                            errors.append(error)
                        finally:
                            done.set()

                    with patch.dict(os.environ, self.environment(path)), patch.object(fcntl, 'flock', lock):
                        thread = threading.Thread(target=read)
                        thread.start()
                        self.assertTrue(acquired.wait(2))
                        self.assertFalse(done.is_set())
                        writer.write(payload[-1:] + b'\n')
                        writer.flush()
                        original(writer.fileno(), fcntl.LOCK_UN)
                        thread.join(2)
                        self.assertFalse(thread.is_alive())
                        self.assertEqual(errors, [])
                        self.assertEqual(results, [[row]] if reader == 'read_trace' else [('operation', None)])

    def test_malformed_complete_and_partial_records_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'trace.jsonl'
            with patch.dict(os.environ, self.environment(path)):
                for payload in ('{broken}\n', '[]\n', '{"event":"submitted"}'):
                    path.write_text(payload)
                    for reader in (lambda: read_trace(path), lambda: observer._context('id')):
                        with self.subTest(payload=payload), self.assertRaises(ValueError):
                            reader()


if __name__ == '__main__':
    unittest.main()
