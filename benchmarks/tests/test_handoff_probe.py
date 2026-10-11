"""Transparency and actual spawn checks for optional handoff diagnostics."""
import concurrent.futures
import json
import multiprocessing
import os
import pickle
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import dbworker
from benchmarks.common import handoff_probe as probe


def _spawn_entry(claim):
    """Execute the imported dispatch entrypoint in a fresh spawned interpreter."""
    try:
        probe.execute_in_process(claim)
    except RuntimeError as error:
        return type(error).__name__, str(error)
    raise AssertionError('Uninitialized child unexpectedly executed')


class HandoffProbeTests(unittest.TestCase):
    def test_no_environment_does_not_touch_runtime(self):
        runtime = SimpleNamespace()
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(probe.install(runtime))
        self.assertEqual(vars(runtime), {})

    def test_top_level_entrypoint_is_pickleable_and_spawn_importable(self):
        self.assertIs(pickle.loads(pickle.dumps(probe.execute_in_process)), probe.execute_in_process)
        with concurrent.futures.ProcessPoolExecutor(max_workers=1,
                mp_context=multiprocessing.get_context('spawn')) as pool:
            result = pool.submit(_spawn_entry, dbworker.Claim(32, 'secret-token')).result(timeout=20)
        self.assertEqual(result, ('RuntimeError', 'Handler process has not been initialized'))

    def test_handler_restored_and_execution_body_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'trace.jsonl'
            handler = lambda job, session: 'original-outcome'
            worker = SimpleNamespace(name='jobs', handler=handler)
            job = SimpleNamespace(operation_id='operation-1', stage='delivery')
            body = lambda claim: worker.handler(job, object())
            with patch.dict(os.environ, {probe.ENVIRONMENT: str(path)}), \
                    patch.object(dbworker, '_process_worker', worker), \
                    patch.object(dbworker, '_execute_in_process', side_effect=body) as original:
                self.assertEqual(probe.execute_in_process(dbworker.Claim(32, 'secret-token')), 'original-outcome')
                self.assertEqual(original.call_count, 1)
                self.assertIs(worker.handler, handler)
                original.side_effect = ValueError('business-error')
                with self.assertRaisesRegex(ValueError, 'business-error'):
                    probe.execute_in_process(dbworker.Claim(32, 'secret-token'))
                self.assertIs(worker.handler, handler)
            records = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([record['event'] for record in records[:4]],
                             ['execution_enter', 'handler_enter', 'handler_exit', 'execution_exit'])
            self.assertNotIn('secret-token', path.read_text())
            self.assertEqual(records[1]['operation_id'], 'operation-1')
            self.assertTrue(all(record['monotonic_ns'] > 0 for record in records))

    def test_sampling_batches_and_fast_future_callback(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'trace.jsonl'
            worker = SimpleNamespace(name='jobs')
            claim = dbworker.Claim(32, 'secret-token')
            future = concurrent.futures.Future()
            future.set_result('outcome')
            executor = SimpleNamespace(submit=lambda function, selected: future)
            runtime = SimpleNamespace(_claim_many=lambda worker, limit: [claim],
                _run=lambda worker, handlers, wakeup: handlers.submit(dbworker._execute_in_process, claim))
            with patch.dict(os.environ, {probe.ENVIRONMENT: str(path)}):
                self.assertTrue(probe.install(runtime))
                self.assertTrue(probe.install(runtime))
                self.assertEqual(runtime._claim_many(worker, 2), [claim])
                self.assertIs(runtime._run(worker, executor, None), future)
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([row['event'] for row in rows],
                             ['claim_batch', 'submit_begin', 'submit_end', 'future_done'])
            self.assertNotIn('secret-token', path.read_text())
            self.assertEqual(rows[0]['claims'][0]['source_id'], 32)
            self.assertFalse(probe.sampled(dbworker.Claim(True, 'token')))
            self.assertFalse(probe.sampled(dbworker.Claim(31, 'token')))

    def test_write_failure_preserves_business_and_claim_dispatch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'trace.jsonl'
            handler = lambda job, session: 'business-success'
            worker = SimpleNamespace(name='jobs', handler=handler)
            claim = dbworker.Claim(32, 'token')
            body = lambda selected: worker.handler(SimpleNamespace(operation_id='op', stage='delivery'), None)
            future = concurrent.futures.Future()
            future.set_result('success')
            executor = SimpleNamespace(submit=lambda function, selected: future)
            runtime = SimpleNamespace(_claim_many=lambda worker, limit: [claim],
                _run=lambda worker, handlers, wakeup: handlers.submit(dbworker._execute_in_process, claim))
            with patch.dict(os.environ, {probe.ENVIRONMENT: str(path)}), \
                    patch.object(probe, '_write', side_effect=OSError('sensitive-details')), \
                    patch.object(dbworker, '_process_worker', worker), \
                    patch.object(dbworker, '_execute_in_process', side_effect=body) as original:
                self.assertEqual(probe.execute_in_process(claim), 'business-success')
                original.side_effect = ValueError('original-business-error')
                with self.assertRaisesRegex(ValueError, 'original-business-error'):
                    probe.execute_in_process(claim)
                self.assertIs(worker.handler, handler)
                probe.install(runtime)
                self.assertEqual(runtime._claim_many(worker, 2), [claim])
                self.assertIs(runtime._run(worker, executor, None), future)
            markers = list(Path(directory).glob('trace.jsonl.invalid-*'))
            self.assertEqual(len(markers), 1)
            self.assertNotIn('sensitive-details', markers[0].read_text())
            self.assertIn('OSError', markers[0].read_text())

    def test_posthog_observer_span_includes_after_handler_return(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'trace.jsonl'
            observed = []
            globals_ = {'record': lambda *args, **kwargs: observed.append(args[3]),
                        '__name__': 'examples.posthog_dbworker.runtime'}
            exec("def handler(job, session):\n    record('delivery', 'op', 'node', 'started')\n    return 'outcome'", globals_)
            handler = globals_['handler']
            worker = SimpleNamespace(name='jobs', handler=handler)
            original_record = globals_['record']
            def execute(claim):
                result = worker.handler(SimpleNamespace(operation_id='op', stage='delivery'), None)
                globals_['record']('delivery', 'op', 'node', 'succeeded')
                return result
            with patch.dict(os.environ, {probe.ENVIRONMENT: str(path)}), \
                    patch.object(dbworker, '_process_worker', worker), \
                    patch.object(dbworker, '_execute_in_process', side_effect=execute):
                self.assertEqual(probe.execute_in_process(dbworker.Claim(32, 'token')), 'outcome')
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([span['phase'] for span in rows[-1]['observer_spans']], ['started', 'succeeded'])
            self.assertEqual(observed, ['started', 'succeeded'])
            self.assertIs(globals_['record'], original_record)

    def test_unsampled_child_does_not_wrap_handler(self):
        worker = SimpleNamespace(name='jobs', handler=lambda *args: None)
        original_handler = worker.handler
        with patch.dict(os.environ, {probe.ENVIRONMENT: '/nonexistent/unwritable.jsonl'}), \
                patch.object(dbworker, '_process_worker', worker), \
                patch.object(dbworker, '_execute_in_process', return_value='unchanged'):
            self.assertEqual(probe.execute_in_process(dbworker.Claim(1, 'token')), 'unchanged')
            self.assertIs(worker.handler, original_handler)


if __name__ == '__main__':
    unittest.main()
