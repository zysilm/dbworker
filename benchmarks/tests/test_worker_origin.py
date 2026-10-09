"""Worker attestation follows the actual registry and callable identity."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from benchmarks.common import native_observer as observer


class WorkerOriginTests(unittest.TestCase):
    def setUp(self):
        observer._worker_origins.clear()

    def task(self):
        def original(value):
            return value
        app = SimpleNamespace(tasks={})
        task = SimpleNamespace(name='sentry.tasks.email.send_email', run=original, app=app)
        app.tasks[task.name] = task
        return task

    def test_cache_is_process_app_and_callable_specific(self):
        task = self.task()
        with patch('benchmarks.common.native_admission.check_original_tasks', return_value={'passed': True}) as check:
            first = observer.worker_origin(task, 'sentry')
            self.assertIs(observer.worker_origin(task, 'sentry'), first)
            self.assertEqual(check.call_count, 1)
            with patch('benchmarks.common.native_observer.os.getpid', return_value=987654):
                observer.worker_origin(task, 'sentry')
            self.assertEqual(check.call_count, 2)
            replacement = self.task()
            observer.worker_origin(replacement, 'sentry')
            self.assertEqual(check.call_count, 3)
            self.assertEqual(check.call_args.kwargs['expected_application'], 'sentry.celery:app')
            self.assertEqual(check.call_args.args[1], [replacement.name])

    def test_same_function_changed_code_cannot_reuse_cached_attestation(self):
        task = self.task()
        def replaced(value):
            return 'changed'
        with patch('benchmarks.common.native_admission.check_original_tasks', return_value={'passed': True}) as check:
            observer.worker_origin(task, 'sentry')
            task.run.__code__ = replaced.__code__
            observer.worker_origin(task, 'sentry')
            self.assertEqual(check.call_count, 2)

    def test_changed_wrapper_inner_code_invalidates_cache(self):
        task = self.task()
        inner = task.run
        def wrapper(value):
            return inner(value)
        wrapper.__wrapped__ = inner
        task.run = wrapper
        with patch('benchmarks.common.native_admission.check_original_tasks', return_value={'passed': True}) as check:
            observer.worker_origin(task, 'sentry')
            def replacement(value):
                return 'changed'
            inner.__code__ = replacement.__code__
            observer.worker_origin(task, 'sentry')
            self.assertEqual(check.call_count, 2)

    def test_changed_autoretry_original_run_invalidates_cache(self):
        task = self.task()
        task._orig_run = task.run
        with patch('benchmarks.common.native_admission.check_original_tasks', return_value={'passed': True}) as check:
            observer.worker_origin(task, 'sentry')
            def replacement(value):
                return 'changed'
            task._orig_run = replacement
            observer.worker_origin(task, 'sentry')
            self.assertEqual(check.call_count, 2)
            task._orig_run.__code__ = task.run.__code__
            observer.worker_origin(task, 'sentry')
            self.assertEqual(check.call_count, 3)

    def test_changed_registry_entry_rejected_before_cached_verdict(self):
        task = self.task()
        with patch('benchmarks.common.native_admission.check_original_tasks', return_value={'passed': True}) as check:
            observer.worker_origin(task, 'sentry')
            task.app.tasks[task.name] = self.task()
            with self.assertRaisesRegex(ValueError, 'registry'):
                observer.worker_origin(task, 'sentry')
            self.assertEqual(check.call_count, 1)

    def test_observer_persists_failed_proof_without_claiming_success(self):
        task = self.task()
        with patch.object(observer, 'worker_origin', side_effect=ValueError('bad registry')):
            proof = observer._observed_worker_origin(task)
        self.assertNotIn('native_worker_origin', proof)
        self.assertIn('bad registry', proof['native_worker_origin_error'])


if __name__ == '__main__':
    unittest.main()
