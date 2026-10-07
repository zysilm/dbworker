"""Reject synthetic worker commands even when comments claim native origins."""
import unittest
from benchmarks.common.native_admission import NativeAdmissionError, check_worker_source


class NativeAdmissionTests(unittest.TestCase):
    def test_native_worker_literal_is_admitted(self):
        result = check_worker_source("launch(['python','-m','celery','-A','saleor.celeryconf:app','worker'])", 'saleor')
        self.assertTrue(result['passed'])

    def test_comments_and_unused_strings_do_not_prove_worker(self):
        for source in ("# saleor.celeryconf:app", "native='saleor.celeryconf:app'", "launch(['celery','-A', configured_app])",
                       "def unused():\n    launch(['celery','-A','saleor.celeryconf:app','worker'])",
                       "if False:\n    launch(['celery','-A','saleor.celeryconf:app','worker'])",
                       "launch(['celery','-A','saleor.celeryconf:app','beat'])"):
            with self.subTest(source=source), self.assertRaises(NativeAdmissionError):
                check_worker_source(source, 'saleor')

    def test_wrapper_import_and_worker_are_rejected(self):
        for extra in ("from benchmarks.upstream.celery_app import app", "launch(['celery','-A','benchmarks.upstream.celery_app:app'])"):
            with self.subTest(extra=extra), self.assertRaises(NativeAdmissionError):
                check_worker_source("launch(['celery','-A','saleor.celeryconf:app'])\n" + extra, 'saleor')

    def test_actual_command_slice_is_checked_instead_of_unused_decoy(self):
        source = "native=['celery','-A','saleor.celeryconf:app','worker']\nlaunch(['celery','-A','synthetic:app','worker'])"
        with self.assertRaises(NativeAdmissionError):
            check_worker_source(source, 'saleor')
        source = "native=['python','-m','celery','-A','saleor.celeryconf:app','worker']\nlaunch(['celery',*native[3:]])"
        self.assertTrue(check_worker_source(source, 'saleor')['passed'])

    def test_mutated_command_cannot_use_earlier_native_assignment(self):
        for mutation in ("command[2]='synthetic:app'", "command.clear()", "command += ['--app=synthetic:app']"):
            source = "command=['celery','-A','saleor.celeryconf:app','worker']\n" + mutation + "\nlaunch(command)"
            with self.subTest(mutation=mutation), self.assertRaises(NativeAdmissionError):
                check_worker_source(source, 'saleor')

    def test_original_worker_decoy_does_not_admit_eager_or_custom_baseline(self):
        native = "launch(['celery','-A','saleor.celeryconf:app','worker'])\n"
        for operation in ("task.apply()", "task.run()", "app.task(name='replacement')(function)",
                          "from celery import Celery as C\napp=C('replacement')"):
            with self.subTest(operation=operation), self.assertRaises(NativeAdmissionError):
                check_worker_source(native + operation, 'saleor')


if __name__ == '__main__':
    unittest.main()
