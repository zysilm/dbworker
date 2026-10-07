"""Exercise live origin admission against replacement apps and wrapped tasks."""
import functools
import importlib.util
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch
from benchmarks.common.native_admission import NativeAdmissionError, check_original_tasks


class NativeOriginTests(unittest.TestCase):
    def test_real_celery_native_autoretry_is_admitted(self):
        try:
            import celery
        except ImportError:
            self.skipTest('Run with a suite Celery interpreter for framework admission')
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'native.py'
            path.write_text('from celery import Celery\napp = Celery("native_fixture")\n'
                            '@app.task(name="original", autoretry_for=(ValueError,), retry_backoff=True)\n'
                            'def original(value):\n    return value + 1\n')
            spec = importlib.util.spec_from_file_location('benchmark_fixture_native', path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            module.app.finalize()
            with patch.dict(sys.modules, benchmark_fixture_native=module):
                result = check_original_tasks(module.app, ['original'], temporary,
                                              expected_application='benchmark_fixture_native:app')
                self.assertEqual(result['tasks']['original']['framework_wrappers'][0]['module'], 'celery.app.autoretry')

    def test_forged_native_filename_and_function_name_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            module = self.fixture(temporary)
            replacement_code = compile('def original(value):\n    return 123\n',
                                       str(Path(temporary) / 'native.py'), 'exec')
            namespace = {}
            exec(replacement_code, namespace)
            module.app.tasks['original'].run = namespace['original']
            with patch.dict(sys.modules, benchmark_fixture_native=module), self.assertRaises(NativeAdmissionError):
                check_original_tasks(module.app, ['original'], temporary,
                                     expected_application='benchmark_fixture_native:app')

    def fixture(self, directory):
        path = Path(directory) / 'native.py'
        path.write_text('def original(value):\n    return value + 1\n')
        spec = importlib.util.spec_from_file_location('benchmark_fixture_native', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.app = types.SimpleNamespace(tasks={'original': types.SimpleNamespace(name='original', run=module.original)})
        return module

    def test_pristine_task_live_origin(self):
        with tempfile.TemporaryDirectory() as temporary:
            module = self.fixture(temporary)
            with patch.dict(sys.modules, benchmark_fixture_native=module):
                result = check_original_tasks(module.app, ['original'], temporary,
                                              expected_application='benchmark_fixture_native:app')
                self.assertEqual(result['tasks']['original']['source'], 'native.py')
                self.assertEqual(result['check'], 'live_task_origin_and_ast')

    def test_wrapper_cannot_hide_behind_functools_wraps(self):
        with tempfile.TemporaryDirectory() as temporary:
            module = self.fixture(temporary)
            @functools.wraps(module.original)
            def replacement(value):
                return module.original(value)
            module.app.tasks['original'].run = replacement
            with patch.dict(sys.modules, benchmark_fixture_native=module), self.assertRaises(NativeAdmissionError):
                check_original_tasks(module.app, ['original'], temporary,
                                     expected_application='benchmark_fixture_native:app')

    def test_lookalike_app_and_empty_tasks_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            module = self.fixture(temporary)
            with patch.dict(sys.modules, benchmark_fixture_native=module):
                with self.assertRaises(NativeAdmissionError):
                    check_original_tasks(types.SimpleNamespace(tasks=module.app.tasks), ['original'], temporary,
                                         expected_application='benchmark_fixture_native:app')
                with self.assertRaises(NativeAdmissionError):
                    check_original_tasks(module.app, [], temporary, expected_application='benchmark_fixture_native:app')


if __name__ == '__main__':
    unittest.main()
