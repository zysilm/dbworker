"""Regressions for false associations accepted by function-membership replay."""
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from benchmarks.common.native_admission import NativeAdmissionError
from benchmarks.common.native_binding import BINDINGS, SOURCE_SHA256, validate_task_binding

ROOT = Path(__file__).resolve().parents[2]


def source_root(suite):
    if suite == 'imagededup':
        return ROOT / 'examples/imagededup_system_redis_celery/src'
    root = ROOT / 'examples' / suite
    return root / 'src' if suite == 'sentry' else root


def original_evidence(suite, task):
    relative, _, function, _ = BINDINGS[suite][task]
    return {'source': relative, 'function': function,
            'source_sha256': hashlib.sha256((source_root(suite) / relative).read_bytes()).hexdigest()}


class NativeBindingTests(unittest.TestCase):
    def test_all_pinned_declarations_are_proven_without_application_imports(self):
        for suite, tasks in BINDINGS.items():
            for task in tasks:
                with self.subTest(suite=suite, task=task):
                    validate_task_binding(suite, task, original_evidence(suite, task), source_root(suite))

    def test_saved_native_samples_keep_framework_wrappers_and_exact_binding(self):
        # Real measured samples include genuine Celery autoretry wrappers. The
        # persisted binding gate validates their unwrapped native registration.
        found = set()
        for suite in BINDINGS:
            path = ROOT / 'benchmarks/results/latest' / f'{suite}.json'
            report = json.loads(path.read_text())
            for row in report['runs']:
                if row['backend'] != 'celery':
                    continue
                for name, evidence in row['native_execution']['tasks'].items():
                    validate_task_binding(suite, name, evidence, source_root(suite))
                    if evidence.get('framework_wrappers'):
                        found.add(suite)
        self.assertTrue({'imagededup', 'posthog'}.issubset(found))

    def test_saleor_genuine_export_function_cannot_attest_email_task(self):
        email_task = 'saleor.plugins.admin_email.tasks.send_email_with_link_to_download_file_task'
        export = original_evidence('saleor', 'export-products')
        with self.assertRaises(NativeAdmissionError):
            validate_task_binding('saleor', email_task, export, source_root('saleor'))

    def test_unrelated_genuine_same_file_functions_are_rejected(self):
        for suite, task, unrelated in (
            ('posthog', 'posthog.email._send_email', 'was_email_delivered'),
            ('sentry', 'sentry.tasks.email.send_email', 'process_inbound_email'),
            ('imagededup', 'images.build', 'compare'),
        ):
            with self.subTest(suite=suite):
                evidence = original_evidence(suite, task)
                evidence['function'] = unrelated
                with self.assertRaises(NativeAdmissionError):
                    validate_task_binding(suite, task, evidence, source_root(suite))

    def test_unknown_task_cross_suite_and_unsafe_source_are_rejected(self):
        evidence = original_evidence('saleor', 'export-products')
        for suite, task in (('saleor', 'unknown'), ('superset', 'export-products'), ('unknown', 'export-products')):
            with self.subTest(suite=suite, task=task), self.assertRaises(NativeAdmissionError):
                validate_task_binding(suite, task, evidence, source_root('saleor'))
        for path in ('../saleor/csv/tasks.py', '/saleor/csv/tasks.py', 'saleor/csv/other.py'):
            invalid = {**evidence, 'source': path}
            with self.subTest(path=path), self.assertRaises(NativeAdmissionError):
                validate_task_binding('saleor', 'export-products', invalid, source_root('saleor'))

    def test_rehashed_source_drift_is_not_self_admitted(self):
        with tempfile.TemporaryDirectory() as temporary:
            original = original_evidence('saleor', 'export-products')
            path = Path(temporary) / original['source']
            path.parent.mkdir(parents=True)
            path.write_bytes((source_root('saleor') / original['source']).read_bytes() + b'\n# source drift\n')
            changed = {**original, 'source_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
            with self.assertRaises(NativeAdmissionError):
                validate_task_binding('saleor', 'export-products', changed, Path(temporary))
            # Keeping the reported original digest cannot hide checkout drift.
            with self.assertRaises(NativeAdmissionError):
                validate_task_binding('saleor', 'export-products', original, Path(temporary))

    def test_symlink_escape_is_rejected_even_with_pinned_content(self):
        original = original_evidence('saleor', 'export-products')
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / original['source']
            path.parent.mkdir(parents=True)
            path.symlink_to(source_root('saleor') / original['source'])
            with self.assertRaises(NativeAdmissionError):
                validate_task_binding('saleor', 'export-products', original, Path(temporary))

    def assert_registration_mutation_rejected(self, suite, task, old, new):
        """Test AST logic even after a hypothetical reviewed file-pin update."""
        evidence = original_evidence(suite, task)
        content = (source_root(suite) / evidence['source']).read_text()
        self.assertIn(old, content)
        content = content.replace(old, new, 1)
        digest = hashlib.sha256(content.encode()).hexdigest()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / evidence['source']
            path.parent.mkdir(parents=True)
            path.write_text(content)
            pins = copy.copy(SOURCE_SHA256)
            pins[(suite, evidence['source'])] = digest
            with patch.dict(SOURCE_SHA256, pins), self.assertRaises(NativeAdmissionError):
                validate_task_binding(suite, task, {**evidence, 'source_sha256': digest}, Path(temporary))

    def test_explicit_registered_name_mismatch_is_rejected_by_ast(self):
        self.assert_registration_mutation_rejected('saleor', 'export-products',
                                                  'name="export-products"', 'name="export-products-decoy"')

    def test_default_registered_name_cannot_be_overridden(self):
        task = 'saleor.plugins.admin_email.tasks.send_email_with_link_to_download_file_task'
        self.assert_registration_mutation_rejected(
            'saleor', task,
            '@app.task(compression="zlib")\ndef send_email_with_link_to_download_file_task',
            '@app.task(name="another-task", compression="zlib")\ndef send_email_with_link_to_download_file_task')

    def test_dynamic_registration_cannot_wrap_another_original_function(self):
        self.assert_registration_mutation_rejected(
            'posthog', 'posthog.email._send_email',
            'name="posthog.email._send_email")(_send_email_now)',
            'name="posthog.email._send_email")(was_email_delivered)')

    def test_missing_decorator_is_rejected_by_ast(self):
        self.assert_registration_mutation_rejected('imagededup', 'images.dispatch',
                                                  '@app.task(name="images.dispatch")', '@unrelated(name="images.dispatch")')

    def test_sentry_wrapper_dependency_drift_is_rejected(self):
        evidence = original_evidence('sentry', 'sentry.tasks.email.send_email')
        with tempfile.TemporaryDirectory() as temporary:
            for relative in (evidence['source'], 'sentry/tasks/base.py'):
                path = Path(temporary) / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes((source_root('sentry') / relative).read_bytes())
            wrapper = Path(temporary) / 'sentry/tasks/base.py'
            wrapper.write_bytes(wrapper.read_bytes() + b'\n# wrapper drift\n')
            with self.assertRaises(NativeAdmissionError):
                validate_task_binding('sentry', 'sentry.tasks.email.send_email', evidence, Path(temporary))


if __name__ == '__main__':
    unittest.main()
