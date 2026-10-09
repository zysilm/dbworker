"""Task fingerprints preserve transport values without exposing payloads."""
import dataclasses
import json
import pickle
import os
import tempfile
import unittest
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from benchmarks.common.argument_evidence import argument_digest


@dataclasses.dataclass
class Payload:
    location: Path
    values: list


class ArgumentEvidenceTests(unittest.TestCase):
    def test_json_transport_and_mapping_order_preserve_values(self):
        args, kwargs = [(1, 'value'), {'nested': [True, None, 2.5]}], {'z': 2, 'a': 1}
        decoded = json.loads(json.dumps([args, kwargs]))
        self.assertEqual(argument_digest(args, kwargs), argument_digest(*decoded))
        self.assertEqual(argument_digest(args, kwargs), argument_digest(args, {'a': 1, 'z': 2}))
        decoded[0][1]['nested'][0] = False
        self.assertNotEqual(argument_digest(args, kwargs), argument_digest(*decoded))

    def test_pickle_dataclass_path_and_bytes_preserve_values(self):
        value = Payload(Path('/tmp/native-input'), [b'bytes', 3])
        args, kwargs = [value], {'metadata': {'count': 2}}
        self.assertEqual(argument_digest(args, kwargs), argument_digest(*pickle.loads(pickle.dumps((args, kwargs)))))
        changed = Payload(Path('/tmp/changed-input'), [b'bytes', 3])
        self.assertNotEqual(argument_digest(args, kwargs), argument_digest([changed], kwargs))
        self.assertNotEqual(argument_digest([b'bytes'], {}), argument_digest(['bytes'], {}))
        self.assertNotEqual(argument_digest([True], {}), argument_digest([1], {}))

    def test_namedtuple_fields_and_set_order(self):
        Point = namedtuple('Point', 'x y')
        self.assertEqual(argument_digest([Point(1, 2), {1, 2}], {}),
                         argument_digest([Point(1, 2), {2, 1}], {}))
        self.assertNotEqual(argument_digest([Point(1, 2)], {}), argument_digest([[1, 2]], {}))

    def test_cycles_unknown_objects_nonfinite_and_bad_shape_fail(self):
        cycle = []
        cycle.append(cycle)
        cyclic_mapping = {}
        cyclic_mapping['self'] = cyclic_mapping
        for value in (cycle, cyclic_mapping, SimpleNamespace(value=1), float('nan'), float('inf')):
            with self.subTest(value_type=type(value).__name__), self.assertRaises(ValueError):
                argument_digest([value], {})
        for args, kwargs in (({}, {}), ([], []), (None, {})):
            with self.assertRaises(ValueError):
                argument_digest(args, kwargs)

    def test_django_email_pickle_when_dependency_available(self):
        try:
            from django.core.mail import EmailMultiAlternatives
        except ImportError:
            self.skipTest('Django is an application dependency, not an admission-runner dependency')
        mail = EmailMultiAlternatives('Subject', 'Body', 'sender@example.test', ['recipient@example.test'])
        mail.attach_alternative('<p>Body</p>', 'text/html')
        self.assertEqual(argument_digest([mail], {}), argument_digest([pickle.loads(pickle.dumps(mail))], {}))
        modified = pickle.loads(pickle.dumps(mail))
        modified.to = ['changed@example.test']
        self.assertNotEqual(argument_digest([mail], {}), argument_digest([modified], {}))

    def test_original_rendered_safe_html_preserves_class_and_content(self):
        try:
            from django.conf import settings
            from django.template.loader import render_to_string
            from django.test import override_settings
            from django.utils.safestring import SafeString, mark_safe
        except ImportError:
            self.skipTest('Django is an application dependency')
        if not settings.configured:
            settings.configure(SECRET_KEY='argument-evidence-unit-only', INSTALLED_APPS=[])
        import django
        django.setup()
        templates = [{'BACKEND': 'django.template.backends.django.DjangoTemplates',
                      'OPTIONS': {'loaders': [('django.template.loaders.locmem.Loader',
                                              {'native-email.html': '<p>{{ body }}</p>'})]}}]
        with override_settings(TEMPLATES=templates):
            html = render_to_string('native-email.html', {'body': 'café & tea'})
        self.assertIs(type(html), SafeString)
        self.assertEqual(html, '<p>café &amp; tea</p>')
        restored = pickle.loads(pickle.dumps(html))
        self.assertIs(type(restored), SafeString)
        self.assertEqual(argument_digest([html], {}), argument_digest([restored], {}))
        self.assertNotEqual(argument_digest([html], {}),
                            argument_digest([str.__str__(html)], {}))
        self.assertNotEqual(argument_digest([html], {}),
                            argument_digest([mark_safe('<p>Changed</p>')], {}))
        class UnknownText(str):
            pass
        class UnknownSafeText(SafeString):
            pass
        for unsupported in (UnknownText('value'), UnknownSafeText('value')):
            with self.assertRaisesRegex(ValueError, 'Unsupported task argument type'):
                argument_digest([unsupported], {})

    def test_rendered_email_original_publication_and_worker_signals_match(self):
        try:
            from django.conf import settings
            from django.core.mail import EmailMultiAlternatives
            from django.template.loader import render_to_string
            from django.test import override_settings
        except ImportError:
            self.skipTest('Django is an application dependency')
        from celery import Celery, signals
        from benchmarks.common import native_observer as observer
        if not settings.configured:
            settings.configure(SECRET_KEY='argument-evidence-unit-only', INSTALLED_APPS=[])
        import django
        django.setup()
        templates = [{'BACKEND': 'django.template.backends.django.DjangoTemplates',
                      'OPTIONS': {'loaders': [('django.template.loaders.locmem.Loader',
                                              {'native-email.html': '<p>{{ body }}</p>'})]}}]
        with override_settings(TEMPLATES=templates):
            html = render_to_string('native-email.html', {'body': 'café & tea'})
        message = EmailMultiAlternatives('Native fixture', 'café & tea',
            'sender@benchmark.invalid', ['recipient@benchmark.invalid'],
            headers={'X-Benchmark': 'warmup:-2'})
        message.attach_alternative(html, 'text/html')
        kwargs = {'message': message, '__start_time': 1700000000.25}
        body = {'id': 'native-email-safe-html', 'task': 'sentry.tasks.email.send_email',
                'args': (), 'kwargs': kwargs}
        restored = pickle.loads(pickle.dumps(body))
        app = Celery('native-email-safe-html-test', broker='memory://', set_as_current=False)
        @app.task(name=body['task'], shared=False)
        def registered_task(message, __start_time):
            raise AssertionError('Signal correctness must not execute a replacement task body')
        task = app.tasks[body['task']]
        proof = {'test_original_worker_proof_acquisition': True}
        with tempfile.TemporaryDirectory() as directory:
            trace = Path(directory, 'workflow.jsonl')
            environment = {'BENCHMARK_TRACE_PATH': str(trace), 'BENCHMARK_BACKEND': 'celery',
                           'BENCHMARK_TASK_STAGES': json.dumps({task.name: 'delivery'})}
            observed_signals = (signals.before_task_publish, signals.after_task_publish,
                                signals.task_prerun, signals.task_postrun)
            receiver_snapshots = [list(signal.receivers) for signal in observed_signals]
            observer.install()
            try:
                with patch.dict(os.environ, environment), \
                        patch.object(observer, 'worker_origin', return_value=proof):
                    with observer.operation('warmup:-2'):
                        published = signals.before_task_publish.send(sender=task.name, body=body, headers={})
                    task.push_request(id=body['id'], headers={})
                    try:
                        started = signals.task_prerun.send(sender=task, task=task,
                            task_id=restored['id'], args=restored['args'], kwargs=restored['kwargs'])
                    finally:
                        task.pop_request()
                for responses in (published, started):
                    self.assertTrue(any(receiver == observer._publish or receiver == observer._started
                                        for receiver, _ in responses))
                    self.assertFalse(any(isinstance(response, Exception) for _, response in responses))
                rows = [json.loads(line) for line in trace.read_text().splitlines()]
                self.assertEqual([row['event'] for row in rows], ['submitted', 'started'])
                expected = argument_digest((), kwargs)
                self.assertEqual([row['details']['argument_sha256'] for row in rows], [expected, expected])
                self.assertEqual([row['operation_id'] for row in rows], ['warmup:-2', 'warmup:-2'])
                self.assertEqual(rows[1]['details']['native_worker_origin'], proof)
                changed = pickle.loads(pickle.dumps(restored))
                changed['kwargs']['message'].to = ['changed@benchmark.invalid']
                self.assertNotEqual(expected, argument_digest(changed['args'], changed['kwargs']))
            finally:
                # Other tests may already have installed production observers.
                # Restore their registrations instead of disconnecting them.
                for signal, receivers in zip(observed_signals, receiver_snapshots):
                    with signal.lock:
                        signal.receivers[:] = receivers
                        signal.sender_receivers_cache.clear()
                observer._current.set(None)
                observer._parent.set(None)
                app.close()


if __name__ == '__main__':
    unittest.main()
