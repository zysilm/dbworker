"""Task fingerprints preserve transport values without exposing payloads."""
import dataclasses
import json
import pickle
import unittest
from collections import namedtuple
from pathlib import Path
from types import SimpleNamespace

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


if __name__ == '__main__':
    unittest.main()
