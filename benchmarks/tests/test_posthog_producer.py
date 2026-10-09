"""Socket-free tests of the original API publication seam and fixture identity."""
import ast
import hashlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from celery import Celery

from examples.posthog_dbworker.producer import Fixture, identity, route_notification

ROOT = Path(__file__).resolve().parents[2]


class PostHogProducerTests(unittest.TestCase):
    def setUp(self):
        self.app = Celery("posthog-producer-unit", broker="memory://")

        @self.app.task(name="posthog.tasks.email.send_two_factor_auth_enabled_email")
        def task(user_id):
            raise AssertionError("Producer interception must not execute the business body")

        self.task = task
        module = types.ModuleType("posthog.tasks.email")
        module.send_two_factor_auth_enabled_email = task
        self.module_patch = patch.dict(sys.modules, {"posthog.tasks.email": module})
        self.module_patch.start()
        self.addCleanup(self.module_patch.stop)
        self.addCleanup(self.app.close)

    def test_original_delay_produces_one_independent_root_and_restores_task(self):
        submitted = []
        original = self.task.apply_async
        with route_notification(lambda user_id: submitted.append(user_id) or 42, 17):
            result = self.task.delay(17)
            self.assertEqual(result.id, "42")
        self.assertEqual(submitted, [17])
        self.assertEqual(self.task.apply_async, original)

    def test_missing_or_duplicate_original_publication_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "did not publish"):
            with route_notification(lambda user_id: 1, 17):
                pass
        with self.assertRaisesRegex(ValueError, "duplicate"):
            with route_notification(lambda user_id: 1, 17):
                self.task.delay(17)
                self.task.delay(17)

    def test_wrong_user_or_unreviewed_options_are_rejected(self):
        for call in (lambda: self.task.delay(18),
                     lambda: self.task.apply_async(args=(17,), countdown=1),
                     lambda: self.task.apply_async(kwargs={"user_id": 17})):
            with self.subTest(call=call):
                with self.assertRaisesRegex(ValueError, "Unreviewed"):
                    with route_notification(lambda user_id: 1, 17):
                        call()

    def test_endpoint_exception_restores_original_publication(self):
        original = self.task.apply_async
        with self.assertRaisesRegex(RuntimeError, "endpoint"):
            with route_notification(lambda user_id: 1, 17):
                raise RuntimeError("endpoint failed")
        self.assertEqual(self.task.apply_async, original)

    def test_operation_identity_does_not_publish_credentials(self):
        fixture = Fixture(17, "private-current-session", "private-other-session",
                          b"private-totp-secret", "private-csrf-token", "user@benchmark.invalid")
        result = identity(fixture, "notification-0000")
        self.assertEqual(result["user_id"], 17)
        self.assertEqual(result["recipient_sha256"], hashlib.sha256(fixture.recipient.encode()).hexdigest())
        self.assertEqual(set(result), {"operation_id", "user_id", "recipient_sha256", "sha256"})
        self.assertNotEqual(result["sha256"], identity(fixture, "notification-0001")["sha256"])
        self.assertNotIn("private", str(result))

    def test_both_arms_call_original_drf_handler_instead_of_root_task_producer(self):
        backend = ast.parse((ROOT / "benchmarks/upstream/posthog_backend.py").read_text())
        batch = next(node for node in ast.walk(backend) if isinstance(node, ast.FunctionDef) and node.name == "execute_batch")
        calls = [ast.unparse(node.func) for node in ast.walk(batch) if isinstance(node, ast.Call)]
        self.assertEqual(calls.count("producer.validate"), 2)
        self.assertIn("producer.route_notification", calls)
        self.assertNotIn("send_two_factor_auth_enabled_email.apply_async", calls)
        producer = ast.parse((ROOT / "examples/posthog_dbworker/producer.py").read_text())
        validate = next(node for node in producer.body if isinstance(node, ast.FunctionDef) and node.name == "validate")
        source = ast.unparse(validate)
        self.assertIn("UserViewSet.as_view({'post': 'two_factor_validate'})", source)
        self.assertIn("APIRequestFactory(enforce_csrf_checks=True)", source)
        self.assertIn("AuthenticationMiddleware", source)
        self.assertNotIn("force_authenticate", source)
        self.assertIn("totp(fixture.secret)", source)


if __name__ == "__main__":
    unittest.main()
