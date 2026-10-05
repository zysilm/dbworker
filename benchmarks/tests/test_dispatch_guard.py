import unittest

from celery import Celery
from examples.dbworker_integration.runtime import UnexpectedTaskDispatch, forbid_task_dispatch
from kombu import Producer


class DispatchGuardTests(unittest.TestCase):
    def test_rejects_broker_publication_and_eager_tasks_and_restores_methods(self):
        original = Producer.publish
        app = Celery("guard-test")

        @app.task
        def local_task():
            return "executed"

        with forbid_task_dispatch():
            with self.assertRaises(UnexpectedTaskDispatch):
                Producer.publish(None, {"message": "unexpected"})
            with self.assertRaises(UnexpectedTaskDispatch):
                local_task.apply()
        self.assertIs(Producer.publish, original)
        self.assertEqual(local_task.apply().get(), "executed")


if __name__ == "__main__":
    unittest.main()
