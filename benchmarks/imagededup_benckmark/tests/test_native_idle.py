"""Exercise native idle admission with Kombu's actual Redis lane-size implementation."""
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock

from imagededup_benckmark.runtime import native_celery_business_idle

try:
    from kombu.transport.redis import Channel
except ImportError:
    Channel = None


class ReadOnlyRedisPipeline:
    def __init__(self, lists):
        self.lists = lists
        self.keys = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def llen(self, key):
        self.keys.append(key)
        return self

    def execute(self):
        return [len(self.lists.get(key, [])) for key in self.keys]


@unittest.skipIf(Channel is None, "Run Redis lane checks in the native Celery environment")
class NativeIdleTest(unittest.TestCase):
    def setUp(self):
        self.lists = {}
        self.pipelines = []

        def pipeline():
            result = ReadOnlyRedisPipeline(self.lists)
            self.pipelines.append(result)
            return result

        @contextmanager
        def client():
            yield SimpleNamespace(pipeline=pipeline)

        # No network or worker processes: retain Kombu's real _size, _q_for_pri,
        # priority steps and separator; substitute only Redis LLEN responses.
        self.channel = Channel.__new__(Channel)
        self.channel.conn_or_acquire = client
        self.channel.queue_declare = Mock(side_effect=AssertionError("Observation must never declare queues"))
        self.responses = {"build@runner": [], "compare@runner": []}
        self.inspect = Mock()
        for method in ("active", "reserved", "scheduled"):
            getattr(self.inspect, method).return_value = dict(self.responses)

        @contextmanager
        def connection():
            yield SimpleNamespace(transport=SimpleNamespace(driver_type="redis"), channel=lambda: self.channel)

        self.app = SimpleNamespace(control=SimpleNamespace(inspect=lambda timeout: self.inspect),
                                   connection_for_read=connection)

    def test_missing_empty_redis_lists_are_idle_and_all_priority_lanes_are_read(self):
        self.assertTrue(native_celery_business_idle(self.app))
        expected = [self.channel._q_for_pri(queue, priority)
                    for queue in ("image_build", "image_compare")
                    for priority in self.channel.priority_steps]
        self.assertEqual([key for pipeline in self.pipelines for key in pipeline.keys], expected)
        self.channel.queue_declare.assert_not_called()

    def test_pending_message_in_each_priority_lane_rejects_idle(self):
        for queue in ("image_build", "image_compare"):
            for priority in self.channel.priority_steps:
                with self.subTest(queue=queue, priority=priority):
                    self.lists.clear()
                    self.lists[self.channel._q_for_pri(queue, priority)] = ["pending-message"]
                    self.assertFalse(native_celery_business_idle(self.app))
        self.channel.queue_declare.assert_not_called()

    def test_custom_priority_steps_are_observed_instead_of_hardcoded_queue_keys(self):
        self.channel.priority_steps = [0, 2, 5, 9]
        self.lists[self.channel._q_for_pri("image_compare", 5)] = ["pending-message"]
        self.assertFalse(native_celery_business_idle(self.app))
        self.assertEqual(self.pipelines[-1].keys,
                         [self.channel._q_for_pri("image_compare", step) for step in self.channel.priority_steps])

    def test_work_in_active_reserved_or_scheduled_states_rejects_idle(self):
        for method in ("active", "reserved", "scheduled"):
            for task_name in ("images.build", "images.compare"):
                with self.subTest(method=method, task_name=task_name):
                    tasks = [{"request": {"name": task_name}}] if method == "scheduled" else [{"name": task_name}]
                    getattr(self.inspect, method).return_value = {"build@runner": tasks, "compare@runner": []}
                    self.assertFalse(native_celery_business_idle(self.app))
                    getattr(self.inspect, method).return_value = dict(self.responses)

    def test_missing_partial_or_inconsistent_worker_responses_reject_idle(self):
        for response in (None, {}, {"build@runner": []}, {"other@runner": [], "compare@runner": []}):
            with self.subTest(response=response):
                self.inspect.reserved.return_value = response
                self.assertFalse(native_celery_business_idle(self.app))


if __name__ == "__main__":
    unittest.main()
