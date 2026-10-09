"""Regression tests for concurrent durable publication and interceptor lifetime."""
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

from celery.app.task import Task
from kombu import Producer
from examples.saleor_dbworker.runtime import durable_children


class ConcurrentSaleorDispatchTests(unittest.TestCase):
    def test_concurrent_contexts_keep_own_operation_and_restore_only_after_last_exit(self):
        original = Task.apply_async
        publish = Producer.publish
        barrier = threading.Barrier(8)
        records = []
        guard = threading.Lock()
        def enqueue(name, args, kwargs, operation_id, parent_id):
            with guard:
                records.append((name, args, kwargs, operation_id, parent_id))
            return SimpleNamespace(id=operation_id)
        def produce(index):
            with durable_children(f"op-{index}", f"parent-{index}"):
                barrier.wait(timeout=3)
                result = Task.apply_async(SimpleNamespace(name="export-products"), args=[index], kwargs={"marker": index})
                self.assertEqual(result.id, f"op-{index}")
                barrier.wait(timeout=3)
        with patch("examples.saleor_dbworker.runtime.enqueue", enqueue):
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(produce, range(8)))
        self.assertEqual(len(records), 8)
        for name, args, kwargs, operation_id, parent_id in records:
            index = args[0]
            self.assertEqual((name, kwargs, operation_id, parent_id),
                             ("export-products", {"marker": index}, f"op-{index}", f"parent-{index}"))
        self.assertIs(Task.apply_async, original)
        self.assertIs(Producer.publish, publish)

    def test_nested_context_restores_outer_identity(self):
        seen = []
        with patch("examples.saleor_dbworker.runtime.enqueue", side_effect=lambda name, args, kwargs, op, parent: seen.append((op, parent))):
            with durable_children("outer", None):
                with durable_children("inner", "root"):
                    Task.apply_async(SimpleNamespace(name="export-products"))
                Task.apply_async(SimpleNamespace(name="export-products"))
        self.assertEqual(seen, [("inner", "root"), ("outer", None)])
