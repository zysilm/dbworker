"""Replay actual DBWorker publication and transaction receipts through admission."""
import copy
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

from benchmarks.common.argument_evidence import argument_digest
from benchmarks.common.native_admission import APPLICATIONS
from benchmarks.common.native_binding import BINDINGS, SOURCE_SHA256
from benchmarks.common.performance_admission import _validate_worker_and_arguments
from benchmarks.common.workflow_graph import WorkflowMismatch, read_trace
from examples.posthog_dbworker import adapter, runtime


def original_source_receipt(task):
    source, module, function, factory = BINDINGS['posthog'][task.name]
    return {'passed': True, 'worker_app': APPLICATIONS['posthog'],
            'check': 'live_task_origin_and_ast', 'observer_only': True,
            'task_names': [task.name], 'tasks': {task.name: {
                'source': source, 'function': function,
                'source_sha256': SOURCE_SHA256[('posthog', source)]}}}


class PostHogTraceTests(unittest.TestCase):
    def test_real_runtime_root_child_receipts_and_commit_boundaries(self):
        """Mock business bodies, but execute actual enqueue, handle and recorder."""
        with tempfile.TemporaryDirectory() as temporary:
            trace = Path(temporary) / 'trace.jsonl'
            engine = create_engine(f'sqlite:///{temporary}/jobs.sqlite')
            self.addCleanup(engine.dispose)
            runtime.Base.metadata.create_all(engine)
            sessions = sessionmaker(engine, expire_on_commit=False)
            tasks = {name: types.SimpleNamespace(name=name) for name in runtime.STAGES}
            app = types.SimpleNamespace(tasks=tasks)
            django_db = types.ModuleType('django.db')
            django_db.close_old_connections = lambda: None
            payload = {'subject': 'Native notification', 'to': ['user@benchmark.invalid'],
                       'body': '<p>Two-factor authentication enabled</p>'}
            delivered = []

            def notify(user_id, submit):
                self.assertEqual(user_id, 23)
                submit(payload)

            with patch.dict(os.environ, {'BENCHMARK_TRACE_PATH': str(trace)}), \
                 patch.dict(sys.modules, {'django.db': django_db}), \
                 patch.object(adapter, 'initialize', return_value=app), \
                 patch.object(adapter, 'notify', side_effect=notify), \
                 patch.object(adapter, 'deliver', side_effect=delivered.append), \
                 patch.object(runtime, 'worker_origin', side_effect=lambda task, suite: original_source_receipt(task)):
                with sessions.begin() as session:
                    root = runtime.enqueue(session, 'notification-0000', 23)
                with sessions() as session:
                    runtime.handle(session.get(runtime.Job, root.id), session)
                    before_commit = read_trace(trace)
                    self.assertFalse(any(event['event'] == 'succeeded' for event in before_commit))
                    self.assertEqual(before_commit[-1]['event'], 'submitted')
                    self.assertEqual(before_commit[-1]['stage'], 'delivery')
                    # Publication intent precedes visibility to another consumer.
                    with sessions() as observer:
                        self.assertEqual(observer.scalar(select(func.count()).select_from(runtime.Job)), 1)
                    session.commit()
                with sessions() as session:
                    child = session.scalar(select(runtime.Job).where(runtime.Job.stage == 'delivery'))
                    runtime.handle(child, session)
                    self.assertEqual(sum(event['event'] == 'succeeded' for event in read_trace(trace)), 1)
                    session.commit()

            events = read_trace(trace)
            self.assertEqual(delivered, [payload])
            self.assertEqual([event['event'] for event in events],
                             ['submitted', 'started', 'submitted', 'succeeded', 'started', 'succeeded'])
            _validate_worker_and_arguments(events, 'posthog')
            for stage, digest in [('notification', argument_digest((23,), {})),
                                  ('delivery', argument_digest((), payload))]:
                relevant = [event for event in events if event['stage'] == stage
                            and event['event'] in ('submitted', 'started')]
                self.assertEqual(len(relevant), 2)
                self.assertEqual({event['details']['argument_sha256'] for event in relevant}, {digest})
                self.assertEqual({event['details']['task_name'] for event in relevant},
                                 {runtime.task_name(stage)})
                broken = copy.deepcopy(events)
                publication = next(event for event in broken if event['stage'] == stage
                                   and event['event'] == 'submitted')
                publication['details'].pop('task_name')
                with self.assertRaisesRegex(WorkflowMismatch, 'task name'):
                    _validate_worker_and_arguments(broken, 'posthog')


if __name__ == '__main__':
    unittest.main()
