import multiprocessing
import os
import tempfile
import time
import unittest
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from pathlib import Path

from sqlalchemy import create_engine, select, update
from sqlalchemy.orm import Session, sessionmaker

from durable_worker_example.db.engine import Base
from durable_worker_example.db.models import Document, FeatureArtifact, Workspace
from dbworker import (
    Coordinator, Finished, LostClaim, _Worker, _WorkerDefinition,
    _execute_in_process, _initialize_process, now,
)


def process_handler(
    artifact: FeatureArtifact, session: Session,
    *, delay: float = 0, started_file: str | None = None, proceed_file: str | None = None,
    fail: bool = False,
) -> Finished:
    artifact_id = artifact.id
    assert session.get(Document, artifact.document_id) is not None
    session.rollback()
    if started_file:
        Path(started_file).write_text(str(os.getpid()))
    if proceed_file:
        deadline = time.monotonic() + 15
        while not Path(proceed_file).exists():
            if time.monotonic() > deadline:
                raise TimeoutError('test release signal')
            time.sleep(.01)
    time.sleep(delay)
    current = session.get(FeatureArtifact, artifact_id)
    assert current is not None
    current.feature_json = {'pid': os.getpid()}
    if fail:
        session.flush()
        raise ValueError('child save failure')
    return Finished()


class ProcessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.url = f'sqlite:///{self.directory.name}/process.db'
        self.engine = create_engine(self.url)
        self.session_factory = sessionmaker(self.engine, expire_on_commit=False)
        Base.metadata.create_all(self.engine)
        with self.session_factory.begin() as session:
            session.add(Workspace(id=1, name='process'))
            for key in (1, 2):
                session.add(Document(id=key, workspace_id=1, name=str(key), text='real source'))
                session.add(FeatureArtifact(id=key, workspace_id=1, document_id=key))
        self.coordinator = Coordinator(self.session_factory, database_url=self.url, lease_seconds=1, poll_seconds=.01)

    def tearDown(self) -> None:
        self.coordinator.stop()
        self.engine.dispose()
        self.directory.cleanup()

    def worker(self, **kwargs: object) -> _Worker:
        self.coordinator.transactional_worker(name='process_test', source=FeatureArtifact,
                        eligible=lambda: select(FeatureArtifact).where(FeatureArtifact.feature_json.is_(None)).order_by(FeatureArtifact.id))(partial(process_handler, **kwargs))
        worker = self.coordinator.workers['process_test']
        self.coordinator.create_worker_tables()
        return worker

    def wait_for(self, predicate: Callable[[], bool]) -> None:
        deadline = time.monotonic() + 20
        while not predicate():
            if time.monotonic() > deadline:
                self.fail('Timed out waiting for child')
            time.sleep(.02)

    def test_entire_handler_runs_in_child_and_reuses_process(self) -> None:
        worker = self.worker()
        self.coordinator.start()
        def completed() -> bool:
            with self.session_factory() as session:
                state = worker.state(session, 2)
                return state is not None and state['execution_status'] == 'finished'
        self.wait_for(completed)
        self.coordinator.stop()
        with self.session_factory() as session:
            features = [session.get(FeatureArtifact, key).feature_json for key in (1, 2)]
        self.assertNotEqual(features[0]['pid'], os.getpid())
        self.assertEqual(features[0]['pid'], features[1]['pid'])

    def test_shutdown_drains_child_and_renews_lease(self) -> None:
        started = Path(self.directory.name, 'started')
        worker = self.worker(delay=2, started_file=str(started))
        self.coordinator.start()
        self.wait_for(started.exists)
        # Stop immediately after the first invocation starts; it exceeds its lease.
        self.coordinator.stop()
        with self.session_factory() as session:
            self.assertEqual(worker.state(session, 1)['execution_status'], 'finished')
            self.assertIsNone(worker.state(session, 2))
        self.assertFalse(self.coordinator._running)

    def test_live_child_lease_is_renewed(self) -> None:
        started = Path(self.directory.name, 'started')
        release = Path(self.directory.name, 'release')
        worker = self.worker(started_file=str(started), proceed_file=str(release))
        self.coordinator.start()
        self.wait_for(started.exists)
        try:
            time.sleep(1.4)
            with self.session_factory() as session:
                self.assertGreater(worker.state(session, 1)['lease_expires_at'], now())
        finally:
            release.touch()
        self.coordinator.stop()

    def test_child_exception_rolls_back_writes_and_parent_marks_failed(self) -> None:
        worker = self.worker(fail=True)
        self.coordinator.start()
        def failed() -> bool:
            with self.session_factory() as session:
                state = worker.state(session, 2)
                return state is not None and state['execution_status'] == 'failed'
        self.wait_for(failed)
        self.coordinator.stop()
        with self.session_factory() as session:
            for key in (1, 2):
                self.assertIsNone(session.get(FeatureArtifact, key).feature_json)
                self.assertEqual(worker.state(session, key)['error'], 'child save failure')

    def test_replaced_claim_cannot_save_from_child(self) -> None:
        started = Path(self.directory.name, 'started')
        release = Path(self.directory.name, 'release')
        worker = self.worker(started_file=str(started), proceed_file=str(release))
        claim = self.coordinator.claim(worker)
        assert claim is not None
        definition = _WorkerDefinition(worker.name, worker.source, worker.handler)
        with ProcessPoolExecutor(1, mp_context=multiprocessing.get_context('spawn'),
                                 initializer=_initialize_process, initargs=(self.url, {}, (definition,), worker.name)) as pool:
            future = pool.submit(_execute_in_process, claim)
            self.wait_for(started.exists)
            try:
                with self.session_factory.begin() as session:
                    session.execute(update(worker.table).where(worker.table.c.source_id == claim.source_id).values(claim_token='replacement'))
            finally:
                release.touch()
            with self.assertRaises(LostClaim):
                future.result(timeout=15)
        with self.session_factory() as session:
            self.assertIsNone(session.get(FeatureArtifact, 1).feature_json)
            self.assertEqual(worker.state(session, 1)['claim_token'], 'replacement')

    def test_in_memory_database_rejected_before_start(self) -> None:
        coordinator = Coordinator(self.session_factory, database_url='sqlite://')
        with self.assertRaisesRegex(ValueError, 'file-backed SQLite'):
            coordinator.start()
        self.assertFalse(coordinator._running)

    def test_nested_handler_rejected_before_claiming(self) -> None:
        def nested(source: FeatureArtifact, session: Session) -> Finished:
            return Finished()
        self.coordinator.transactional_worker(name='nested_handler', source=FeatureArtifact)(nested)
        worker = self.coordinator.workers['nested_handler']
        self.coordinator.create_worker_tables()
        with self.assertRaisesRegex(ValueError, 'importable'):
            self.coordinator.start()
        self.assertFalse(self.coordinator._running)
        with self.session_factory() as session:
            self.assertIsNone(worker.state(session, 1))
