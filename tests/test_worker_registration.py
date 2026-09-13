import pickle
import unittest
from unittest.mock import Mock, patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from dbworker import Coordinator, Finished


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "registration_source"
    id: Mapped[int] = mapped_column(primary_key=True)


def importable_handler(source: Source, session: Session) -> Finished:
    return Finished()


class RegistrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite://")
        self.addCleanup(self.engine.dispose)
        self.session_factory = sessionmaker(self.engine)
        self.coordinator = Coordinator(self.session_factory, database_url=self.engine.url)

    def test_decorator_registers_configuration_without_starting(self) -> None:
        eligible = lambda: select(Source).order_by(Source.id)

        @self.coordinator.transactional_worker(
            name="configured", source=Source, eligible=eligible, concurrency=3,
        )
        def handler(source: Source, session: Session) -> Finished:
            return Finished()

        worker = self.coordinator.workers["configured"]
        self.assertIs(worker.handler, handler)
        self.assertIs(worker.eligible, eligible)
        self.assertIs(worker.source, Source)
        self.assertEqual(worker.concurrency, 3)
        self.assertEqual(worker.table.name, "configured_work")
        self.assertFalse(self.coordinator._running)
        # Direct calls remain ordinary calls, without managed DB resources.
        with self.session_factory() as session:
            self.assertIsInstance(handler(Source(id=1), session), Finished)
            self.assertFalse(session.in_transaction())

    def test_importable_function_stays_picklable_and_registration_is_local(self) -> None:
        decorate = self.coordinator.transactional_worker(name="importable", source=Source)
        handler = decorate(importable_handler)
        self.assertIs(handler, importable_handler)
        self.assertIs(pickle.loads(pickle.dumps(handler)), importable_handler)
        other = Coordinator(self.session_factory, database_url=self.engine.url)
        self.assertEqual(other.workers, {})
        self.assertNotIn("_dbworker_transactional", vars(handler))

    def test_construction_and_decoration_do_not_connect_to_database(self) -> None:
        with patch.object(self.engine, "connect", side_effect=AssertionError("import opened a connection")):
            coordinator = Coordinator(self.session_factory, database_url=self.engine.url)
            coordinator.transactional_worker(name="import_only", source=Source)(importable_handler)
        self.assertFalse(coordinator._running)

    def test_duplicate_does_not_replace_handler(self) -> None:
        decorate = self.coordinator.transactional_worker(name="duplicate", source=Source)
        decorate(importable_handler)
        with self.assertRaisesRegex(ValueError, "Duplicate worker name"):
            decorate(importable_handler)
        self.assertIs(self.coordinator.workers["duplicate"].handler, importable_handler)

    def test_registration_after_start_is_rejected_without_creating_metadata(self) -> None:
        decorate = self.coordinator.transactional_worker(name="too_late", source=Source)
        self.coordinator._running["running"] = (Mock(), Mock())
        with self.assertRaisesRegex(RuntimeError, "before starting"):
            decorate(importable_handler)
        self.assertNotIn("too_late", self.coordinator.workers)
        self.assertNotIn("too_late_work", Base.metadata.tables)
