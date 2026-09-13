import unittest
from unittest.mock import patch

from sqlalchemy import String, create_engine, exists, select
from sqlalchemy.sql.selectable import Exists
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, aliased, mapped_column, sessionmaker

from dbworker import Coordinator, ExecutionStatus, Finished, Unfinished, _execute_claim


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "status_source"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)


class ExecutionStatusTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite://")
        self.addCleanup(self.engine.dispose)
        self.session_factory = sessionmaker(self.engine)
        self.coordinator = Coordinator(self.session_factory, database_url=self.engine.url)

        @self.coordinator.transactional_worker(name="build", source=Source)
        def build(source: Source, session: Session) -> Finished | Unfinished:
            return Unfinished() if source.id == "partial" else Finished()

        @self.coordinator.transactional_worker(name="other", source=Source)
        def other(source: Source, session: Session) -> Finished:
            return Finished()

        Base.metadata.create_all(self.engine)
        with self.session_factory.begin() as session:
            session.add_all(Source(id=key) for key in ("ready", "failed", "partial", "working", "new"))
        worker = self.coordinator.workers["build"]
        for key in ("ready", "failed", "partial", "working"):
            worker.eligible = lambda key=key: select(Source).where(Source.id == key)
            claim = self.coordinator.claim(worker)
            assert claim is not None
            if key == "failed":
                self.coordinator._fail(worker, claim, ValueError("test failure"))
            elif key != "working":
                _execute_claim(worker, claim, self.session_factory)

    def terminal(self, source_id: object) -> Exists:
        return self.coordinator.has_execution_status(
            worker="build", source_id=source_id,
            statuses=(ExecutionStatus.FINISHED, ExecutionStatus.FAILED),
        )

    def test_lookup_returns_enum_and_is_scoped_to_worker(self) -> None:
        with self.session_factory() as session:
            for key, expected in (("ready", ExecutionStatus.FINISHED), ("failed", ExecutionStatus.FAILED),
                                  ("partial", ExecutionStatus.UNFINISHED), ("working", ExecutionStatus.WORKING),
                                  ("new", None), ("missing", None)):
                self.assertIs(self.coordinator.execution_status(session, worker="build", source_id=key), expected)
            self.assertIsNone(self.coordinator.execution_status(session, worker="other", source_id="ready"))

    def test_predicate_supports_literals_columns_and_aliases(self) -> None:
        with self.session_factory() as session:
            self.assertTrue(session.scalar(select(self.terminal("ready"))))
            self.assertFalse(session.scalar(select(self.terminal("new"))))
            self.assertEqual(set(session.scalars(select(Source.id).where(self.terminal(Source.id)))), {"ready", "failed"})
            candidate = aliased(Source)
            self.assertEqual(set(session.scalars(select(candidate.id).where(~self.terminal(candidate.id)))), {"partial", "working", "new"})
            # The dependency is nested inside another EXISTS, as in comparison eligibility.
            outstanding = exists().where(Source.id == "ready", ~self.terminal(Source.id))
            self.assertFalse(session.scalar(select(outstanding)))
            outstanding = exists().where(Source.id == "partial", ~self.terminal(Source.id))
            self.assertTrue(session.scalar(select(outstanding)))

    def test_empty_statuses_and_unknown_worker(self) -> None:
        with self.session_factory() as session:
            empty = self.coordinator.has_execution_status(worker="build", source_id=Source.id, statuses=())
            self.assertEqual(list(session.scalars(select(Source.id).where(empty))), [])
            with self.assertRaises(KeyError):
                self.coordinator.execution_status(session, worker="typo", source_id="ready")
        with self.assertRaises(KeyError):
            self.coordinator.has_execution_status(worker="typo", source_id="ready", statuses=(ExecutionStatus.FINISHED,))

    def test_predicate_construction_never_opens_connection(self) -> None:
        with patch.object(self.engine, "connect", side_effect=AssertionError("opened connection")):
            self.terminal(Source.id)
