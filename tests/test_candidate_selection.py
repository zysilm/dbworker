import sqlite3
import unittest
from datetime import timedelta

from sqlalchemy import String, create_engine, func, select
from sqlalchemy.dialects import mysql, postgresql
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from dbworker import Coordinator, ExecutionStatus, Finished, now


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "candidate_source"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)


class Gate(Base):
    __tablename__ = "candidate_gate"
    source_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    priority: Mapped[int] = mapped_column()
    enabled: Mapped[bool] = mapped_column(default=True)


class CandidateSelectionTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite://")
        self.addCleanup(self.engine.dispose)
        self.session_factory = sessionmaker(self.engine)
        self.checked: list[str] = []

        def eligible(source_id: str) -> int:
            self.checked.append(source_id)
            return 1

        with self.engine.connect() as connection:
            dbapi = connection.connection.driver_connection
            assert isinstance(dbapi, sqlite3.Connection)
            dbapi.create_function("check_eligibility", 1, eligible)
        self.coordinator = Coordinator(self.session_factory, database_url=self.engine.url)

        @self.coordinator.transactional_worker(
            name="candidate", source=Source,
            eligible=lambda: select(Source).join(Gate, Gate.source_id == Source.id).where(
                Gate.enabled, func.check_eligibility(Source.id) == 1,
            ).order_by(Gate.priority.desc(), Source.id),
        )
        def handler(source: Source, session: Session) -> Finished:
            return Finished()

        Base.metadata.create_all(self.engine)
        self.coordinator.create_worker_tables()
        self.worker = self.coordinator.workers["candidate"]

    def test_finished_sources_do_not_run_application_eligibility(self) -> None:
        with self.session_factory.begin() as session:
            session.add_all(Source(id=str(i)) for i in range(200))
            session.add_all(Gate(source_id=str(i), priority=i) for i in range(200))
            session.execute(self.worker.table.insert(), [
                dict(source_id=str(i), execution_status=ExecutionStatus.FINISHED) for i in range(200)
            ])
        self.assertIsNone(self.coordinator.claim(self.worker))
        # Count actual SQL predicate evaluations instead of asserting a timing
        # threshold or a particular SQLite query-plan string.
        self.assertEqual(self.checked, [])

    def test_availability_preserves_custom_join_order_and_string_keys(self) -> None:
        statuses = {
            "finished": ExecutionStatus.FINISHED,
            "failed": ExecutionStatus.FAILED,
            "active": ExecutionStatus.WORKING,
            "expired": ExecutionStatus.WORKING,
            "unfinished": ExecutionStatus.UNFINISHED,
        }
        priorities = {"finished": 100, "failed": 99, "active": 98, "disabled": 97,
                      "expired": 3, "unfinished": 2, "new": 1}
        with self.session_factory.begin() as session:
            for key, priority in priorities.items():
                session.add(Source(id=key))
                session.add(Gate(source_id=key, priority=priority, enabled=key != "disabled"))
            for key, status in statuses.items():
                session.execute(self.worker.table.insert().values(
                    source_id=key, execution_status=status, claim_token="old",
                    lease_expires_at=now() + timedelta(seconds=-60 if key == "expired" else 60),
                ))
        for expected in ("expired", "unfinished", "new"):
            claim = self.coordinator.claim(self.worker)
            self.assertIsNotNone(claim)
            assert claim is not None
            self.assertEqual(claim.source_id, expected)
        self.assertIsNone(self.coordinator.claim(self.worker))
        self.assertFalse({"finished", "failed", "active", "disabled"}.intersection(self.checked))

    def test_locking_query_compiles_for_postgresql_and_mysql(self) -> None:
        query = self.coordinator._candidate(self.worker, now()).with_for_update(
            skip_locked=True, of=self.worker.source_table,
        )
        for dialect in (postgresql.dialect(), mysql.dialect()):  # type: ignore[no-untyped-call]
            with self.subTest(dialect=dialect.name):
                sql = str(query.compile(dialect=dialect))
                self.assertIn("SKIP LOCKED", sql)
                self.assertIn("FOR UPDATE", sql)
