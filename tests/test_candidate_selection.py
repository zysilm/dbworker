import sqlite3
import unittest
from datetime import timedelta
from unittest.mock import patch

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
        self.assertEqual(self.coordinator._claim_many(self.worker, 64), [])
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

    def test_row_locking_query_excludes_null_leases_and_terminal_states(self) -> None:
        # Exercise the row-locking query shape on SQLite, without requiring a
        # service. Actual concurrent source locking is checked separately.
        self.coordinator.__dict__["_supports_skip_locked"] = True
        self.worker.eligible = lambda: select(Source).join(
            Gate, Gate.source_id == Source.id,
        ).where(Gate.enabled).order_by(Gate.priority.desc())
        with self.session_factory.begin() as session:
            for key, priority in (("null-lease", 100), ("finished", 99), ("failed", 98), ("new", 1)):
                session.add(Source(id=key))
                session.add(Gate(source_id=key, priority=priority))
            session.execute(self.worker.table.insert(), [
                dict(source_id="null-lease", execution_status=ExecutionStatus.WORKING),
                dict(source_id="finished", execution_status=ExecutionStatus.FINISHED),
                dict(source_id="failed", execution_status=ExecutionStatus.FAILED),
            ])
        with self.session_factory() as session:
            self.assertEqual(session.scalar(self.coordinator._candidate(self.worker, now())), "new")

    def test_row_locking_query_keeps_ledger_subquery_when_eligibility_joins_it(self) -> None:
        self.coordinator.__dict__["_supports_skip_locked"] = True
        table = self.worker.table
        self.worker.eligible = lambda: select(Source).join(
            table, table.c.source_id == Source.id,
        ).where(table.c.execution_status == ExecutionStatus.UNFINISHED)
        with self.session_factory.begin() as session:
            session.add(Source(id="unfinished"))
            session.execute(table.insert().values(
                source_id="unfinished", execution_status=ExecutionStatus.UNFINISHED,
            ))
        query = self.coordinator._candidate(self.worker, now()).with_for_update(
            skip_locked=True, of=self.worker.source_table,
        )
        for dialect in (postgresql.dialect(), mysql.dialect()):
            sql = str(query.compile(dialect=dialect))
            self.assertIn("SKIP LOCKED", sql)
        with self.session_factory() as session:
            self.assertEqual(session.scalar(query), "unfinished")

    def add_pending_sources(self) -> None:
        with self.session_factory.begin() as session:
            session.add_all(Source(id=str(i)) for i in range(3))
            session.add_all(Gate(source_id=str(i), priority=i) for i in range(3))

    def test_batch_preserves_priority_and_commits_distinct_ownership(self) -> None:
        self.add_pending_sources()
        claims = self.coordinator._claim_many(self.worker, 2)
        self.assertEqual([claim.source_id for claim in claims], ["2", "1"])
        self.assertEqual(len({claim.token for claim in claims}), 2)
        with self.session_factory() as session:
            for claim in claims:
                state = self.worker.state(session, claim.source_id)
                self.assertEqual(state["claim_token"], claim.token)
                self.assertEqual(state["execution_status"], ExecutionStatus.WORKING)
        self.assertEqual([claim.source_id for claim in self.coordinator._claim_many(self.worker, 2)], ["0"])
        self.assertEqual(self.coordinator._claim_many(self.worker, 2), [])

    def test_failed_batch_rolls_back_all_ownership(self) -> None:
        self.add_pending_sources()
        record_claim = self.coordinator._record_claim

        def fail_second(session, worker, source_id, timestamp):
            if source_id == "1":
                raise RuntimeError("Second claim failed")
            return record_claim(session, worker, source_id, timestamp)

        with patch.object(self.coordinator, "_record_claim", side_effect=fail_second):
            with self.assertRaisesRegex(RuntimeError, "Second claim failed"):
                self.coordinator._claim_many(self.worker, 3)
        with self.session_factory() as session:
            self.assertEqual(list(session.scalars(select(self.worker.table.c.source_id))), [])

    def test_batch_deduplicates_custom_join_results(self) -> None:
        self.add_pending_sources()
        self.worker.eligible = lambda: select(Source).join_from(
            Source, Gate, Gate.enabled.is_(True),
        ).order_by(Source.id, Gate.source_id)
        claims = self.coordinator._claim_many(self.worker, 6)
        self.assertEqual([claim.source_id for claim in claims], ["0", "1"])
        with self.session_factory() as session:
            for claim in claims:
                self.assertEqual(self.worker.state(session, claim.source_id)["claim_token"], claim.token)

    def test_slow_batch_refreshes_leases_before_committing(self) -> None:
        self.add_pending_sources()
        self.coordinator.lease_seconds = 1
        clock = [now()]
        record_claim = self.coordinator._record_claim

        def slow_record(session, worker, source_id, timestamp):
            claim = record_claim(session, worker, source_id, timestamp)
            clock[0] += timedelta(seconds=2)
            return claim

        with patch("dbworker.now", side_effect=lambda: clock[0]):
            with patch.object(self.coordinator, "_record_claim", side_effect=slow_record):
                claims = self.coordinator._claim_many(self.worker, 2)
            with self.session_factory() as session:
                for claim in claims:
                    self.assertEqual(self.worker.state(session, claim.source_id)["lease_expires_at"],
                                     clock[0] + timedelta(seconds=1))
            # Once committed, another claimant must not immediately steal the
            # first item just because recording the later item took time.
            self.assertEqual([claim.source_id for claim in self.coordinator._claim_many(self.worker, 2)], ["0"])
