"""Check PostgreSQL ownership and rollback in a fresh schema per test."""

import os
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from sqlalchemy import String, create_engine, event, func, select, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from dbworker import (
    Claim, Coordinator, ExecutionStatus, Finished, LostClaim, _execute_claim, now,
)


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "postgres_claim_source"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    processed: Mapped[bool] = mapped_column(default=False)


class Gate(Base):
    __tablename__ = "postgres_claim_gate"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    priority: Mapped[int] = mapped_column(default=0)


def handle(source, session):
    return Finished()


def flushing_handler(source, session):
    source.processed = True
    session.flush()
    return Finished()


@unittest.skipUnless(os.environ.get("DBWORKER_TEST_POSTGRES_URL"), "Owned PostgreSQL test service required")
class PostgresClaimTests(unittest.TestCase):
    def setUp(self):
        self.schema = "claim_" + uuid.uuid4().hex
        self.raw = create_engine(os.environ["DBWORKER_TEST_POSTGRES_URL"])
        with self.raw.begin() as connection:
            connection.execute(text(f"CREATE SCHEMA {self.schema}"))
        self.engine = self.raw.execution_options(schema_translate_map={None: self.schema})
        self.sessions = sessionmaker(self.engine)
        self.runtime, self.worker = self.coordinator()
        Base.metadata.create_all(self.engine)
        self.runtime.create_worker_tables()

    def tearDown(self):
        with self.raw.begin() as connection:
            connection.execute(text(f"DROP SCHEMA {self.schema} CASCADE"))
        self.raw.dispose()

    def coordinator(self):
        runtime = Coordinator(self.sessions, database_url=self.raw.url)
        runtime.transactional_worker(name="postgres_claim", source=Source,
                                     eligible=lambda: select(Source).order_by(Source.id))(handle)
        return runtime, runtime.workers["postgres_claim"]

    def sources(self, ids):
        with self.sessions.begin() as session:
            session.add_all(Source(id=value) for value in ids)

    def test_availability_priority_and_null_lease(self):
        priorities = {"finished": 100, "failed": 99, "active": 98, "null": 97,
                      "expired": 3, "unfinished": 2, "new": 1}
        self.sources(priorities)
        with self.sessions.begin() as session:
            session.add_all(Gate(id=key, priority=value) for key, value in priorities.items())
            for key, status in (("finished", ExecutionStatus.FINISHED), ("failed", ExecutionStatus.FAILED),
                                ("active", ExecutionStatus.WORKING), ("null", ExecutionStatus.WORKING),
                                ("expired", ExecutionStatus.WORKING), ("unfinished", ExecutionStatus.UNFINISHED)):
                session.execute(self.worker.table.insert().values(
                    source_id=key, execution_status=status, claim_token="old", error="old error",
                    lease_expires_at=None if key == "null" else now() + timedelta(
                        seconds=-60 if key == "expired" else 60)))
        self.worker.eligible = lambda: select(Source).join(Gate, Gate.id == Source.id).order_by(Gate.priority.desc())
        claims = self.runtime._claim_many(self.worker, 8)
        self.assertEqual([c.source_id for c in claims], ["expired", "unfinished", "new"])
        self.assertEqual(len({c.token for c in claims}), 3)
        with self.sessions() as session:
            for claim in claims:
                state = self.worker.state(session, claim.source_id)
                self.assertEqual(state["claim_token"], claim.token)
                self.assertIsNone(state["error"])
                self.assertGreater(state["lease_expires_at"], now())
        self.assertEqual(self.runtime._claim_many(self.worker, 8), [])

    def test_upsert_cannot_overwrite_unavailable_ownership(self):
        self.sources(["active", "null", "finished", "new"])
        with self.sessions.begin() as session:
            for key, status in (("active", ExecutionStatus.WORKING), ("null", ExecutionStatus.WORKING),
                                ("finished", ExecutionStatus.FINISHED)):
                session.execute(self.worker.table.insert().values(
                    source_id=key, execution_status=status, claim_token="preserved",
                    lease_expires_at=None if key == "null" else now() + timedelta(seconds=60)))
            claims = self.runtime._record_claims(session, self.worker, ["active", "null", "finished", "new"], now())
        self.assertEqual([c.source_id for c in claims], ["new"])
        with self.sessions() as session:
            for key in ["active", "null", "finished"]:
                self.assertEqual(self.worker.state(session, key)["claim_token"], "preserved")

    def test_competing_coordinators_claim_each_source_once(self):
        self.sources([str(i) for i in range(80)])
        def consume(unused):
            runtime, worker = self.coordinator()
            result = []
            while claims := runtime._claim_many(worker, 8):
                result.extend(claims)
            return result
        with ThreadPoolExecutor(max_workers=4) as executor:
            claims = [c for group in executor.map(consume, range(4)) for c in group]
        self.assertEqual(len(claims), 80)
        self.assertEqual(len({c.source_id for c in claims}), 80)
        self.assertEqual(len({c.token for c in claims}), 80)

    def test_source_row_locks_are_skipped(self):
        self.sources(["a", "b"])
        with self.sessions.begin() as owner:
            owner.execute(select(Source.id).where(Source.id == "a").with_for_update())
            claim = self.runtime.claim(self.worker)
            self.assertEqual(claim.source_id, "b")
        self.assertEqual(self.runtime.claim(self.worker).source_id, "a")

    def test_failed_refresh_rolls_back_entire_batch(self):
        self.sources(["a", "b", "c"])
        def fail_refresh(connection, cursor, statement, parameters, context, many):
            if statement.lstrip().startswith("UPDATE") and "lease_expires_at" in statement:
                raise RuntimeError("Injected lease refresh failure")
        event.listen(self.engine, "before_cursor_execute", fail_refresh)
        try:
            with self.assertRaisesRegex(RuntimeError, "Injected lease refresh failure"):
                self.runtime._claim_many(self.worker, 3)
        finally:
            event.remove(self.engine, "before_cursor_execute", fail_refresh)
        with self.sessions() as session:
            self.assertEqual(session.scalar(select(func.count()).select_from(self.worker.table)), 0)

    def test_duplicate_join_rows_do_not_duplicate_upsert_keys(self):
        self.sources(["a", "b", "c"])
        with self.sessions.begin() as session:
            session.add_all(Gate(id=str(i)) for i in range(3))
        self.worker.eligible = lambda: select(Source).join_from(
            Source, Gate, Gate.priority == 0).order_by(Source.id, Gate.id)
        self.assertEqual([c.source_id for c in self.runtime._claim_many(self.worker, 6)], ["a", "b"])

    def test_reclaimed_batch_rolls_back_stale_handler_writes(self):
        self.sources(["a"])
        self.worker.handler = flushing_handler
        with self.sessions.begin() as session:
            session.execute(self.worker.table.insert().values(
                source_id="a", execution_status=ExecutionStatus.WORKING,
                claim_token="expired-owner", lease_expires_at=now() - timedelta(seconds=60)))
        claims = self.runtime._claim_many(self.worker, 2)
        self.assertEqual(len(claims), 1)
        fresh = claims[0]
        with self.assertRaises(LostClaim):
            _execute_claim(self.worker, Claim("a", "expired-owner"), self.sessions)
        with self.sessions() as session:
            self.assertFalse(session.get(Source, "a").processed)
            self.assertEqual(self.worker.state(session, "a")["claim_token"], fresh.token)
        self.assertIsInstance(_execute_claim(self.worker, fresh, self.sessions), Finished)
        with self.sessions() as session:
            self.assertTrue(session.get(Source, "a").processed)
            self.assertEqual(self.worker.state(session, "a")["execution_status"], ExecutionStatus.FINISHED)
            self.assertIsNone(self.worker.state(session, "a")["claim_token"])
