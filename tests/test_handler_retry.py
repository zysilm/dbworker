import sqlite3
import tempfile
import time
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

import dbworker
from dbworker import Coordinator, ExecutionStatus, Finished


class Base(DeclarativeBase):
    pass


class RetryJob(Base):
    __tablename__ = "retry_job"
    id: Mapped[int] = mapped_column(primary_key=True)
    # Invocations are recorded outside the handler transaction, which a failure rolls back.
    journal: Mapped[str]
    failures: Mapped[int]


def flaky_handler(job, session):
    """Fail like a contended SQLite writer for the first `failures` invocations."""
    with open(job.journal, "a+", encoding="utf-8") as journal:
        journal.seek(0)
        invocation = len(journal.readlines()) + 1
        journal.write(f"{invocation}\n")
    if invocation <= job.failures:
        raise sqlite3.OperationalError("database is locked")
    return Finished()


class HandlerRetryTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.engine = create_engine(f"sqlite:///{self.directory / 'jobs.db'}")
        self.addCleanup(self.engine.dispose)
        self.sessions = sessionmaker(self.engine)

    def coordinator(self, **worker_options):
        coordinator = Coordinator(self.sessions, database_url=self.engine.url,
                                  poll_seconds=.01, max_poll_seconds=.05)
        coordinator.transactional_worker(name="retry", source=RetryJob, **worker_options)(flaky_handler)
        Base.metadata.create_all(self.engine)
        coordinator.create_worker_tables()
        return coordinator

    def add_job(self, failures=0):
        with self.sessions.begin() as session:
            job = RetryJob(journal="", failures=failures)
            session.add(job)
            session.flush()
            job.journal = str(self.directory / f"journal-{job.id}.txt")
            return job.id

    def state(self, coordinator, job_id):
        with self.sessions() as session:
            return coordinator.workers["retry"].state(session, job_id)

    def run_until_settled(self, coordinator, job_id):
        coordinator.start()
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                state = self.state(coordinator, job_id)
                if state is not None and state["execution_status"] in (ExecutionStatus.FINISHED, ExecutionStatus.FAILED):
                    time.sleep(.3)  # a settled execution must not be invoked again
                    return self.state(coordinator, job_id)
                time.sleep(.01)
            self.fail(f"Job {job_id} did not settle: {self.state(coordinator, job_id)}")
        finally:
            coordinator.stop()

    def invocations(self, job_id):
        return len((self.directory / f"journal-{job_id}.txt").read_text(encoding="utf-8").splitlines())

    def test_transient_handler_failure_is_retried_until_it_finishes(self):
        coordinator = self.coordinator(retry_backoff_seconds=.01)
        job_id = self.add_job(failures=2)
        with patch("dbworker.logger.exception"):
            state = self.run_until_settled(coordinator, job_id)
        self.assertEqual(state["execution_status"], ExecutionStatus.FINISHED)
        self.assertIsNone(state["error"])
        self.assertEqual(state["attempts"], 0)
        self.assertIsNone(state["available_at"])
        self.assertEqual(self.invocations(job_id), 3)

    def test_persistent_handler_failure_is_failed_after_max_attempts(self):
        coordinator = self.coordinator(retry_backoff_seconds=.01)
        job_id = self.add_job(failures=99)
        with patch("dbworker.logger.exception"):
            state = self.run_until_settled(coordinator, job_id)
        self.assertEqual(state["execution_status"], ExecutionStatus.FAILED)
        self.assertEqual(state["error"], "database is locked")
        self.assertEqual(state["attempts"], 3)
        self.assertEqual(self.invocations(job_id), 3)

    def test_single_attempt_fails_on_first_exception(self):
        coordinator = self.coordinator(max_attempts=1)
        job_id = self.add_job(failures=1)
        with patch("dbworker.logger.exception"):
            state = self.run_until_settled(coordinator, job_id)
        self.assertEqual(state["execution_status"], ExecutionStatus.FAILED)
        self.assertEqual(self.invocations(job_id), 1)

    def test_retry_waits_for_exponential_backoff(self):
        coordinator = self.coordinator(max_attempts=3, retry_backoff_seconds=10)
        worker = coordinator.workers["retry"]
        job_id = self.add_job()
        start = dbworker.now()
        for attempt, delay in ((1, 10), (2, 20)):
            with patch("dbworker.now", return_value=start):
                claim = coordinator.claim(worker)
                self.assertIsNotNone(claim)
                coordinator._fail(worker, claim, RuntimeError("boom"))
            state = self.state(coordinator, job_id)
            self.assertEqual(state["execution_status"], ExecutionStatus.UNFINISHED)
            self.assertEqual(state["error"], "boom")
            self.assertEqual(state["attempts"], attempt)
            self.assertEqual(state["available_at"], start + timedelta(seconds=delay))
            with patch("dbworker.now", return_value=start + timedelta(seconds=delay - 1)):
                self.assertIsNone(coordinator.claim(worker))
            start += timedelta(seconds=delay)
        with patch("dbworker.now", return_value=start):
            claim = coordinator.claim(worker)
            coordinator._fail(worker, claim, RuntimeError("boom"))
        state = self.state(coordinator, job_id)
        self.assertEqual(state["execution_status"], ExecutionStatus.FAILED)
        self.assertEqual(state["attempts"], 3)
        self.assertIsNone(state["available_at"])

    def test_stale_claim_cannot_record_a_failure(self):
        coordinator = self.coordinator()
        worker = coordinator.workers["retry"]
        job_id = self.add_job()
        stale = coordinator.claim(worker)
        with patch("dbworker.now", return_value=dbworker.now() + timedelta(hours=1)):
            current = coordinator.claim(worker)
        self.assertNotEqual(stale.token, current.token)
        coordinator._fail(worker, stale, RuntimeError("stale"))
        state = self.state(coordinator, job_id)
        self.assertEqual(state["execution_status"], ExecutionStatus.WORKING)
        self.assertEqual(state["attempts"], 0)
        self.assertIsNone(state["error"])

    def test_released_claim_returns_to_the_queue_without_consuming_an_attempt(self):
        coordinator = self.coordinator(max_attempts=1, retry_backoff_seconds=10)
        worker = coordinator.workers["retry"]
        job_id = self.add_job()
        start = dbworker.now()
        with patch("dbworker.now", return_value=start):
            claim = coordinator.claim(worker)
            coordinator._release(worker, [claim], RuntimeError("handler process died"))
        state = self.state(coordinator, job_id)
        self.assertEqual(state["execution_status"], ExecutionStatus.UNFINISHED)
        self.assertEqual(state["attempts"], 0)
        self.assertEqual(state["error"], "handler process died")
        self.assertIsNone(state["claim_token"])
        self.assertEqual(state["available_at"], start + timedelta(seconds=10))
        with patch("dbworker.now", return_value=start + timedelta(seconds=9)):
            self.assertIsNone(coordinator.claim(worker))
        with patch("dbworker.now", return_value=start + timedelta(seconds=10)):
            current = coordinator.claim(worker)
        self.assertIsNotNone(current)
        # A release that arrives after the work was claimed again changes nothing.
        coordinator._release(worker, [claim], RuntimeError("late"))
        self.assertEqual(self.state(coordinator, job_id)["claim_token"], current.token)

    def test_reset_failed_restores_the_attempt_budget(self):
        coordinator = self.coordinator(max_attempts=1)
        worker = coordinator.workers["retry"]
        job_id = self.add_job()
        coordinator._fail(worker, coordinator.claim(worker), RuntimeError("boom"))
        with self.sessions.begin() as session:
            self.assertTrue(worker.reset_failed(session, job_id))
        state = self.state(coordinator, job_id)
        self.assertEqual(state["execution_status"], ExecutionStatus.UNFINISHED)
        self.assertEqual(state["attempts"], 0)
        self.assertIsNotNone(coordinator.claim(worker))

    def test_existing_work_table_gains_retry_columns(self):
        Base.metadata.tables["retry_job"].create(self.engine)
        with self.engine.begin() as connection:
            # The 0.1.3 layout, with a failure recorded by that release.
            connection.execute(text(
                "CREATE TABLE retry_work (source_id INTEGER NOT NULL PRIMARY KEY REFERENCES retry_job (id), "
                "execution_status VARCHAR(10) NOT NULL, claim_token VARCHAR(36), "
                "lease_expires_at DATETIME, error TEXT)"))
            connection.execute(text("INSERT INTO retry_job (id, journal, failures) VALUES (1, '', 0)"))
            connection.execute(text("INSERT INTO retry_work (source_id, execution_status, error) "
                                    "VALUES (1, 'failed', 'database is locked')"))
        coordinator = self.coordinator()
        coordinator.create_worker_tables()  # idempotent
        columns = {column["name"] for column in inspect(self.engine).get_columns("retry_work")}
        self.assertLessEqual({"attempts", "available_at"}, columns)
        state = self.state(coordinator, 1)
        self.assertEqual(state["execution_status"], ExecutionStatus.FAILED)
        self.assertEqual(state["attempts"], 0)
        with self.sessions.begin() as session:
            self.assertTrue(coordinator.workers["retry"].reset_failed(session, 1))
        self.assertIsNotNone(coordinator.claim(coordinator.workers["retry"]))

    def test_retry_options_are_validated(self):
        coordinator = Coordinator(self.sessions, database_url=self.engine.url)
        with self.assertRaises(ValueError):
            coordinator.transactional_worker(name="none", source=RetryJob, max_attempts=0)(flaky_handler)
        with self.assertRaises(ValueError):
            coordinator.transactional_worker(name="negative", source=RetryJob, retry_backoff_seconds=-1)(flaky_handler)
