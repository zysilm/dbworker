"""A handler process that dies or raises oddly must not stop its worker."""

import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from dbworker import Coordinator, ExecutionStatus, Finished


class Base(DeclarativeBase):
    pass


class ResilienceJob(Base):
    __tablename__ = "resilience_job"
    id: Mapped[int] = mapped_column(primary_key=True)
    mode: Mapped[str]
    marker: Mapped[str] = mapped_column(default="")


class TwoArgumentError(Exception):
    """Pickles in the handler process, but cannot be rebuilt from its args."""

    def __init__(self, code, detail):
        super().__init__(f"{code}: {detail}")


def resilience_handler(job, session):
    if job.mode == "crash_once" and not os.path.exists(job.marker):
        Path(job.marker).touch()
        time.sleep(.3)  # let sibling handlers be in flight
        os._exit(1)  # like an out-of-memory kill or a segfault
    if job.mode == "crash_always":
        time.sleep(.3)
        os._exit(1)
    if job.mode == "unpicklable":
        raise TwoArgumentError(42, "boom")
    if job.mode == "slow":
        time.sleep(.6)
    return Finished()


class SchedulerResilienceTest(unittest.TestCase):
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
        coordinator.transactional_worker(
            name="resilience", source=ResilienceJob, concurrency=2, **worker_options)(resilience_handler)
        Base.metadata.create_all(self.engine)
        coordinator.create_worker_tables()
        self.addCleanup(coordinator.stop)
        return coordinator

    def add_jobs(self, *modes):
        with self.sessions.begin() as session:
            jobs = [ResilienceJob(mode=mode) for mode in modes]
            session.add_all(jobs)
            session.flush()
            for job in jobs:
                job.marker = str(self.directory / f"marker-{job.id}")
            return [job.id for job in jobs]

    def states(self, coordinator, job_ids):
        with self.sessions() as session:
            return {job_id: coordinator.workers["resilience"].state(session, job_id) for job_id in job_ids}

    def wait_settled(self, coordinator, job_ids, timeout=60):
        settled = (ExecutionStatus.FINISHED, ExecutionStatus.FAILED)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            states = self.states(coordinator, job_ids)
            if all(state is not None and state["execution_status"] in settled for state in states.values()):
                return states
            time.sleep(.02)
        self.fail(f"Jobs did not settle: {self.states(coordinator, job_ids)}")

    def test_crashed_handler_process_does_not_stop_the_worker(self):
        coordinator = self.coordinator(retry_backoff_seconds=.01)
        job_ids = self.add_jobs("crash_once", "slow", "slow", "ok")
        with patch("dbworker.logger.exception"), patch("dbworker.logger.warning"):
            coordinator.start()
            thread, _ = coordinator._running["resilience"]
            states = self.wait_settled(coordinator, job_ids)
            for job_id, state in states.items():
                # Neither the crashed job nor its in-flight siblings are failed
                # or charged an attempt for the lost process pool.
                self.assertEqual(state["execution_status"], ExecutionStatus.FINISHED, job_id)
                self.assertEqual(state["attempts"], 0, job_id)
                self.assertIsNone(state["error"], job_id)
            later = self.add_jobs("ok")
            self.assertEqual(self.wait_settled(coordinator, later)[later[0]]["execution_status"],
                             ExecutionStatus.FINISHED)
            self.assertTrue(thread.is_alive())
            coordinator.stop()

    def test_handler_that_always_kills_its_process_is_failed_alone(self):
        coordinator = self.coordinator(max_attempts=2, retry_backoff_seconds=.01)
        job_ids = self.add_jobs("crash_always", "slow", "slow", "ok")
        with patch("dbworker.logger.exception"), patch("dbworker.logger.warning"), patch("dbworker.logger.error"):
            coordinator.start()
            thread, _ = coordinator._running["resilience"]
            states = self.wait_settled(coordinator, job_ids)
            poison = states[job_ids[0]]
            self.assertEqual(poison["execution_status"], ExecutionStatus.FAILED)
            self.assertEqual(poison["attempts"], 2)
            self.assertIn("terminated abruptly", poison["error"])
            for job_id in job_ids[1:]:
                self.assertEqual(states[job_id]["execution_status"], ExecutionStatus.FINISHED, job_id)
                self.assertEqual(states[job_id]["attempts"], 0, job_id)
            self.assertTrue(thread.is_alive())
            coordinator.stop()

    def test_unpicklable_handler_exception_fails_only_its_own_job(self):
        coordinator = self.coordinator(max_attempts=1)
        job_ids = self.add_jobs("unpicklable", "slow", "slow", "ok")
        with patch("dbworker.logger.exception"), patch("dbworker.logger.warning"), \
                patch.object(coordinator, "_replace_pool", wraps=coordinator._replace_pool) as replace:
            coordinator.start()
            thread, _ = coordinator._running["resilience"]
            states = self.wait_settled(coordinator, job_ids)
            self.assertEqual(states[job_ids[0]]["execution_status"], ExecutionStatus.FAILED)
            self.assertIn("TwoArgumentError", states[job_ids[0]]["error"])
            self.assertIn("42: boom", states[job_ids[0]]["error"])
            for job_id in job_ids[1:]:
                self.assertEqual(states[job_id]["execution_status"], ExecutionStatus.FINISHED, job_id)
            self.assertTrue(thread.is_alive())
            replace.assert_not_called()  # the handler processes survived
            coordinator.stop()
