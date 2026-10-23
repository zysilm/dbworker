import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from dbworker import Coordinator, ExecutionStatus, Finished


class Base(DeclarativeBase):
    pass


class Job(Base):
    __tablename__ = "claim_recovery_job"
    id: Mapped[int] = mapped_column(primary_key=True)


def handle_job(job, session):
    return Finished()


class SqliteClaimRecoveryTest(unittest.TestCase):
    def test_write_lock_timeout_does_not_stop_polling(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "jobs.db"
            engine = create_engine(f"sqlite:///{path}", connect_args={"timeout": .05})
            sessions = sessionmaker(engine)
            runtime = Coordinator(sessions, database_url=engine.url,
                                  poll_seconds=.01, max_poll_seconds=.05)
            runtime.transactional_worker(name="claim_recovery", source=Job, concurrency=2)(handle_job)
            Base.metadata.create_all(engine)

            def add_job():
                with sessions.begin() as session:
                    job = Job()
                    session.add(job)
                    session.flush()
                    return job.id

            def wait_finished(job_id):
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    with sessions() as session:
                        status = runtime.execution_status(session, worker="claim_recovery", source_id=job_id)
                    if status == ExecutionStatus.FINISHED:
                        return
                    time.sleep(.01)
                self.fail(f"Job {job_id} did not finish")

            try:
                runtime.start()
                thread, _ = runtime._running["claim_recovery"]
                wait_finished(add_job())
                time.sleep(.1)
                wait_finished(add_job())
                contention = threading.Event()
                with closing(sqlite3.connect(path)) as locker:
                    locker.execute("BEGIN IMMEDIATE")
                    with patch("dbworker.logger.exception", side_effect=lambda *args: contention.set()) as logged:
                        self.assertTrue(contention.wait(5), "No claim recovery was logged")
                        self.assertTrue(thread.is_alive())
                        self.assertEqual(logged.call_args.args[0], "Error claiming work for %s; retrying after backoff")
                        locker.rollback()
                wait_finished(add_job())
                self.assertTrue(thread.is_alive())
            finally:
                runtime.stop()
                engine.dispose()
