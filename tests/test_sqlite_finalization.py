import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from dbworker import Coordinator, ExecutionStatus, Finished, LostClaim, Unfinished, _execute_claim


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "finalization_source"
    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[int] = mapped_column(default=0)


class FinalizationTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = str(Path(directory.name, "work.db"))
        self.engine = create_engine(f"sqlite:///{self.path}", connect_args={"timeout": 0.01})
        self.addCleanup(self.engine.dispose)
        self.sessions = sessionmaker(self.engine)
        self.calls = 0
        self.outcome = Finished()

        def handler(source, session):
            self.calls += 1
            source.value += 1
            return self.outcome

        self.coordinator = Coordinator(self.sessions, database_url=self.engine.url)
        self.coordinator.transactional_worker(name="finalize", source=Source)(handler)
        self.worker = self.coordinator.workers["finalize"]
        Base.metadata.create_all(self.engine)
        with self.sessions.begin() as session:
            session.add_all([Source(id=1), Source(id=2)])
        self.claim = self.coordinator.claim(self.worker)
        self.busy_errors = []

    def contend(self, replace_token=False, release_on_error=True):
        """Another connection owns the writer lock until a real BUSY is observed."""
        locked = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        failures = []

        def writer():
            try:
                with sqlite3.connect(self.path, timeout=2) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    if replace_token:
                        connection.execute("UPDATE finalize_work SET claim_token='replacement' WHERE source_id=1")
                    else:
                        connection.execute("UPDATE finalization_source SET value=10 WHERE id=2")
                    locked.set()
                    if not release.wait(3):
                        raise AssertionError("completion never encountered the writer lock")
            except BaseException as exc:
                failures.append(exc)
            finally:
                finished.set()

        thread = threading.Thread(target=writer)
        started = False

        def before(connection, cursor, statement, parameters, context, executemany):
            nonlocal started
            if statement.startswith("UPDATE finalize_work") and not started:
                started = True
                thread.start()
                self.assertTrue(locked.wait(3))

        def error(context):
            code = getattr(context.original_exception, "sqlite_errorcode", None)
            if code == sqlite3.SQLITE_BUSY:
                self.busy_errors.append(code)
                if release_on_error:
                    release.set()
                    self.assertTrue(finished.wait(3))

        event.listen(self.engine, "before_cursor_execute", before)
        event.listen(self.engine, "handle_error", error)

        def cleanup():
            release.set()
            if started:
                thread.join(3)
            event.remove(self.engine, "before_cursor_execute", before)
            event.remove(self.engine, "handle_error", error)
            self.assertFalse(failures, failures)
        self.addCleanup(cleanup)

    def test_busy_completion_preserves_pending_writes_without_rerunning_handler(self):
        self.contend()
        _execute_claim(self.worker, self.claim, self.sessions)
        self.assertEqual(self.calls, 1)
        self.assertEqual(self.busy_errors, [sqlite3.SQLITE_BUSY])
        with self.sessions() as session:
            self.assertEqual(session.get(Source, 1).value, 1)
            self.assertEqual(session.get(Source, 2).value, 10)
            state = self.worker.state(session, 1)
            self.assertEqual(state["execution_status"], ExecutionStatus.FINISHED)
            self.assertIsNone(state["claim_token"])
            self.assertIsNone(state["lease_expires_at"])

    def test_busy_unfinished_completion(self):
        self.outcome = Unfinished()
        self.contend()
        _execute_claim(self.worker, self.claim, self.sessions)
        with self.sessions() as session:
            self.assertEqual(self.worker.state(session, 1)["execution_status"], ExecutionStatus.UNFINISHED)
            self.assertEqual(session.get(Source, 1).value, 1)
        self.assertEqual(self.calls, 1)

    def test_busy_wal_completion(self):
        with self.engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA journal_mode=WAL")
        self.contend()
        _execute_claim(self.worker, self.claim, self.sessions)
        self.assertEqual(self.busy_errors, [sqlite3.SQLITE_BUSY])
        self.assertEqual(self.calls, 1)
        with self.sessions() as session:
            self.assertEqual(session.get(Source, 1).value, 1)
            self.assertEqual(self.worker.state(session, 1)["execution_status"], ExecutionStatus.FINISHED)

    def test_replaced_token_during_wait_rolls_back_handler_writes(self):
        self.contend(replace_token=True)
        with self.assertRaises(LostClaim):
            _execute_claim(self.worker, self.claim, self.sessions)
        with self.sessions() as session:
            self.assertEqual(session.get(Source, 1).value, 0)
            self.assertEqual(self.worker.state(session, 1)["claim_token"], "replacement")
        self.assertEqual(self.calls, 1)

    def test_persistent_busy_is_bounded_and_rolls_back(self):
        self.contend(release_on_error=False)
        with patch("dbworker._SQLITE_FINALIZATION_RETRY_SECONDS", 0.03):
            with self.assertRaises(OperationalError) as error:
                _execute_claim(self.worker, self.claim, self.sessions)
        self.assertEqual(error.exception.orig.sqlite_errorcode, sqlite3.SQLITE_BUSY)
        self.assertGreaterEqual(len(self.busy_errors), 2)
        self.assertEqual(self.calls, 1)
        with self.sessions() as session:
            self.assertEqual(session.get(Source, 1).value, 0)
            self.assertEqual(self.worker.state(session, 1)["claim_token"], self.claim.token)

    def test_stale_wal_snapshot_is_not_retried(self):
        with self.engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA journal_mode=WAL")

        def handler(source, session):
            self.calls += 1
            # Explicit read transaction, as used by modern sqlite3 transaction
            # control. The external commit invalidates its WAL snapshot.
            session.connection().exec_driver_sql("BEGIN")
            session.refresh(source)
            source.value += 1
            with sqlite3.connect(self.path) as writer:
                writer.execute("UPDATE finalization_source SET value=10 WHERE id=2")
            return Finished()

        self.worker.handler = handler
        with patch("dbworker.time.sleep") as sleep:
            with self.assertRaises(OperationalError) as error:
                _execute_claim(self.worker, self.claim, self.sessions)
            sleep.assert_not_called()
        self.assertEqual(error.exception.orig.sqlite_errorcode, sqlite3.SQLITE_BUSY_SNAPSHOT)
        self.assertEqual(self.calls, 1)
        with self.sessions() as session:
            self.assertEqual(session.get(Source, 1).value, 0)
            self.assertEqual(self.worker.state(session, 1)["claim_token"], self.claim.token)

    def test_unrelated_operational_error_is_not_retried(self):
        def invalid_sql(connection, cursor, statement, parameters, context, executemany):
            if statement.startswith("UPDATE finalize_work"):
                return "UPDATE nonexistent_table SET value=1", ()
            return statement, parameters

        event.listen(self.engine, "before_cursor_execute", invalid_sql, retval=True)
        self.addCleanup(event.remove, self.engine, "before_cursor_execute", invalid_sql)
        with patch("dbworker.time.sleep") as sleep:
            with self.assertRaises(OperationalError) as error:
                _execute_claim(self.worker, self.claim, self.sessions)
            sleep.assert_not_called()
        self.assertEqual(error.exception.orig.sqlite_errorcode, sqlite3.SQLITE_ERROR)
        self.assertEqual(self.calls, 1)

    def test_busy_after_driver_rollback_is_not_retried(self):
        self.contend()

        def rollback(context):
            context.connection.connection.driver_connection.rollback()

        event.listen(self.engine, "handle_error", rollback)
        self.addCleanup(event.remove, self.engine, "handle_error", rollback)
        with patch("dbworker.time.sleep") as sleep:
            with self.assertRaises(OperationalError):
                _execute_claim(self.worker, self.claim, self.sessions)
            sleep.assert_not_called()
        self.assertEqual(self.calls, 1)
        with self.sessions() as session:
            self.assertEqual(session.get(Source, 1).value, 0)
            self.assertEqual(self.worker.state(session, 1)["claim_token"], self.claim.token)

    def test_flush_failure_after_retry_rolls_back_completion(self):
        self.contend()

        def fail_flush(session, context, instances):
            raise RuntimeError("business write failed")

        event.listen(self.sessions, "before_flush", fail_flush)
        try:
            with self.assertRaisesRegex(RuntimeError, "business write failed"):
                _execute_claim(self.worker, self.claim, self.sessions)
        finally:
            event.remove(self.sessions, "before_flush", fail_flush)
        with self.sessions() as session:
            self.assertEqual(session.get(Source, 1).value, 0)
            self.assertEqual(self.worker.state(session, 1)["execution_status"], ExecutionStatus.WORKING)
            self.assertEqual(self.worker.state(session, 1)["claim_token"], self.claim.token)


if __name__ == "__main__":
    unittest.main()
