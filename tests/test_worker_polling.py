import sqlite3
import unittest
from concurrent.futures import Future
from typing import cast
from unittest.mock import Mock, patch

from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from dbworker import Claim, Coordinator, Finished, Outcome, _Worker


class ClockEvent:
    """Advance a fake clock only when no completion has signalled the event."""

    def __init__(self) -> None:
        self.now = 0.0
        self.signalled = False
        self.waits: list[float] = []

    def clear(self) -> None:
        self.signalled = False

    def set(self) -> None:
        self.signalled = True

    def wait(self, timeout: float) -> None:
        if not self.signalled:
            self.waits.append(timeout)
            self.now += timeout


class CompletionEvent(ClockEvent):
    """Complete an in-flight handler before an outstanding polling deadline."""

    def __init__(self, parent: Future[Outcome], *, fail: bool = False) -> None:
        super().__init__()
        self.parent = parent
        self.fail = fail

    def wait(self, timeout: float) -> None:
        if self.signalled:
            return
        if not self.parent.done() and self.now < .1 <= self.now + timeout:
            self.waits.append(timeout)
            self.now = .1
            if self.fail:
                self.parent.set_exception(RuntimeError('Controlled worker failure'))
            else:
                self.parent.set_result(Finished())
        else:
            super().wait(timeout)


class PollingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine('sqlite://')
        self.coordinator = Coordinator(sessionmaker(self.engine), database_url=self.engine.url)
        self.worker = cast(_Worker, Mock(name='worker', concurrency=1))
        self.clock = ClockEvent()
        self.handlers = Mock()
        self.addCleanup(self.engine.dispose)

    def run_scheduler(self) -> None:
        with patch('dbworker.time.monotonic', side_effect=lambda: self.clock.now):
            self.coordinator._run(self.worker, self.handlers, self.clock)

    def test_empty_claims_back_off_to_cap_and_success_resets_delay(self) -> None:
        times: list[float] = []
        completed: Future[Outcome] = Future()
        completed.set_result(Finished())
        self.handlers.submit.return_value = completed

        def claim(worker: _Worker) -> Claim | None:
            times.append(self.clock.now)
            if len(times) == 9:
                return Claim(1, 'token')
            if len(times) == 11:
                self.coordinator._stop.set()
            return None

        with patch.object(self.coordinator, 'claim', side_effect=claim):
            self.run_scheduler()
        self.assertEqual(times, [0, .25, .75, 1.75, 3.75, 7.75, 15.75, 25.75, 35.75, 35.75, 36])
        self.assertEqual(self.clock.waits, [.25, .5, 1, 2, 4, 8, 10, 10, .25])

    def test_successful_work_refills_without_polling_delay(self) -> None:
        times: list[float] = []
        completed: Future[Outcome] = Future()
        completed.set_result(Finished())
        self.handlers.submit.return_value = completed

        def claim(worker: _Worker) -> Claim | None:
            times.append(self.clock.now)
            if len(times) == 5:
                self.coordinator._stop.set()
                return None
            return Claim(len(times), str(len(times)))

        with patch.object(self.coordinator, 'claim', side_effect=claim):
            self.run_scheduler()
        self.assertEqual(times, [0] * 5)
        self.assertEqual(self.clock.waits, [])

    def test_busy_claims_back_off_to_cap_and_success_resets_delay(self) -> None:
        for code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_BUSY_RECOVERY,
                     sqlite3.SQLITE_BUSY_TIMEOUT, sqlite3.SQLITE_BUSY_SNAPSHOT):
            with self.subTest(code=code):
                self.coordinator._stop.clear()
                self.clock = ClockEvent()
                error = sqlite3.OperationalError('database is locked')
                error.sqlite_errorcode = code
                times: list[float] = []
                completed: Future[Outcome] = Future()
                completed.set_result(Finished())
                self.handlers.submit.return_value = completed

                def claim_many(worker: _Worker, limit: int) -> list[Claim]:
                    times.append(self.clock.now)
                    if len(times) < 9:
                        raise OperationalError('BEGIN IMMEDIATE', {}, error)
                    if len(times) == 9:
                        return [Claim(1, 'token')]
                    if len(times) == 11:
                        self.coordinator._stop.set()
                    return []

                with patch.object(self.coordinator, '_claim_many', side_effect=claim_many), \
                        patch('dbworker.logger.exception') as logged:
                    self.run_scheduler()
                self.assertEqual(times, [0, .25, .75, 1.75, 3.75, 7.75, 15.75, 25.75, 35.75, 35.75, 36])
                self.assertEqual(self.clock.waits, [.25, .5, 1, 2, 4, 8, 10, 10, .25])
                self.assertEqual(logged.call_count, 8)

    def test_non_busy_claim_errors_are_not_swallowed(self) -> None:
        locked = sqlite3.OperationalError('database table is locked')
        locked.sqlite_errorcode = sqlite3.SQLITE_LOCKED
        readonly = sqlite3.OperationalError('attempt to write a readonly database')
        readonly.sqlite_errorcode = sqlite3.SQLITE_READONLY
        for original in (sqlite3.OperationalError('no such table'),
                         sqlite3.OperationalError('database is locked'),
                         locked, readonly,
                         RuntimeError('connection failed')):
            with self.subTest(original=original):
                error = OperationalError('SELECT', {}, original)
                with patch.object(self.coordinator, '_claim_many', side_effect=error):
                    with self.assertRaises(OperationalError) as raised:
                        self.run_scheduler()
                self.assertIs(raised.exception, error)
        self.handlers.submit.assert_not_called()

    def test_stop_during_busy_claim_backoff(self) -> None:
        error = sqlite3.OperationalError('database is locked')
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY

        def stop_wait(timeout: float) -> None:
            self.assertEqual(timeout, self.coordinator.poll_seconds)
            self.coordinator._stop.set()

        with patch.object(self.coordinator, '_claim_many', side_effect=OperationalError('BEGIN IMMEDIATE', {}, error)) as claim, \
                patch.object(self.clock, 'wait', side_effect=stop_wait), \
                patch('dbworker.logger.exception'):
            self.run_scheduler()
        claim.assert_called_once()
        self.handlers.submit.assert_not_called()

    def test_renewal_continues_during_busy_claim_backoff(self) -> None:
        self.worker.concurrency = 2
        self.coordinator.lease_seconds = 3
        pending: Future[Outcome] = Future()
        self.handlers.submit.return_value = pending
        error = sqlite3.OperationalError('database is locked')
        error.sqlite_errorcode = sqlite3.SQLITE_BUSY
        times: list[float] = []
        renewals: list[float] = []

        def claim_many(worker: _Worker, limit: int) -> list[Claim]:
            times.append(self.clock.now)
            if len(times) == 1:
                return [Claim(1, 'token')]
            raise OperationalError('BEGIN IMMEDIATE', {}, error)

        def renew(worker: _Worker, claims: object) -> None:
            self.assertEqual(list(claims), [Claim(1, 'token')])
            renewals.append(self.clock.now)
            if len(renewals) == 3:
                self.coordinator._stop.set()
                pending.set_result(Finished())

        with patch.object(self.coordinator, '_claim_many', side_effect=claim_many), \
                patch.object(self.coordinator, 'renew', side_effect=renew), \
                patch('dbworker.logger.exception'):
            self.run_scheduler()
        self.assertEqual(times, [0, 0, .25, .75, 1.75])
        self.assertEqual(renewals, [1, 2, 3])
        self.handlers.submit.assert_called_once()

    def test_committed_child_ignores_previous_empty_claim_deadline(self) -> None:
        self.worker.concurrency = 2
        parent: Future[Outcome] = Future()
        child: Future[Outcome] = Future()
        child.set_result(Finished())
        self.clock = CompletionEvent(parent)
        self.handlers.submit.side_effect = [parent, child]
        times: list[float] = []
        limits: list[int] = []

        def claim_many(worker: _Worker, limit: int) -> list[Claim]:
            times.append(self.clock.now)
            limits.append(limit)
            if len(times) == 1:
                return [Claim(1, 'parent')]
            if parent.done():
                self.coordinator._stop.set()
                return [Claim(2, 'child')]
            return []

        with patch.object(self.coordinator, '_claim_many', side_effect=claim_many):
            self.run_scheduler()
        self.assertEqual(times, [0, 0, .1])
        self.assertEqual(limits, [2, 1, 2])
        self.assertEqual(self.handlers.submit.call_count, 2)

    def test_successful_completion_resets_idle_delay_for_later_root(self) -> None:
        self.worker.concurrency = 2
        self.coordinator.poll_seconds = .05
        self.coordinator.max_poll_seconds = .25
        parent: Future[Outcome] = Future()
        root: Future[Outcome] = Future()
        root.set_result(Finished())
        self.clock = CompletionEvent(parent)
        self.handlers.submit.side_effect = [parent, root]
        times: list[float] = []

        def claim_many(worker: _Worker, limit: int) -> list[Claim]:
            times.append(self.clock.now)
            if len(times) == 1:
                return [Claim(1, 'parent')]
            if self.clock.now >= .12:
                self.coordinator._stop.set()
                return [Claim(2, 'later-root')]
            return []

        with patch.object(self.coordinator, '_claim_many', side_effect=claim_many):
            self.run_scheduler()
        self.assertEqual(len(times), 5)
        for observed, expected in zip(times, [0, 0, .05, .1, .15]):
            self.assertAlmostEqual(observed, expected)
        self.assertEqual(self.handlers.submit.call_count, 2)
        # Without the reset, the previous .2 delay postpones this root until .3.

    def test_failed_completion_rechecks_without_resetting_idle_delay(self) -> None:
        self.worker.concurrency = 2
        parent: Future[Outcome] = Future()
        self.clock = CompletionEvent(parent, fail=True)
        self.handlers.submit.return_value = parent
        times: list[float] = []

        def claim_many(worker: _Worker, limit: int) -> list[Claim]:
            times.append(self.clock.now)
            if len(times) == 1:
                return [Claim(1, 'parent')]
            if len(times) == 4:
                self.coordinator._stop.set()
            return []

        with patch.object(self.coordinator, '_claim_many', side_effect=claim_many), \
                patch.object(self.coordinator, '_fail') as fail, patch('dbworker.logger.exception'):
            self.run_scheduler()
        self.assertEqual(times[:3], [0, 0, .1])
        self.assertAlmostEqual(times[3], .6)
        fail.assert_called_once()

    def test_renewal_continues_while_draining_on_shutdown(self) -> None:
        self.coordinator.lease_seconds = 3
        pending: Future[Outcome] = Future()
        self.handlers.submit.return_value = pending
        renewals: list[float] = []

        def claim(worker: _Worker) -> Claim:
            self.coordinator._stop.set()
            return Claim(1, 'token')

        def renew(worker: _Worker, claims: object) -> None:
            renewals.append(self.clock.now)
            if len(renewals) == 2:
                pending.set_result(Finished())

        with patch.object(self.coordinator, 'claim', side_effect=claim), patch.object(self.coordinator, 'renew', side_effect=renew):
            self.run_scheduler()
        self.assertEqual(renewals, [1, 2])
        self.assertEqual(self.clock.waits, [1, 1])

    def test_renewal_does_not_shorten_empty_claim_backoff(self) -> None:
        self.worker.concurrency = 2
        self.coordinator.lease_seconds = 3
        pending: Future[Outcome] = Future()
        self.handlers.submit.return_value = pending
        times: list[float] = []
        renewals: list[float] = []

        def claim(worker: _Worker) -> Claim | None:
            times.append(self.clock.now)
            if len(times) == 1:
                return Claim(1, 'token')
            if len(times) == 6:
                self.coordinator._stop.set()
                pending.set_result(Finished())
            return None

        def renew(worker: _Worker, claims: object) -> None:
            renewals.append(self.clock.now)

        def claim_many(worker: _Worker, limit: int) -> list[Claim]:
            value = claim(worker)
            return [] if value is None else [value]

        with patch.object(self.coordinator, '_claim_many', side_effect=claim_many), patch.object(self.coordinator, 'renew', side_effect=renew):
            self.run_scheduler()
        self.assertEqual(times, [0, 0, .25, .75, 1.75, 3.75])
        self.assertEqual(renewals, [1, 2, 3])

    def test_committed_batch_is_dispatched_when_stop_arrives_during_claiming(self) -> None:
        self.worker.concurrency = 3
        completed: Future[Outcome] = Future()
        completed.set_result(Finished())
        self.handlers.submit.return_value = completed

        def claim_many(worker: _Worker, limit: int) -> list[Claim]:
            self.assertEqual(limit, 3)
            self.coordinator._stop.set()
            return [Claim(i, str(i)) for i in range(3)]

        with patch.object(self.coordinator, '_claim_many', side_effect=claim_many):
            self.run_scheduler()
        self.assertEqual(self.handlers.submit.call_count, 3)
        self.assertEqual(self.clock.waits, [])

    def test_maximum_poll_interval_validation(self) -> None:
        with self.assertRaises(ValueError):
            Coordinator(sessionmaker(self.engine), database_url=self.engine.url, poll_seconds=1, max_poll_seconds=.5)
