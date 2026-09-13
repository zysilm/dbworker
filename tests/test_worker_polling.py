import unittest
from concurrent.futures import Future
from typing import cast
from unittest.mock import Mock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dbworker import Claim, Coordinator, Finished, Outcome, Worker


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


class PollingTest(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine('sqlite://')
        self.coordinator = Coordinator(sessionmaker(self.engine), database_url=self.engine.url)
        self.worker = cast(Worker, Mock(name='worker', concurrency=1))
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

        def claim(worker: Worker) -> Claim | None:
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

        def claim(worker: Worker) -> Claim | None:
            times.append(self.clock.now)
            if len(times) == 5:
                self.coordinator._stop.set()
                return None
            return Claim(len(times), str(len(times)))

        with patch.object(self.coordinator, 'claim', side_effect=claim):
            self.run_scheduler()
        self.assertEqual(times, [0] * 5)
        self.assertEqual(self.clock.waits, [])

    def test_renewal_continues_while_draining_on_shutdown(self) -> None:
        self.coordinator.lease_seconds = 3
        pending: Future[Outcome] = Future()
        self.handlers.submit.return_value = pending
        renewals: list[float] = []

        def claim(worker: Worker) -> Claim:
            self.coordinator._stop.set()
            return Claim(1, 'token')

        def renew(worker: Worker, claims: object) -> None:
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

        def claim(worker: Worker) -> Claim | None:
            times.append(self.clock.now)
            if len(times) == 1:
                return Claim(1, 'token')
            if len(times) == 6:
                self.coordinator._stop.set()
                pending.set_result(Finished())
            return None

        def renew(worker: Worker, claims: object) -> None:
            renewals.append(self.clock.now)

        with patch.object(self.coordinator, 'claim', side_effect=claim), patch.object(self.coordinator, 'renew', side_effect=renew):
            self.run_scheduler()
        self.assertEqual(times, [0, 0, .25, .75, 1.75, 3.75])
        self.assertEqual(renewals, [1, 2, 3])

    def test_maximum_poll_interval_validation(self) -> None:
        with self.assertRaises(ValueError):
            Coordinator(sessionmaker(self.engine), database_url=self.engine.url, poll_seconds=1, max_poll_seconds=.5)
