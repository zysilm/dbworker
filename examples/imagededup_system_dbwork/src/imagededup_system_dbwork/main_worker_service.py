"""Standalone coordinator service, independent of the FastAPI process."""

import signal
from threading import Event
from types import FrameType

from imagededup_system_dbwork.db.engine import Base
from imagededup_system_dbwork.workers import coordinator, engine


def run() -> None:
    stopped = Event()

    def stop(signum: int, frame: FrameType | None) -> None:
        stopped.set()

    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        Base.metadata.create_all(engine)
        coordinator.create_worker_tables()
        coordinator.start()
        stopped.wait()
    finally:
        try:
            coordinator.stop()
        finally:
            engine.dispose()
            for sig, handler in previous.items():
                signal.signal(sig, handler)


if __name__ == "__main__":
    run()
