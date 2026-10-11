"""Verify eager process readiness and cleanup without synthetic business jobs."""

import multiprocessing
import tempfile
import threading
import unittest
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from dbworker import Coordinator, Finished


class Base(DeclarativeBase):
    pass


class Source(Base):
    __tablename__ = "startup_source"
    id: Mapped[int] = mapped_column(primary_key=True)


def handle(source, session):
    return Finished()


class StartupTests(unittest.TestCase):
    def test_idle_start_prepares_all_processes_without_business_jobs(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = create_engine(f"sqlite:///{Path(directory) / 'jobs.db'}")
            self.addCleanup(engine.dispose)
            Base.metadata.create_all(engine)
            runtime = Coordinator(sessionmaker(engine), database_url=engine.url)
            runtime.transactional_worker(name="startup", source=Source, concurrency=3)(handle)
            runtime.create_worker_tables()
            self.addCleanup(runtime.stop)
            before = {child.pid for child in multiprocessing.active_children()}
            runtime.start()
            children = [child for child in multiprocessing.active_children() if child.pid not in before]
            self.assertEqual(len(children), 3)
            self.assertTrue(all(child.is_alive() for child in children))
            with engine.connect() as connection:
                self.assertEqual(connection.scalar(select(func.count()).select_from(Source)), 0)
                self.assertEqual(connection.scalar(select(func.count()).select_from(
                    runtime.workers["startup"].table)), 0)
            runtime.stop()
            self.assertFalse(any(child.is_alive() for child in children))

    def test_initializer_failure_aborts_start_and_cleans_children(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = create_engine(f"sqlite:///{Path(directory) / 'jobs.db'}")
            self.addCleanup(engine.dispose)
            runtime = Coordinator(sessionmaker(engine), database_url=engine.url,
                                  engine_options={"invalid_startup_option": True})
            runtime.transactional_worker(name="startup", source=Source, concurrency=2)(handle)
            before = {child.pid for child in multiprocessing.active_children()}
            with self.assertRaises((threading.BrokenBarrierError, BrokenProcessPool)):
                runtime.start()
            self.assertFalse(runtime._running)
            self.assertEqual({child.pid for child in multiprocessing.active_children()}, before)
