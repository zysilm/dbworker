"""Database-owned work, independent of application models and result storage."""

from __future__ import annotations

import logging
import multiprocessing
import re
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from concurrent.futures import Executor, Future, ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Generic, ParamSpec, TypeAlias, TypeVar, cast

from sqlalchemy import Column, DateTime, ForeignKey, String, Table, Text, and_, inspect, insert, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import CursorResult, RowMapping
from sqlalchemy.orm import DeclarativeBase, Mapper, Session, sessionmaker
from sqlalchemy.sql import ColumnElement, Select

SourceT = TypeVar("SourceT", bound=DeclarativeBase)
ResultT = TypeVar("ResultT")
Parameters = ParamSpec("Parameters")

logger = logging.getLogger(__name__)


def now() -> datetime:
    # Database columns use naive UTC on every supported backend.
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass(frozen=True)
class Finished:
    pass


@dataclass(frozen=True)
class Unfinished:
    pass


Outcome: TypeAlias = Finished | Unfinished


@dataclass(frozen=True)
class Claim:
    source_id: object
    token: str


class LostClaim(Exception):
    """The result belongs to an execution whose ownership has been replaced."""


class Worker(Generic[SourceT]):
    def __init__(
        self,
        *,
        name: str,
        source: type[SourceT],
        handler: Callable[[SourceT, Context[SourceT]], Outcome],
        eligible: Callable[[], Select[tuple[SourceT]]] | None = None,
        concurrency: int = 1,
    ) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("Worker name must contain lowercase letters, digits, and underscores")
        if concurrency < 1:
            raise ValueError("Worker concurrency must be positive")
        self.name = name
        self.source = source
        self.handler = handler
        self.eligible = eligible or (lambda: select(source))
        self.concurrency = concurrency
        mapper = cast(Mapper[SourceT], inspect(source))
        if len(mapper.primary_key) != 1:
            raise ValueError("Worker sources must have one primary-key column")
        self.source_key = cast(Column[Any], mapper.primary_key[0])
        self.source_table = cast(Table, mapper.local_table)
        metadata = self.source_table.metadata
        table_name = f"{name}_work"
        if table_name in metadata.tables:
            self.table = metadata.tables[table_name]
            references = list(self.table.c.source_id.foreign_keys)
            if len(references) != 1 or references[0].column is not self.source_key:
                raise ValueError(f"Worker table {table_name} references another source")
        else:
            self.table = Table(
                table_name, metadata,
                Column("source_id", self.source_key.type.copy(), ForeignKey(self.source_key), primary_key=True),
                Column("status", String(20), nullable=False),
                Column("claim_token", String(36)),
                Column("lease_expires_at", DateTime, index=True),
                Column("error", Text),
            )

    def state(self, session: Session, source_id: object) -> RowMapping | None:
        return session.execute(select(self.table).where(self.table.c.source_id == source_id)).mappings().first()

    def reset_failed(self, session: Session, source_id: object) -> bool:
        result = cast(CursorResult[Any], session.execute(
            update(self.table)
            .where(self.table.c.source_id == source_id, self.table.c.status == "failed")
            .values(status="unfinished", error=None, claim_token=None, lease_expires_at=None)
        ))
        return result.rowcount == 1


class Context(Generic[SourceT]):
    """One handler invocation. The coordinator commits after the handler returns.

    read() supplies a short-lived read session. cpu() runs a pure function in
    the process pool. Access session only for the final save: it acquires write
    protection, and its transaction remains open until the outcome is committed.
    Handlers must not commit/rollback this session or perform CPU work after it
    has been opened.
    """

    def __init__(
        self,
        coordinator: Coordinator,
        worker: Worker[SourceT],
        claim: Claim,
        cpu_pool: Executor,
    ) -> None:
        self._coordinator = coordinator
        self.worker = worker
        self.claim = claim
        self._cpu_pool = cpu_pool
        self._session: Session | None = None

    def read(self) -> Session:
        if self._session is not None:
            raise RuntimeError("Use the final save session once saving has begun")
        return self._coordinator.session_factory()

    def cpu(
        self,
        function: Callable[Parameters, ResultT],
        *args: Parameters.args,
        **kwargs: Parameters.kwargs,
    ) -> ResultT:
        if self._session is not None:
            raise RuntimeError("CPU work must precede the final save transaction")
        return self._cpu_pool.submit(function, *args, **kwargs).result()

    @property
    def session(self) -> Session:
        if self._session is None:
            session = self._coordinator.session_factory()
            table = self.worker.table
            try:
                result = cast(CursorResult[Any], session.execute(
                    update(table).where(
                        table.c.source_id == self.claim.source_id,
                        table.c.status == "working",
                        table.c.claim_token == self.claim.token,
                    ).values(status="unfinished", claim_token=None, lease_expires_at=None)
                ))
                if result.rowcount != 1:
                    raise LostClaim()
            except BaseException:
                session.rollback()
                session.close()
                raise
            self._session = session
        return self._session

    def finish(self, outcome: Outcome) -> None:
        if not isinstance(outcome, (Finished, Unfinished)):
            raise TypeError("A worker handler must return Finished() or Unfinished()")
        session = self.session
        session.execute(
            update(self.worker.table)
            .where(self.worker.table.c.source_id == self.claim.source_id)
            .values(status="finished" if isinstance(outcome, Finished) else "unfinished", error=None)
        )
        session.commit()

    def close(self) -> None:
        if self._session is not None:
            self._session.close()  # Rolls back an unsuccessful save.


class Coordinator:
    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        lease_seconds: float = 30,
        poll_seconds: float = 0.25,
    ) -> None:
        if lease_seconds <= 0 or poll_seconds <= 0:
            raise ValueError("Lease and poll intervals must be positive")
        self.session_factory = session_factory
        self.lease_seconds = lease_seconds
        self.poll_seconds = poll_seconds
        # A registry contains different mapped source types; individual Worker
        # instances retain the source/handler type relationship.
        self.workers: dict[str, Worker[Any]] = {}
        self._running: dict[str, tuple[threading.Thread, ThreadPoolExecutor, ProcessPoolExecutor]] = {}
        self._stop = threading.Event()
        with session_factory() as session:
            dialect = session.connection().dialect
            version = dialect.server_version_info or ()
            self._supports_skip_locked = (
                dialect.name == "postgresql" and version >= (9, 5)
                or dialect.name == "mysql" and not getattr(dialect, "is_mariadb", False) and version >= (8, 0, 1)
                or dialect.name in ("mysql", "mariadb") and getattr(dialect, "is_mariadb", False) and version >= (10, 6)
            )

    def register(self, worker: Worker[SourceT]) -> Worker[SourceT]:
        if self._running:
            raise RuntimeError("Register workers before starting the coordinator")
        if worker.name in self.workers:
            raise ValueError(f"Duplicate worker name: {worker.name}")
        self.workers[worker.name] = worker
        return worker

    def create_worker_tables(self) -> None:
        with self.session_factory() as session:
            for worker in self.workers.values():
                worker.table.create(session.get_bind(), checkfirst=True)

    def _available(self, table: Table, timestamp: datetime) -> ColumnElement[bool]:
        return or_(
            table.c.status == "unfinished",
            and_(table.c.status == "working", table.c.lease_expires_at < timestamp),
        )

    def _candidate(self, worker: Worker[SourceT], timestamp: datetime) -> Select[tuple[Any]]:
        table = worker.table
        return (
            worker.eligible()
            .with_only_columns(worker.source_key, maintain_column_froms=True)
            .outerjoin(table, table.c.source_id == worker.source_key)
            .where(or_(table.c.source_id.is_(None), self._available(table, timestamp)))
            .limit(1)
        )

    def _record_claim(
        self, session: Session, worker: Worker[SourceT], source_id: object | None, timestamp: datetime,
    ) -> Claim | None:
        if source_id is None:
            return None
        table = worker.table
        token = str(uuid.uuid4())
        values: dict[str, object] = dict(status="working", claim_token=token,
                      lease_expires_at=now() + timedelta(seconds=self.lease_seconds), error=None)
        existing = session.scalar(select(table.c.source_id).where(table.c.source_id == source_id))
        if existing is None:
            # The primary key arbitrates simultaneous first claims. A savepoint
            # lets us inspect a uniqueness conflict without poisoning the session.
            try:
                with session.begin_nested():
                    session.execute(insert(table).values(source_id=source_id, **values))
            except IntegrityError as exc:
                code = getattr(exc.orig, "sqlite_errorcode", None)
                sqlstate = getattr(exc.orig, "sqlstate", None) or getattr(exc.orig, "pgcode", None)
                mysql_code = exc.orig.args[0] if exc.orig is not None and exc.orig.args else None
                if code not in (1555, 2067) and sqlstate != "23505" and mysql_code != 1062:
                    raise
                return None
        else:
            result = cast(CursorResult[Any], session.execute(update(table).where(
                table.c.source_id == source_id, self._available(table, timestamp),
            ).values(**values)))
            if result.rowcount != 1:
                return None
        return Claim(source_id, token)

    def _claim_with_db_lock(self, worker: Worker[SourceT]) -> Claim | None:
        timestamp = now()
        with self.session_factory.begin() as session:
            # Lock the source row, which exists even before the first work row.
            source_id = session.scalar(self._candidate(worker, timestamp).with_for_update(
                skip_locked=True, of=worker.source_table,
            ))
            return self._record_claim(session, worker, source_id, timestamp)

    def _claim_with_conditional_update(self, worker: Worker[SourceT]) -> Claim | None:
        timestamp = now()
        with self.session_factory.begin() as session:
            source_id = session.scalar(self._candidate(worker, timestamp))
            return self._record_claim(session, worker, source_id, timestamp)

    def claim(self, worker: Worker[SourceT]) -> Claim | None:
        if self._supports_skip_locked:
            return self._claim_with_db_lock(worker)
        return self._claim_with_conditional_update(worker)

    def renew(self, worker: Worker[SourceT], claims: Iterable[Claim]) -> None:
        table = worker.table
        with self.session_factory.begin() as session:
            for claim in claims:
                session.execute(update(table).where(
                    table.c.source_id == claim.source_id, table.c.status == "working",
                    table.c.claim_token == claim.token,
                ).values(lease_expires_at=now() + timedelta(seconds=self.lease_seconds)))

    def execute_claim(
        self, worker: Worker[SourceT], claim: Claim, cpu_pool: Executor,
    ) -> Outcome:
        context = Context(self, worker, claim, cpu_pool)
        try:
            with self.session_factory() as session:
                source = session.get(worker.source, claim.source_id)
                if source is None:
                    raise ValueError("Worker source no longer exists")
            outcome = worker.handler(source, context)
            context.finish(outcome)
            return outcome
        finally:
            context.close()

    def _fail(self, worker: Worker[SourceT], claim: Claim, error: Exception) -> None:
        table = worker.table
        with self.session_factory.begin() as session:
            session.execute(update(table).where(
                table.c.source_id == claim.source_id, table.c.status == "working",
                table.c.claim_token == claim.token,
            ).values(status="failed", error=str(error), claim_token=None, lease_expires_at=None))

    def _run(
        self, worker: Worker[SourceT], handlers: ThreadPoolExecutor, cpu: ProcessPoolExecutor,
    ) -> None:
        active: dict[Future[Outcome], Claim] = {}
        next_renewal = time.monotonic()
        while not self._stop.is_set() or active:
            for future in list(active):
                if future.done():
                    claim = active.pop(future)
                    try:
                        future.result()
                    except LostClaim:
                        logger.info("Discarded stale result for %s:%s", worker.name, claim.source_id)
                    except Exception as exc:
                        logger.exception("Worker %s failed for %s", worker.name, claim.source_id)
                        self._fail(worker, claim, exc)
            if not self._stop.is_set():
                while len(active) < worker.concurrency:
                    next_claim = self.claim(worker)
                    if next_claim is None:
                        break
                    active[handlers.submit(self.execute_claim, worker, next_claim, cpu)] = next_claim
            if time.monotonic() >= next_renewal:
                self.renew(worker, active.values())
                next_renewal = time.monotonic() + self.lease_seconds / 3
            if self._stop.is_set():
                time.sleep(self.poll_seconds)
            else:
                self._stop.wait(self.poll_seconds)

    def start(self) -> None:
        if self._running:
            raise RuntimeError("Coordinator already started")
        self._stop.clear()
        for worker in self.workers.values():
            handlers = ThreadPoolExecutor(max_workers=worker.concurrency, thread_name_prefix=worker.name)
            cpu = ProcessPoolExecutor(max_workers=worker.concurrency, mp_context=multiprocessing.get_context("spawn"))
            thread = threading.Thread(target=self._run, args=(worker, handlers, cpu), name=worker.name, daemon=True)
            self._running[worker.name] = (thread, handlers, cpu)
            thread.start()

    def stop(self) -> None:
        self._stop.set()
        for thread, handlers, cpu in self._running.values():
            thread.join()
            handlers.shutdown(wait=True)
            cpu.shutdown(wait=True)
        self._running.clear()
