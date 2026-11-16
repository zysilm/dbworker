"""Database-owned work, independent of application models and result storage."""

from __future__ import annotations

import logging
import multiprocessing
import pickle  # nosec B403 -- used only for local serialization checks; this module never deserializes foreign pickle data.
import re
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from functools import cached_property
from enum import StrEnum
from datetime import datetime, timedelta, timezone
from typing import Any, TypeAlias, TypeVar, cast
from multiprocessing.util import Finalize

from sqlalchemy import Column, DateTime, Enum, ForeignKey, Integer, String, Table, Text, and_, create_engine, event, exists, inspect, insert, or_, select, tuple_, update
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.engine import CursorResult, RowMapping, URL, make_url
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.orm import DeclarativeBase, Mapper, Session, sessionmaker
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import ExecutableDDLElement
from sqlalchemy.sql import ColumnElement, Select, Update
from sqlalchemy.sql.selectable import Exists

logger = logging.getLogger(__name__)

_SQLITE_FINALIZATION_RETRY_SECONDS = 5.0
_MAX_RETRY_BACKOFF_SECONDS = 300.0


def now() -> datetime:
    # Database columns use naive UTC on every supported backend.
    return datetime.now(timezone.utc).replace(tzinfo=None)


class ExecutionStatus(StrEnum):
    WORKING = "working"
    UNFINISHED = "unfinished"
    FINISHED = "finished"
    FAILED = "failed"


@dataclass(frozen=True)
class Finished:
    pass


@dataclass(frozen=True)
class Unfinished:
    pass


Outcome: TypeAlias = Finished | Unfinished
Handler: TypeAlias = Callable[[Any, Session], Outcome]
_Function = TypeVar("_Function", bound=Callable[..., Any])


@dataclass(frozen=True)
class Claim:
    source_id: object
    token: str


class LostClaim(Exception):
    """The result belongs to an execution whose ownership has been replaced."""


class _Worker:
    def __init__(
        self,
        *,
        name: str,
        source: type[DeclarativeBase],
        handler: Handler,
        eligible: Callable[[], Select[tuple[Any]]] | None = None,
        concurrency: int = 1,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 1.0,
    ) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("Worker name must contain lowercase letters, digits, and underscores")
        if concurrency < 1:
            raise ValueError("Worker concurrency must be positive")
        if max_attempts < 1:
            raise ValueError("Worker max_attempts must be positive")
        if retry_backoff_seconds < 0:
            raise ValueError("Worker retry backoff must not be negative")
        self.name = name
        self.source = source
        self.handler = handler
        self.eligible = eligible or (lambda: select(source))
        self.concurrency = concurrency
        self.max_attempts = max_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        mapper = cast(Mapper[Any], inspect(source))
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
                Column("execution_status", Enum(
                    ExecutionStatus, native_enum=False,
                    values_callable=lambda enum: [member.value for member in enum],
                    validate_strings=True,
                ), nullable=False),
                Column("claim_token", String(36)),
                Column("lease_expires_at", DateTime, index=True),
                Column("error", Text),
                # Consecutive handler failures; a committed outcome resets it.
                Column("attempts", Integer, nullable=False, default=0, server_default="0"),
                # A failed attempt is not claimable again before this time.
                Column("available_at", DateTime),
            )

    def state(self, session: Session, source_id: object) -> RowMapping | None:
        return session.execute(select(self.table).where(self.table.c.source_id == source_id)).mappings().first()

    def reset_failed(self, session: Session, source_id: object) -> bool:
        result = cast(CursorResult[Any], session.execute(
            update(self.table)
            .where(self.table.c.source_id == source_id, self.table.c.execution_status == ExecutionStatus.FAILED)
            .values(execution_status=ExecutionStatus.UNFINISHED, error=None, claim_token=None, lease_expires_at=None,
                    attempts=0, available_at=None)
        ))
        return result.rowcount == 1


class _AddColumn(ExecutableDDLElement):
    """ALTER TABLE ... ADD COLUMN for a work table created by an earlier release."""

    def __init__(self, column: Column[Any]) -> None:
        self.column = column


@compiles(_AddColumn)
def _compile_add_column(element: _AddColumn, compiler: Any, **kw: Any) -> str:
    return (f"ALTER TABLE {compiler.preparer.format_table(element.column.table)} "
            f"ADD COLUMN {compiler.get_column_specification(element.column)}")


@dataclass(frozen=True)
class _WorkerDefinition:
    """Importable callables and model classes, never live database resources."""

    name: str
    source: type[DeclarativeBase]
    handler: Handler


_process_worker: _Worker | None = None
_process_session_factory: sessionmaker[Session] | None = None


def _initialize_process(
    database_url: str | URL,
    engine_options: dict[str, Any],
    definitions: tuple[_WorkerDefinition, ...],
    worker_name: str,
    ready: Any,
) -> None:
    global _process_worker, _process_session_factory
    try:
        engine = create_engine(database_url, **engine_options)
        _process_session_factory = sessionmaker(engine, expire_on_commit=False)
        Finalize(None, engine.dispose, exitpriority=10)
        # Rebuild registered work-table metadata locally, including dependencies used
        # by application queries. Eligibility callbacks stay in the parent process.
        for definition in definitions:
            worker = _Worker(name=definition.name, source=definition.source, handler=definition.handler)
            if definition.name == worker_name:
                _process_worker = worker
        ready.wait(timeout=180)
    except BaseException:
        ready.abort()
        raise


def _process_ready() -> None:
    """Start the executor without claiming or executing an application job."""


def _execute_in_process(claim: Claim) -> Outcome:
    if _process_worker is None or _process_session_factory is None:
        raise RuntimeError("Handler process has not been initialized")
    try:
        return _execute_claim(_process_worker, claim, _process_session_factory)
    except Exception as exc:
        # An exception the parent cannot rebuild (for example a required
        # multi-argument __init__) would break the whole process pool there.
        try:
            pickle.loads(pickle.dumps(exc))  # nosec B301 -- round-trips an object this process just created.
        except Exception:
            raise RuntimeError(f"{type(exc).__qualname__}: {exc}") from exc
        raise


def _retry_sqlite_completion(session: Session, completion: Update, error: OperationalError) -> CursorResult[Any]:
    """Wait for a writer without discarding business writes or replaying a handler."""
    deadline = time.monotonic() + _SQLITE_FINALIZATION_RETRY_SECONDS
    delay = 0.01
    while True:
        # A busy SQLite statement leaves the transaction intact. A stale WAL
        # snapshot cannot become writable by waiting (SQLITE_BUSY_SNAPSHOT).
        if (not isinstance(error.orig, sqlite3.OperationalError)
                or getattr(error.orig, "sqlite_errorcode", None) not in (
                    sqlite3.SQLITE_BUSY, sqlite3.SQLITE_BUSY_RECOVERY, sqlite3.SQLITE_BUSY_TIMEOUT,
                )
                or error.connection_invalidated or not session.is_active):
            raise error
        connection = session.connection()
        driver = connection.connection.driver_connection
        if (connection.dialect.name != "sqlite"
                or driver is None or not driver.in_transaction):
            raise error
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise error
        time.sleep(min(delay, remaining))
        if time.monotonic() >= deadline:
            raise error
        delay = min(delay * 2, 0.1)
        try:
            return cast(CursorResult[Any], session.execute(completion))
        except OperationalError as exc:
            error = exc


def _execute_claim(worker: _Worker, claim: Claim, session_factory: sessionmaker[Session]) -> Outcome:
    """Run a whole handler and atomically persist its writes and outcome."""
    with session_factory() as session:
        def prevent_handler_commit(session: Session) -> None:
            raise RuntimeError("The worker commits the handler session; return Finished() or Unfinished() instead")

        event.listen(session, "before_commit", prevent_handler_commit)
        try:
            source = session.get(worker.source, claim.source_id)
            if source is None:
                raise ValueError("Worker source no longer exists")
            outcome = worker.handler(source, session)
            if not isinstance(outcome, (Finished, Unfinished)):
                raise TypeError("A worker handler must return Finished() or Unfinished()")
            # All handler writes, including already-flushed SQL, are still in
            # this transaction. Rejecting ownership rolls them all back.
            with session.no_autoflush:
                completion = update(worker.table).where(
                    worker.table.c.source_id == claim.source_id,
                    worker.table.c.execution_status == ExecutionStatus.WORKING,
                    worker.table.c.claim_token == claim.token,
                ).values(execution_status=ExecutionStatus.FINISHED if isinstance(outcome, Finished) else ExecutionStatus.UNFINISHED,
                         claim_token=None, lease_expires_at=None, error=None, attempts=0, available_at=None)
                try:
                    result = cast(CursorResult[Any], session.execute(completion))
                except OperationalError as exc:
                    result = _retry_sqlite_completion(session, completion, exc)
            if result.rowcount != 1:
                raise LostClaim()
        finally:
            event.remove(session, "before_commit", prevent_handler_commit)
        session.commit()
        return outcome


class Coordinator:
    """Parent-process claiming, scheduling and lease renewal.

    Entire handlers execute in spawned processes with their own engines/sessions.
    database_url and engine_options configure those independent child engines.
    """

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        *,
        database_url: str | URL,
        engine_options: dict[str, Any] | None = None,
        lease_seconds: float = 30,
        poll_seconds: float = 0.25,
        max_poll_seconds: float = 10,
    ) -> None:
        if lease_seconds <= 0 or poll_seconds <= 0:
            raise ValueError("Lease and poll intervals must be positive")
        if max_poll_seconds < poll_seconds:
            raise ValueError("Maximum poll interval must be at least the initial interval")
        self.database_url = database_url
        self.engine_options = dict(engine_options or {})
        self.session_factory = session_factory
        self.lease_seconds = lease_seconds
        self.poll_seconds = poll_seconds
        self.max_poll_seconds = max_poll_seconds
        self.workers: dict[str, _Worker] = {}
        self._running: dict[str, tuple[threading.Thread, ProcessPoolExecutor]] = {}
        self._definitions: tuple[_WorkerDefinition, ...] = ()
        self._stop = threading.Event()
        self._wakeups: dict[str, threading.Event] = {}

    @cached_property
    def _supports_skip_locked(self) -> bool:
        # Resolve on the first claim so importing decorated handlers in spawned
        # processes does not open a parent-coordinator database connection.
        with self.session_factory() as session:
            dialect = session.connection().dialect
            version = dialect.server_version_info or ()
            return (
                dialect.name == "postgresql" and version >= (9, 5)
                or dialect.name == "mysql" and not getattr(dialect, "is_mariadb", False) and version >= (8, 0, 1)
                or dialect.name in ("mysql", "mariadb") and getattr(dialect, "is_mariadb", False) and version >= (10, 6)
            )

    def transactional_worker(
        self,
        *,
        name: str,
        source: type[DeclarativeBase],
        eligible: Callable[[], Select[tuple[Any]]] | None = None,
        concurrency: int = 1,
        max_attempts: int = 3,
        retry_backoff_seconds: float = 1.0,
    ) -> Callable[[_Function], _Function]:
        """Register a handler whose session and final commit belong to the runtime.

        A handler exception rolls back that attempt. The work is retried after
        retry_backoff_seconds (doubling per consecutive failure) and is marked
        FAILED once max_attempts consecutive attempts have raised. Use
        max_attempts=1 to fail on the first exception.

        Registration creates worker metadata, but starts no processing. The
        original function is returned unchanged; direct calls are not managed.
        """
        def decorate(handler: _Function) -> _Function:
            if self._running:
                raise RuntimeError("Register workers before starting the coordinator")
            if name in self.workers:
                raise ValueError(f"Duplicate worker name: {name}")
            self.workers[name] = _Worker(
                name=name, source=source, handler=handler,
                eligible=eligible, concurrency=concurrency,
                max_attempts=max_attempts, retry_backoff_seconds=retry_backoff_seconds,
            )
            return handler

        return decorate

    def execution_status(
        self, session: Session, *, worker: str, source_id: object,
    ) -> ExecutionStatus | None:
        """Return the recorded execution status, or None if never claimed.

        Lease expiry does not change the recorded status. Unknown worker names
        raise KeyError, rather than being confused with an unclaimed source.
        """
        table = self.workers[worker].table
        value = session.scalar(select(table.c.execution_status).where(table.c.source_id == source_id))
        return ExecutionStatus(value) if value is not None else None

    def has_execution_status(
        self, *, worker: str, source_id: object, statuses: Iterable[ExecutionStatus],
    ) -> Exists:
        """Build a SQL predicate for a source ID or SQLAlchemy source-ID expression.

        An unclaimed source matches no status; an empty status set matches none.
        This only constructs SQL and does not open a database connection.
        """
        table = self.workers[worker].table
        return exists().where(
            table.c.source_id == source_id,
            table.c.execution_status.in_(tuple(statuses)),
        ).correlate_except(table)

    def create_worker_tables(self) -> None:
        """Create missing work tables and add columns introduced by later releases."""
        with self.session_factory() as session:
            bind = session.get_bind()
        for worker in self.workers.values():
            table = worker.table
            table.create(bind, checkfirst=True)

            def present(column: Column[Any]) -> bool:
                # Probe with ordinary SQL rather than reflection, so the session's
                # schema translation and search path apply as they do to claims.
                try:
                    with self.session_factory() as session:
                        session.execute(select(column).limit(0))
                    return True
                except DBAPIError:
                    return False

            for column in (table.c.attempts, table.c.available_at):
                if present(column):
                    continue
                try:
                    with self.session_factory.begin() as session:
                        session.connection().execute(_AddColumn(column))
                except DBAPIError:
                    # Another service may have added it while starting up.
                    if not present(column):
                        raise

    def _available(self, table: Table, timestamp: datetime) -> ColumnElement[bool]:
        return or_(
            and_(table.c.execution_status == ExecutionStatus.UNFINISHED,
                 or_(table.c.available_at.is_(None), table.c.available_at <= timestamp)),
            and_(table.c.execution_status == ExecutionStatus.WORKING, table.c.lease_expires_at < timestamp),
        )

    def _candidate(self, worker: _Worker, timestamp: datetime) -> Select[tuple[Any]]:
        table = worker.table
        # Keep availability separate from application eligibility. Joining work
        # state onto the eligibility query can make SQLite evaluate expensive
        # correlated predicates for every finished source before rejecting it.
        available_ids = (
            select(worker.source_key)
            .outerjoin(table, table.c.source_id == worker.source_key)
            .where(or_(table.c.source_id.is_(None), self._available(table, timestamp)))
        )
        return (
            worker.eligible()
            .with_only_columns(worker.source_key, maintain_column_froms=True)
            .where(worker.source_key.in_(available_ids))
            .limit(1)
        )

    def _record_claim(
        self, session: Session, worker: _Worker, source_id: object | None, timestamp: datetime,
    ) -> Claim | None:
        if source_id is None:
            return None
        table = worker.table
        token = str(uuid.uuid4())
        values: dict[str, object] = dict(execution_status=ExecutionStatus.WORKING, claim_token=token,
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

    def _record_claims(
        self, session: Session, worker: _Worker, source_ids: Iterable[object], timestamp: datetime,
    ) -> list[Claim]:
        ids = list(source_ids)
        if not ids:
            return []
        if session.get_bind().dialect.name != "postgresql":
            return [claim for source_id in ids
                    if (claim := self._record_claim(session, worker, source_id, timestamp)) is not None]
        table = worker.table
        expires = now() + timedelta(seconds=self.lease_seconds)
        values = [
            dict(source_id=source_id, execution_status=ExecutionStatus.WORKING,
                 claim_token=str(uuid.uuid4()), lease_expires_at=expires, error=None)
            for source_id in ids
        ]
        statement = postgresql_insert(table).values(values[0] if len(values) == 1 else values)
        statement = statement.on_conflict_do_update(
            index_elements=[table.c.source_id],
            set_={name: statement.excluded[name] for name in
                  ("execution_status", "claim_token", "lease_expires_at", "error")},
            where=self._available(table, timestamp),
        ).returning(table.c.source_id, table.c.claim_token)
        tokens = {source_id: token for source_id, token in session.execute(statement).tuples()}
        # RETURNING order is not an eligibility-order guarantee.
        return [Claim(source_id, tokens[source_id]) for source_id in ids if source_id in tokens]

    def _claim_with_db_lock(self, worker: _Worker) -> Claim | None:
        timestamp = now()
        with self.session_factory.begin() as session:
            # Lock the source row, which exists even before the first work row.
            source_id = session.scalar(self._candidate(worker, timestamp).with_for_update(
                skip_locked=True, of=worker.source_table,
            ))
            return self._record_claim(session, worker, source_id, timestamp)

    def _claim_with_conditional_update(self, worker: _Worker) -> Claim | None:
        timestamp = now()
        with self.session_factory.begin() as session:
            source_id = session.scalar(self._candidate(worker, timestamp))
            return self._record_claim(session, worker, source_id, timestamp)

    def claim(self, worker: _Worker) -> Claim | None:
        if self._supports_skip_locked:
            return self._claim_with_db_lock(worker)
        return self._claim_with_conditional_update(worker)

    def _claim_many(self, worker: _Worker, limit: int) -> list[Claim]:
        """Commit ownership for free execution slots before dispatching work."""
        if limit == 1:
            claim = self.claim(worker)
            return [] if claim is None else [claim]
        supports_skip_locked = self._supports_skip_locked
        timestamp = now()
        with self.session_factory.begin() as session:
            connection = session.connection()
            if (connection.dialect.name == "sqlite"
                    and not connection.connection.driver_connection.in_transaction):
                # Legacy sqlite3 transaction control does not begin on SELECT.
                # Without an outer transaction, releasing the first insertion
                # savepoint would commit it even if a later claim fails.
                connection.exec_driver_sql("BEGIN IMMEDIATE")
            query = self._candidate(worker, timestamp).limit(limit)
            if supports_skip_locked:
                query = query.with_for_update(skip_locked=True, of=worker.source_table)
            # Custom eligibility joins may return the same source more than once.
            source_ids = dict.fromkeys(session.scalars(query))
            claims = self._record_claims(session, worker, source_ids, timestamp)
            if claims:
                # Earlier records must not enter the queue with leases aged by
                # the rest of this batch. Refresh while ownership is still locked.
                table = worker.table
                session.execute(update(table).where(
                    table.c.execution_status == ExecutionStatus.WORKING,
                    tuple_(table.c.source_id, table.c.claim_token).in_(
                        [(claim.source_id, claim.token) for claim in claims]),
                ).values(lease_expires_at=now() + timedelta(seconds=self.lease_seconds)))
            return claims

    def renew(self, worker: _Worker, claims: Iterable[Claim]) -> None:
        table = worker.table
        with self.session_factory.begin() as session:
            for claim in claims:
                session.execute(update(table).where(
                    table.c.source_id == claim.source_id, table.c.execution_status == ExecutionStatus.WORKING,
                    table.c.claim_token == claim.token,
                ).values(lease_expires_at=now() + timedelta(seconds=self.lease_seconds)))

    def _fail(self, worker: _Worker, claim: Claim, error: Exception) -> None:
        """Record a handler exception: schedule a retry, or FAILED when attempts run out."""
        table = worker.table
        owned = (table.c.source_id == claim.source_id, table.c.execution_status == ExecutionStatus.WORKING,
                 table.c.claim_token == claim.token)
        with self.session_factory.begin() as session:
            # Only the claim owner changes this row, so read-then-write is safe;
            # a replaced claim matches neither statement.
            attempts = session.scalar(select(table.c.attempts).where(*owned))
            if attempts is None:
                return
            attempts += 1
            retry = attempts < worker.max_attempts
            delay = min(worker.retry_backoff_seconds * 2 ** (attempts - 1), _MAX_RETRY_BACKOFF_SECONDS)
            session.execute(update(table).where(*owned).values(
                execution_status=ExecutionStatus.UNFINISHED if retry else ExecutionStatus.FAILED,
                error=str(error), claim_token=None, lease_expires_at=None, attempts=attempts,
                available_at=now() + timedelta(seconds=delay) if retry else None,
            ))
        if retry:
            logger.warning("Worker %s will retry %s in %.1fs (attempt %d of %d failed)",
                           worker.name, claim.source_id, delay, attempts, worker.max_attempts)

    def _release(self, worker: _Worker, claims: Iterable[Claim], error: Exception) -> None:
        """Requeue work whose handler process was lost, without consuming an attempt.

        A broken pool does not say which handler caused it, so no claim is blamed.
        """
        table = worker.table
        try:
            with self.session_factory.begin() as session:
                for claim in claims:
                    session.execute(update(table).where(
                        table.c.source_id == claim.source_id, table.c.execution_status == ExecutionStatus.WORKING,
                        table.c.claim_token == claim.token,
                    ).values(execution_status=ExecutionStatus.UNFINISHED, error=str(error), claim_token=None,
                             lease_expires_at=None,
                             available_at=now() + timedelta(seconds=worker.retry_backoff_seconds)))
        except Exception:
            logger.exception("Could not requeue work for %s; it becomes claimable when its lease expires", worker.name)

    def _start_pool(self, worker: _Worker) -> ProcessPoolExecutor:
        context = multiprocessing.get_context("spawn")
        ready = context.Barrier(worker.concurrency + 1)
        handlers = ProcessPoolExecutor(
            max_workers=worker.concurrency, mp_context=context,
            initializer=_initialize_process,
            initargs=(self.database_url, self.engine_options, self._definitions, worker.name, ready),
        )
        try:
            # Initializers hold the startup barrier, so every configured
            # process must start before any probe can finish. These probes
            # create no source rows, claims, or execution-ledger entries.
            probes = [handlers.submit(_process_ready) for _ in range(worker.concurrency)]
            ready.wait(timeout=180)
            for probe in probes:
                probe.result()
        except BaseException:
            ready.abort()
            handlers.shutdown(wait=True, cancel_futures=True)
            raise
        return handlers

    def _replace_pool(self, worker: _Worker, broken: ProcessPoolExecutor) -> ProcessPoolExecutor:
        """Start fresh handler processes after the executor lost one."""
        broken.shutdown(wait=False, cancel_futures=True)
        handlers = self._start_pool(worker)
        thread, _ = self._running[worker.name]
        self._running[worker.name] = (thread, handlers)
        return handlers

    def _run(
        self, worker: _Worker, handlers: ProcessPoolExecutor,
        wakeup: threading.Event,
    ) -> None:
        # No database or process error may end this loop: a dead scheduler
        # stops claiming and renewing while the coordinator still looks alive.
        active: dict[Future[Outcome], Claim] = {}
        next_renewal = time.monotonic()
        next_claim_at = 0.0
        delay = self.poll_seconds
        # A lost handler process breaks the pool and fails every in-flight
        # future without saying which handler caused it. Work lost together is
        # requeued unblamed, then run one at a time (see suspects) so that a
        # handler which keeps killing its process is charged alone.
        broken: BrokenProcessPool | None = None
        lost: list[Claim] = []
        suspects: dict[object, float] = {}

        def record_failure(claim: Claim, error: Exception) -> None:
            try:
                self._fail(worker, claim, error)
            except Exception:
                logger.exception("Could not record the failure of %s:%s; it becomes claimable "
                                 "when its lease expires", worker.name, claim.source_id)

        while not self._stop.is_set() or active:
            # Clear before inspecting futures so a completion during this iteration
            # still wakes the wait below.
            wakeup.clear()
            for future in list(active):
                if future.done():
                    claim = active.pop(future)
                    # Completion can publish eligible children after an empty query.
                    next_claim_at = 0.0
                    try:
                        future.result()
                        delay = self.poll_seconds
                    except LostClaim:
                        logger.info("Discarded stale result for %s:%s", worker.name, claim.source_id)
                    except BrokenProcessPool as exc:
                        broken = exc
                        lost.append(claim)
                        continue
                    except Exception as exc:
                        logger.exception("Worker %s failed for %s", worker.name, claim.source_id)
                        record_failure(claim, exc)
                    suspects.pop(claim.source_id, None)
            if broken is not None and lost and not active:
                if len(lost) == 1:
                    logger.error("Worker %s lost its handler process while running only %s",
                                 worker.name, lost[0].source_id)
                    record_failure(lost[0], broken)
                    suspects.pop(lost[0].source_id, None)
                else:
                    logger.warning("Worker %s lost a handler process while running %s; requeueing them "
                                   "to run one at a time", worker.name, [claim.source_id for claim in lost])
                    self._release(worker, lost, broken)
                    expiry = time.monotonic() + self.lease_seconds + worker.retry_backoff_seconds
                    suspects.update((claim.source_id, expiry) for claim in lost)
                lost = []
            if time.monotonic() >= next_renewal:
                next_renewal = time.monotonic() + self.lease_seconds / 3
                if active:
                    try:
                        self.renew(worker, active.values())
                    except Exception:
                        logger.exception("Error renewing leases for %s; retrying", worker.name)
                        next_renewal = time.monotonic() + self.lease_seconds / 10
            # Suspects not seen again (no longer eligible, or claimed elsewhere) are forgotten.
            for source_id in [source_id for source_id, expiry in suspects.items() if time.monotonic() >= expiry]:
                del suspects[source_id]
            capacity = 1 if suspects else worker.concurrency
            # A broken pool is replaced once its remaining futures have reported.
            claiming = not self._stop.is_set() and not (broken is not None and active)
            if claiming and time.monotonic() >= next_claim_at:
                if broken is not None:
                    try:
                        handlers = self._replace_pool(worker, handlers)
                        broken = None
                        logger.warning("Restarted handler processes for %s", worker.name)
                    except Exception:
                        logger.exception("Could not restart handler processes for %s; retrying after backoff",
                                         worker.name)
                        next_claim_at = time.monotonic() + delay
                        delay = min(delay * 2, self.max_poll_seconds)
                while broken is None and len(active) < capacity and not self._stop.is_set():
                    try:
                        claims = self._claim_many(worker, capacity - len(active))
                    except Exception:
                        logger.exception("Error claiming work for %s; retrying after backoff", worker.name)
                        claims = []
                    if not claims:
                        next_claim_at = time.monotonic() + delay
                        delay = min(delay * 2, self.max_poll_seconds)
                        break
                    delay = self.poll_seconds
                    next_claim_at = 0.0
                    for index, claim in enumerate(claims):
                        try:
                            future = handlers.submit(_execute_in_process, claim)
                        except BrokenProcessPool as exc:
                            logger.warning("Handler process pool for %s is broken; requeueing undispatched work",
                                           worker.name)
                            broken = exc
                            self._release(worker, claims[index:], exc)
                            break
                        active[future] = claim
                        future.add_done_callback(lambda completed: wakeup.set())
            deadlines: list[float] = []
            if active:
                deadlines.append(next_renewal)
            if not self._stop.is_set() and not (broken is not None and active) and len(active) < capacity:
                deadlines.append(next_claim_at)
            if not deadlines:
                break
            wakeup.wait(max(0.0, min(deadlines) - time.monotonic()))

    def start(self) -> None:
        if self._running:
            raise RuntimeError("Coordinator already started")
        url = make_url(self.database_url)
        if url.get_backend_name() == "sqlite" and (url.database in (None, "", ":memory:") or url.query.get("mode") == "memory"):
            raise ValueError("Handler processes require file-backed SQLite, not an in-memory database")
        definitions = tuple(_WorkerDefinition(w.name, w.source, w.handler) for w in self.workers.values())
        try:
            pickle.dumps((self.database_url, self.engine_options, definitions))
        except (TypeError, AttributeError, pickle.PicklingError) as exc:
            raise ValueError("Handlers and models must be importable; bound handler arguments and engine options must be serializable") from exc
        self._definitions = definitions
        self._stop.clear()
        for worker in self.workers.values():
            try:
                handlers = self._start_pool(worker)
            except BaseException:
                self.stop()
                raise
            wakeup = threading.Event()
            self._wakeups[worker.name] = wakeup
            thread = threading.Thread(target=self._run, args=(worker, handlers, wakeup), name=worker.name, daemon=True)
            self._running[worker.name] = (thread, handlers)
            thread.start()

    def stop(self) -> None:
        self._stop.set()
        for wakeup in self._wakeups.values():
            wakeup.set()
        for thread, _ in list(self._running.values()):
            thread.join()
        # A scheduler may have replaced its pool; shut down the current one.
        for _, handlers in self._running.values():
            handlers.shutdown(wait=True)
        self._running.clear()
        self._wakeups.clear()
