"""Replace SQL Lab scheduling while retaining its original command and task body."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass

from sqlalchemy import JSON, Integer, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from dbworker import Coordinator, Finished
from benchmarks.common.native_observer import job_context, record


class Base(DeclarativeBase):
    pass


class SQLLabJob(Base):
    __tablename__ = "superset_sql_lab_job"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    operation_id: Mapped[str] = mapped_column(String(160), unique=True)
    arguments: Mapped[dict] = mapped_column(JSON)


@dataclass(frozen=True)
class PublishedJob:
    id: str

    def forget(self):
        """SQL Lab does not use the scheduler's task-result backend."""


class SQLLabDispatch:
    """The native asynchronous executor calls delay and then forget unchanged."""

    def __init__(self, sessions):
        self.sessions = sessions

    def delay(self, query_id, rendered_query, **kwargs):
        from superset import db
        from superset.models.sql_lab import Query

        query = db.session.get(Query, query_id)
        if query is None:
            raise RuntimeError("SQL Lab query disappeared before publication")
        operation_id = query.client_id
        arguments = {"query_id": query_id, "rendered_query": rendered_query, **kwargs}
        with self.sessions.begin() as session:
            session.add(SQLLabJob(id=query_id, operation_id=operation_id, arguments=arguments))
        node = f"sql_lab:{query_id}"
        record("sql_lab", operation_id, node, "submitted", backend="dbworker")
        return PublishedJob(node)


def handle_sql_lab(job: SQLLabJob, session: Session) -> Finished:
    from examples.superset_dbworker.adapter import initialize
    from examples.dbworker_integration.runtime import forbid_task_dispatch
    app = initialize()
    from superset import db
    from superset.models.sql_lab import Query
    from superset.sql_lab import get_sql_results

    identity, operation_id, arguments = job.id, job.operation_id, dict(job.arguments)
    node = f"sql_lab:{identity}"
    session.rollback()
    record("sql_lab", operation_id, node, "started", backend="dbworker")
    try:
        with job_context(operation_id, node), forbid_task_dispatch(), app.app_context():
            # DBWorker replaces scheduling only. This is the original async task
            # body with its original return_results/store_results/user arguments.
            get_sql_results.run(**arguments)
            db.session.remove()
            query = db.session.get(Query, identity)
            if query is None or query.status != "success" or not query.results_key:
                raise RuntimeError(f"Native SQL Lab operation failed: {identity}")
        record("sql_lab", operation_id, node, "succeeded", backend="dbworker")
        return Finished()
    except Exception as error:
        record("sql_lab", operation_id, node, "failed", backend="dbworker", error=str(error))
        raise


def coordinator(database_url, concurrency=2):
    engine = create_engine(database_url)
    sessions = sessionmaker(engine, expire_on_commit=False)
    runtime = Coordinator(sessions, database_url=database_url, poll_seconds=.05,
                          max_poll_seconds=.25, lease_seconds=300)
    runtime.transactional_worker(name="sql_lab", source=SQLLabJob,
                                 eligible=lambda: select(SQLLabJob), concurrency=concurrency)(handle_sql_lab)
    Base.metadata.create_all(engine)
    return runtime, sessions


@contextmanager
def use_dbworker_executor(sessions):
    """Change only executor selection for the DBWorker application variation."""
    from superset.sqllab.api import SqlLabRestApi
    from superset.sqllab.sql_json_executer import ASynchronousSqlJsonExecutor

    original = SqlLabRestApi._create_sql_json_executor

    def select_executor(execution_context, query_dao):
        if execution_context.is_run_asynchronous():
            return ASynchronousSqlJsonExecutor(query_dao, SQLLabDispatch(sessions))
        return original(execution_context, query_dao)

    SqlLabRestApi._create_sql_json_executor = staticmethod(select_executor)
    try:
        yield
    finally:
        SqlLabRestApi._create_sql_json_executor = staticmethod(original)
