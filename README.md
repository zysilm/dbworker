# dbworker

Database-backed work coordination using SQLAlchemy and child processes, without a separate broker. Applications register ordinary handler functions; the coordinator claims work, renews leases and dispatches handlers. A handler's database writes and completion outcome commit together.

## Layout

```text
pyproject.toml                 Framework Poetry project
src/
    dbworker.py                Coordinator, worker execution and outcomes
examples/                      Independent applications, not a Python package
    imagededup_system_dbwork/
        pyproject.toml         Example dependencies and API command
        src/imagededup_system_dbwork/
        tests/                 Application and worker integration tests
    imagededup_system_redis_celery/
        pyproject.toml         Independent Redis/Celery image example
        src/imagededup_system_redis_celery/
        tests/
benchmarks/
    imagededup_benckmark/       Dataset download, sequential API benchmarks, JSON results
tests/                         Framework-only tests
```

The framework module lives directly under `src` and is imported as `dbworker`. Its only runtime dependency is SQLAlchemy. Example applications are excluded from the framework distribution. Each example owns its Poetry project and references the repository root as an editable path dependency.

## Install and test the framework

```sh
poetry install
poetry run python -m unittest discover -s tests -v
poetry run mypy --strict src/dbworker.py
```

## Run the image deduplication example

```sh
cd examples/imagededup_system_dbwork
poetry env use python3.12
poetry install
poetry run imagededup-system-dbwork-api
```

The example serves `http://127.0.0.1:8001`. Its [README](examples/imagededup_system_dbwork/README.md) explains the API, application progress tables, claiming and execution behavior.

## Redis and Celery comparison example

The independent [Redis/Celery image example](examples/imagededup_system_redis_celery/README.md) provides the same image API on port 8002. It uses Celery prefork workers and Redis, with SQL results and a transactional publication outbox. It has its own Poetry project, takes existing image files, and does not depend on DBWorker or include dataset downloading.

## Image benchmarks

The [benchmark project](benchmarks/imagededup_benckmark/README.md) owns MIRFLICKR downloading and preparation. It runs the image APIs sequentially with four build workers and four comparison workers each, covering build-only, comparison-only and mixed workloads, and writes structured JSON results.

## Handler interface

```python
from dbworker import Coordinator, Finished
from sqlalchemy import select
from sqlalchemy.orm import Session

coordinator = Coordinator(session_factory, database_url=database_url)


@coordinator.transactional_worker(
    name="artifact_build",
    source=FeatureArtifact,
    eligible=lambda: select(FeatureArtifact)
        .where(FeatureArtifact.hash_value.is_(None))
        .order_by(FeatureArtifact.id),
    concurrency=4,
)
def build_artifact(artifact: FeatureArtifact, session: Session) -> Finished:
    # Use ordinary SQLAlchemy operations. The runtime commits the final
    # transaction and task outcome together after this function returns.
    ...
    return Finished()
```

The decorator creates and registers the worker internally; no separate `Worker(...)` or registration call is needed. It preserves the ordinary function and its signature. Registration starts no processing: create application and worker tables, then call `coordinator.start()` and eventually `coordinator.stop()`. Register all handlers before starting.

The FastAPI example declares decorated, module-level handlers in `main.py`. Those handlers call the plain application functions in `domain/artifact_build.py` and `domain/comparison.py`. The coordinator and decorators are constructed on import; database tables and processing start in FastAPI's lifespan. Importing handlers in child processes opens no coordinator connection.

Handlers must remain importable for spawned child processes. `functools.partial` can bind serializable handler configuration when applying the decorator to an existing function.

Execution status is accessible through `coordinator.execution_status(session, worker="artifact_build", source_id=artifact_id)`. Failed work can be reset through `coordinator.workers["artifact_build"].reset_failed(session, artifact_id)`. The decorator's transaction contract applies only during worker execution; direct function calls remain ordinary calls.

The handler receives a real SQLAlchemy `Session`, owned and closed by the runtime. Return `Finished()` or `Unfinished()`; do not commit or close this session. Early commits are rejected. The runtime verifies claim ownership and commits the final application writes and task outcome together; errors or a replaced claim roll back the transaction.

After read-only work, copy the values needed for computation and call `session.rollback()` to release the connection. Compute using those copied values; the next SQL operation starts the final transaction. Rollback expires ORM objects and discards any pending writes, so do not use it after work you intend to persist. Accessing expired ORM attributes during computation would perform another database read.

This is not one transaction spanning all reads and computation: only the final transaction is committed with completion. External effects and writes through independent connections are outside that guarantee. See the example for a complete CPU handler and application-owned progress tracking.

## Querying execution status and dependencies

```python
from dbworker import ExecutionStatus

execution_status = coordinator.execution_status(
    session, worker="artifact_build", source_id=artifact_id,
)
if execution_status is ExecutionStatus.FINISHED:
    ...

build_finished_or_failed = coordinator.has_execution_status(
    worker="artifact_build", source_id=FeatureArtifact.id,
    statuses=(ExecutionStatus.FINISHED, ExecutionStatus.FAILED),
)
```

`ExecutionStatus` is a `StrEnum` with `WORKING`, `UNFINISHED`, `FINISHED`, and `FAILED`. SQLAlchemy stores their lowercase values and reads enum members. A source with no execution record returns `None`; an unknown worker name raises `KeyError`. An expired lease remains `WORKING` until a subsequent transition.

`has_execution_status()` constructs a SQLAlchemy `EXISTS` expression without querying the database. Its source ID may be a literal, mapped attribute, or aliased SQL column. Use it in eligibility queries or application functions. The example passes its coordinator to the comparison functions, which construct their build-status predicate internally. Unclaimed sources match none of the statuses, so negating a terminal-status predicate includes them. An empty status collection matches nothing. Register the referenced worker before constructing its predicate.

Generated work-table names are an implementation detail. The example uses this public method for parent-process eligibility and child-process completion decisions, while retaining its application-owned `ScoredCandidate` ledger.
