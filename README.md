# dbworker

Database-backed work coordination using SQLAlchemy and child processes, without a separate broker. Applications register ordinary handler functions; the coordinator claims work, renews leases and dispatches handlers. A handler's database writes and completion outcome commit together.

## Layout

```text
pyproject.toml                 Framework Poetry project
src/
    dbworker.py                Coordinator, worker execution and outcomes
    examples/                  Independent applications, not a Python package
        artifact_comparison/
            pyproject.toml     Example dependencies and API command
            src/durable_worker_example/
            tests/             Application and worker integration tests
tests/                         Framework-only tests
```

The framework module lives directly under `src` and is imported as `dbworker`. Its only runtime dependency is SQLAlchemy. Example applications are excluded from the framework distribution. Each example owns its Poetry project and references the repository root as an editable path dependency.

## Install and test the framework

```sh
poetry install
poetry run python -m unittest discover -s tests -v
poetry run mypy --strict src/dbworker.py
```

## Run the FastAPI example

```sh
cd src/examples/artifact_comparison
poetry install
poetry run durable-worker-example-api
```

The example serves `http://127.0.0.1:8001`. Its [README](src/examples/artifact_comparison/README.md) explains the API, application progress tables, claiming and execution behavior.

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
        .where(FeatureArtifact.feature_json.is_(None))
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

The FastAPI example declares decorated, module-level handlers in `main.py`. Those handlers call the plain application functions in `domain/workflows.py`. The coordinator and decorators are constructed on import; database tables and processing start in FastAPI's lifespan. Importing handlers in child processes opens no coordinator connection.

Handlers must remain importable for spawned child processes. `functools.partial` can bind serializable handler configuration when applying the decorator to an existing function.

Worker state is accessible through `coordinator.workers["artifact_build"].state(session, source_id)`; failed work can be reset through that worker's `reset_failed(session, source_id)` method. The decorator's transaction contract applies only during worker execution; direct function calls remain ordinary calls.

The handler receives a real SQLAlchemy `Session`, owned and closed by the runtime. Return `Finished()` or `Unfinished()`; do not commit or close this session. Early commits are rejected. The runtime verifies claim ownership and commits the final application writes and task outcome together; errors or a replaced claim roll back the transaction.

After read-only work, copy the values needed for computation and call `session.rollback()` to release the connection. Compute using those copied values; the next SQL operation starts the final transaction. Rollback expires ORM objects and discards any pending writes, so do not use it after work you intend to persist. Accessing expired ORM attributes during computation would perform another database read.

This is not one transaction spanning all reads and computation: only the final transaction is committed with completion. External effects and writes through independent connections are outside that guarantee. See the example for a complete CPU handler and application-owned progress tracking.
