# dbworker

Database-backed work coordination using SQLAlchemy and child processes, without a separate broker. Applications register ordinary handler functions; the coordinator claims work, renews leases and dispatches handlers. A handler's database writes and completion outcome commit together.

## Layout

```text
pyproject.toml                 Framework Poetry project
src/
    dbworker.py                Coordinator, Worker, outcomes and execution
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
from dbworker import Finished, transactional
from sqlalchemy.orm import Session


@transactional
def handler(source: object, session: Session) -> Finished:
    # Use ordinary SQLAlchemy operations. The runtime commits the final
    # transaction and task outcome together after this function returns.
    return Finished()
```

Pass the decorated function to `Worker(handler=handler, ...)`. The decorator preserves the ordinary function and its signature; its managed transaction contract applies when the worker executes it. Configured handlers can use `functools.partial`.

The handler receives a real SQLAlchemy `Session`, owned and closed by the runtime. Return `Finished()` or `Unfinished()`; do not commit or close this session. Early commits are rejected. The runtime verifies claim ownership and commits the final application writes and task outcome together; errors or a replaced claim roll back the transaction.

After read-only work, copy the values needed for computation and call `session.rollback()` to release the connection. Compute using those copied values; the next SQL operation starts the final transaction. Rollback expires ORM objects and discards any pending writes, so do not use it after work you intend to persist. Accessing expired ORM attributes during computation would perform another database read.

This is not one transaction spanning all reads and computation: only the final transaction is committed with completion. External effects and writes through independent connections are outside that guarantee. See the example for a complete CPU handler and application-owned progress tracking.
