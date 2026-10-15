# DBWorker quickstart: SQLAlchemy background jobs with SQLite

DBWorker processes existing SQLAlchemy model rows in background child processes,
using the database to coordinate work. This tutorial creates two rows, doubles
their values, and waits for the recorded completion status. It uses a local
SQLite file and needs no broker setup or enqueue call.

| Requirement | This tutorial |
| --- | --- |
| Package | `dbworker==0.1.2` |
| Python | `>=3.12,<4.0` |
| SQLAlchemy | `>=2.0,<3.0`; installed with DBWorker |
| Database | A local SQLite file, created next to the script |
| Execution | Save the complete example as a Python file and run it from a terminal |
| Result | `[(3, 6), (5, 10)]`, retained in that SQLite file |

The version requirements come from the [package metadata](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/pyproject.toml#L16-L18).

For API details, see [Usage instructions](usage.md). For transaction and recovery
questions, see the [FAQ](faq.md).

## Install

In a Python environment of your choice:

```sh
python -m pip install dbworker==0.1.2
```

SQLite uses Python's standard-library driver. PostgreSQL and MySQL deployments
also need the DBAPI driver appropriate for their SQLAlchemy database URL.

## Save and run the example

Save this code as `quickstart_dbworker.py`. Run it as a file, rather than pasting
it into a notebook or an interactive Python prompt: spawned processes must be able
to import the model and handler definitions.

```python
import time
from pathlib import Path

from sqlalchemy import Integer, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from dbworker import Coordinator, ExecutionStatus, Finished


class Base(DeclarativeBase):
    pass


class Number(Base):
    __tablename__ = "quickstart_number"

    id: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[int] = mapped_column(Integer)
    doubled: Mapped[int | None] = mapped_column(Integer, nullable=True)


# An absolute, file-backed URL points every spawned process at the same database.
database_path = Path(__file__).resolve().with_suffix(".sqlite3")
database_url = f"sqlite:///{database_path.as_posix()}"
engine = create_engine(database_url)
sessions = sessionmaker(engine, expire_on_commit=False)
coordinator = Coordinator(sessions, database_url=database_url)


@coordinator.transactional_worker(
    name="double_value",
    source=Number,
    eligible=lambda: select(Number).where(Number.doubled.is_(None)).order_by(Number.id),
    concurrency=2,
)
def double_value(number: Number, session: Session) -> Finished:
    number.doubled = number.value * 2
    return Finished()


def main() -> None:
    # Table creation here is a demo convenience. Use your application's schema
    # migration process when integrating with an existing production database.
    Base.metadata.create_all(engine)
    coordinator.create_worker_tables()
    with sessions.begin() as session:
        for identity, value in ((1, 3), (2, 5)):
            if session.get(Number, identity) is None:
                session.add(Number(id=identity, value=value))

    try:
        coordinator.start()
        deadline = time.monotonic() + 30
        while True:
            with sessions() as session:
                states = [
                    coordinator.execution_status(session, worker="double_value", source_id=i)
                    for i in (1, 2)
                ]
                if ExecutionStatus.FAILED in states:
                    raise RuntimeError("A task failed; inspect coordinator.workers['double_value'].state(session, source_id)")
                if all(state == ExecutionStatus.FINISHED for state in states):
                    numbers = session.scalars(select(Number).order_by(Number.id)).all()
                    print([(number.value, number.doubled) for number in numbers])
                    break
            if time.monotonic() >= deadline:
                raise TimeoutError("Tasks did not finish within 30 seconds")
            time.sleep(0.05)
    finally:
        coordinator.stop()
        engine.dispose()


if __name__ == "__main__":
    main()
```

Run:

```sh
python quickstart_dbworker.py
```

Expected output:

```text
[(3, 6), (5, 10)]
```

The example retains its database next to the script. Running the file again prints
the existing completed results. It does not enqueue a second copy of each job.

## What happens

1. The application inserts business rows. No enqueue call is made.
2. `eligible` selects source rows whose `doubled` value is missing. DBWorker also
   filters out work already recorded as finished, failed, or actively claimed.
3. The coordinator commits ownership and dispatches the claimed IDs to its
   spawned process pool.
4. The handler changes `Number.doubled` using the supplied session.
5. DBWorker conditionally updates the execution status using the claim token and
   commits that status together with the handler's database writes.

The `session` argument is available for related SQLAlchemy queries and writes;
this handler only needs the mapped `number`. Do not call `session.commit()` or
`session.close()` inside the handler. DBWorker owns those operations.

## Use your own SQLAlchemy model

Replace `Number` with your model and replace `number.doubled = number.value * 2`
with the database work to perform. Keep these responsibilities explicit:

| Part of the example | What your application supplies |
| --- | --- |
| `Number` | An importable mapped model with one primary-key column |
| `sessions` and `database_url` | A synchronous session factory and URL pointing at the same database |
| `eligible` | A `select(YourModel)` query expressing business readiness; omit it to consider all source rows |
| `double_value` | An importable handler returning `Finished()` or `Unfinished()` |
| `create_all()` and the two inserts | Your existing schema migrations and business input creation |
| The polling loop | Your API or service's progress reporting and lifecycle management |

Registration and row selection are defined by the [worker interface](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L73-L115)
and the [candidate query](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L344-L365).

## Inspect a failure and allow another attempt

If a handler fails, DBWorker records `FAILED` and does not automatically run it
again. In the example's application process, inspect source ID `1` with:

```python
with sessions() as session:
    state = coordinator.workers["double_value"].state(session, 1)
    if state is not None:
        print(state["execution_status"], state["error"])
```

After correcting the cause, allow that failed execution to be considered again:

```python
with sessions.begin() as session:
    reset = coordinator.workers["double_value"].reset_failed(session, 1)
```

These snippets use the definitions in the complete example. The committed reset
marks failed work `UNFINISHED`; a running coordinator still needs to find the row
eligible. Changing `eligible` alone cannot make a recorded `FAILED` row claimable.
See [failure recovery](faq.md#what-happens-when-a-handler-raises-an-exception)
and the [reset implementation](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L117-L127).

## Reuse the example database

On a second run, the script keeps the same source IDs and prints the completed
results again. To add more work, insert a new source ID and extend the polling
loop if you want it to wait for that input too. Editing a finished row does not
clear its execution history. See [finished-row behavior](faq.md#does-changing-a-finished-row-make-its-handler-run-again).

## Run an API and worker service

Use your application's SQLAlchemy model, session factory, database URL, and schema
migration process. A source model must have one primary-key column. Register
importable handlers before calling `start()`, and keep service startup behind the
main guard or in the application's explicit service entry point.

Your API and worker service may run independently against the same database.
The [image-deduplication example](../examples/imagededup_system_dbwork/README.md)
shows that separation, as well as dependencies and paged processing.

For SQLite, begin with modest concurrency and short write transactions. More
processes do not create additional simultaneous SQLite writers. See
[SQLite lock contention](usage.md#sqlite-lock-contention) before changing timeouts
or journal mode. For emails or other external effects, design for safe repetition;
the supplied database transaction does not make an external service transactional.

## Implementation references

The source links above are pinned to the implementation reviewed for this guide.
See [registration and status helpers](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L287-L337),
[process startup and shutdown](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L545-L592), and
[the handler transaction](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L207-L239) for the corresponding behavior.
