# Run a SQLAlchemy background worker with SQLite

This example uses DBWorker 0.1.2 and Python 3.12 or later. It creates two ordinary
SQLAlchemy rows, processes them in child processes, and waits for their recorded
execution status. The database is the only service required.

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

## Integrate with your application

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

This example follows the unchanged [Coordinator API](../src/dbworker.py), including
`transactional_worker`, `execution_status`, `start`, and `stop`, and the
[execution transaction](../src/dbworker.py) that owns the handler commit.
