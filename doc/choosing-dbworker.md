# Choosing DBWorker for a SQLAlchemy application

DBWorker is a Python background worker whose inputs are existing SQLAlchemy model
rows. The coordinator discovers eligible rows, claims ownership, and runs your
handler in a spawned process. The supplied database session owns both business
writes and the recorded execution outcome.

This integration can fit an application that already treats database state as the
source of pending work. Start with the [SQLite quickstart](quickstart.md), and read
the [transaction and recovery FAQ](faq.md) before using external services.

## A concrete fit: an existing row needs processing

Suppose an application stores imported assets. Each asset needs a derived value,
and a later comparison must wait until that value is ready. The application can
already express those conditions as SQL queries.

With DBWorker, the first worker uses the asset model as its source and selects
rows needing preparation. A second worker can use `has_execution_status()` in its
eligibility query to wait for recorded completion. Partial comparisons can commit
an application-defined page cursor or ledger and return `Unfinished()`.

The [image-deduplication example](../examples/imagededup_system_dbwork/README.md)
implements that pattern with independently runnable API and worker services.
The application still owns its business models, progress, and results. DBWorker
adds a separate execution table for each registered worker.

## Questions to answer before adopting it

| Your application's requirement | What to check with DBWorker |
| --- | --- |
| SQLAlchemy models already represent the input | Each source needs one primary-key column. `eligible` can express business readiness and ordering. |
| Business results and task completion must agree | Make the related database writes through the supplied session so they share the execution-status transaction. |
| A long operation should run in pages | Persist progress in application data and return `Unfinished()`. Each invocation starts at the beginning. |
| The API and worker are separate services | Point them at the same database and start the coordinator explicitly in the worker service. |
| An email, file, or remote API call is involved | Design idempotency and ambiguous-outcome recovery separately; the database transaction cannot include arbitrary external effects. |
| Failed work should be retried | Define your failure policy. The core records `FAILED`; application code explicitly resets failed work when appropriate. |
| Existing Celery integrations must be preserved | Audit publication entry points, routing assumptions, lifecycle hooks, and retry behavior. Do not infer compatibility from a scoped adapter. |
| The workload needs recurring or delayed operations | Model the time/readiness condition in your application query and provide recurring inputs or a separate scheduler as needed. |

See the [usage reference](usage.md) for registration, eligibility, execution-state
queries, dependencies, reset operations, and coordinator configuration.

## Choose the database and concurrency together

SQLite can keep a small deployment simple because the worker processes use the
same file-backed database. It remains a single-writer database. Separate claims,
additional processes, WAL mode, and larger timeouts do not create additional
simultaneous writers. API writes, claiming, lease renewal, and finalization all
need to be considered in the actual workload.

Begin with modest concurrency and short transactions. A long computation can
release a read transaction before making writes, when that is correct for the
application's consistency requirements. For sustained concurrent writing, assess
PostgreSQL or MySQL with the real workload and the appropriate database driver.
DBWorker uses supported row-locking capabilities on applicable server versions;
that does not remove other database bottlenecks.

The 0.1.2 SQLite finalization retry helps with a specific recoverable busy update.
It is not a blanket retry of arbitrary failed work or an invalidated snapshot.
Read the [precise lock-retry scope](usage.md#sqlite-lock-contention) before relying
on it as a recovery mechanism.

## Assess process and service behavior

Handlers run in spawned child processes with their own engines and sessions.
Register importable definitions before startup and pass serializable process
configuration. Keep a worker service alive for as long as processing is required.
`stop()` drains active work rather than forcibly terminating a stuck handler.

Use the supplied session for work whose writes must participate in the owned
transaction. Direct calls to the decorated function remain ordinary Python calls;
they do not acquire runtime-managed ownership. An API should read recorded state
through the coordinator helpers when it needs progress or error information.

## Evaluate performance on your workload

The repository's [application benchmark report](benchmark-results.md) compares
declared workflows against their original Celery paths and retains correctness
and execution evidence. These are scoped application comparisons. They do not
establish support for every task graph, failure mode, or application deployment.

Read these measurements together:

- Wall time includes the fixed scheduled submission window, so a near-equal wall
  time can coexist with different queue latency or backlog.
- Task P95 and peak published outstanding work, where recorded, expose behavior
  that a single wall-time number can hide.
- Three repetitions describe variation at one offered load. They do not locate
  the maximum sustainable capacity of either backend.
- Image hash building uses a declared native bulk-import workload. Its job
  granularity should not be generalized to every other scenario.

For an adoption trial, verify the same business outputs on both implementations,
include startup when it matters to your service, and record latency, backlog,
failures, database contention, and external effects. Select the implementation
that satisfies those requirements; do not generalize one favorable benchmark
ratio to another application.

## Implementation and migration scope

The public registration, eligibility, transaction, and ownership behavior is in
[src/dbworker.py](../src/dbworker.py). The
[benchmark method](../benchmarks/README.md) explains the scope of the application
adapters. This page describes DBWorker 0.1.2 and the repository evidence available
with that implementation.
