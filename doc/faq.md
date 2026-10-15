# DBWorker FAQ: transactions, retries, and database limits

DBWorker runs importable Python handlers for existing SQLAlchemy model rows. It
tracks each worker's execution separately from the application's business data.
Start with the [runnable SQLite quickstart](quickstart.md), or use the complete
[usage reference](usage.md) when integrating with an existing application.

## Do I need Redis, a broker, or an enqueue API?

No separate broker is required. Register a handler for a SQLAlchemy source model
and let `eligible` select rows that should be processed. The coordinator discovers
those rows through polling; creating or enabling a matching row can provide work
without an enqueue call. DBWorker creates a worker-specific execution table so
business results and execution bookkeeping remain distinct.

An eligibility query can include application conditions and ordering. DBWorker
does not supply a general cron API: time-based eligibility and recurring business
records need an application-defined data model and query.

## Which databases and models can I use?

The package is intended for SQLite, PostgreSQL, and MySQL through SQLAlchemy 2.x.
It requires Python 3.12 or later. Install the appropriate database driver for
PostgreSQL or MySQL. A worker's source model must have exactly one primary-key
column; it need not be an integer key. The worker interface uses a synchronous
SQLAlchemy `Session`, rather than `AsyncSession`. Worker processes require file-backed SQLite,
so an in-memory SQLite URL is rejected at startup.

## What commits together, and who owns the session?

The supplied SQLAlchemy session belongs to DBWorker. On a valid `Finished()` or
`Unfinished()` return, the runtime commits writes made through that session and
its execution-status update in one transaction. A handler must not commit or close
this session itself. A handler exception prevents those writes from committing.

Writes through another engine/session, independently committed work, file writes,
network requests, and external service effects are outside that transaction.
For a long computation, a handler may release its read transaction with
`session.rollback()` before making writes it needs to keep; first copy the values
needed for computation. Rollback is not a way to preserve uncommitted progress.

## Does this guarantee exactly-once email or payment delivery?

No. A database transaction cannot atomically commit an arbitrary external service
operation. An external effect can occur before the handler's database commit or
before a crash. Recovery or a manual retry may execute the handler again.
Use an application-appropriate idempotency key or another recovery design for
such effects, and consider ambiguous external acknowledgments explicitly.

## What happens when a handler raises an exception?

DBWorker rolls back the handler transaction and records `FAILED` with an error.
Failed work is not automatically claimable. Inspect it through
`coordinator.workers[name].state(session, source_id)`. After correcting the cause,
call `reset_failed()` inside a committed transaction to mark it `UNFINISHED` and
clear the previous error and claim. The running coordinator may then claim it
again if it matches `eligible`.

Resetting does not undo already committed application progress or external effects.
See the [failure and retry examples](usage.md#understand-failures-and-retry-work).

## What does Unfinished save?

`Unfinished()` commits the current invocation's database writes and records that
more work remains. The next invocation starts the Python function from its
beginning. DBWorker does not save the function's instruction pointer or local
variables. Store a cursor, processed-page ledger, or another continuation marker
in application data when implementing work in steps.

## Can an expired claim still report WORKING?

Yes. Lease expiration alone does not update the recorded enum status. A recorded
`WORKING` claim whose lease has expired can become reclaimable; active claims are
renewed by the coordinator. If the coordinator dies or stops renewing ownership,
other coordinators can reclaim expired work.

An observed handler failure with a live coordinator can instead be recorded as
`FAILED`. Do not assume every process failure follows only the lease-expiry path.
`execution_status()` reports recorded execution state, not a live process-health
probe.

## How are stale results fenced off?

Before committing, DBWorker updates the work row only if its source ID,
`WORKING` status, and claim token still match that execution. If ownership has
been replaced, the conditional update cannot succeed, `LostClaim` rejects the
result, and writes in the supplied transaction are rolled back. This protects
that database transaction; it cannot roll back an external effect already sent.

## Why can two independently claimed SQLite tasks still contend?

Claims identify ownership of separate work items. SQLite still permits only one
writer at a time, and the coordinator and API can also write to the same database.
Increasing handler concurrency does not remove that database-wide constraint.
Keep transactions short and measure the actual application before increasing
concurrency. WAL can reduce reader/writer contention, but does not add concurrent
writers or make stale read snapshots writable.

## What SQLite finalization retry was added in 0.1.2?

If the final execution-status `UPDATE` encounters recoverable SQLite `BUSY`,
`BUSY_RECOVERY`, or `BUSY_TIMEOUT`, DBWorker can retry that same conditional update
in the intact transaction. It retains pending business writes and the original
claim-token condition, and does not rerun the handler. Backoff starts at 10 ms and
is capped at 100 ms; the additional retry window is five seconds after the first
error. Each database attempt can also wait for the connection's busy timeout.

This does not retry handler queries, ORM flushes, commits, or coordinator queries.
It does not retry an invalidated connection, a transaction rolled back by the
driver, or `BUSY_SNAPSHOT`. A stale WAL snapshot needs a new transaction; repeating
the status update cannot repair it. Exhausted retries follow the ordinary failure
path. See [SQLite configuration and limitations](usage.md#sqlite-lock-contention).

## How do I make one worker wait for another?

Use `has_execution_status()` in the dependent worker's `eligible` query. It builds
a SQL condition such as “this input has finished under worker A”; it does not run
a database query or return a Python boolean when constructed. Register the named
worker before constructing that condition. Inputs with no execution record do
not match a requested recorded status.

See [progress and dependent work](usage.md#track-progress-and-coordinate-dependent-work).

## Does changing a finished row make its handler run again?

Not by itself. Execution tracking is per worker name and source primary key;
recorded `FINISHED` work is excluded from normal claiming. `reset_failed()` only
resets failed executions. For repeatable business operations, model new inputs or
an explicit application workflow rather than assuming that editing a completed
row clears its execution history.

## Where should handlers be registered, and how should services shut down?

Register importable handlers and models before startup. Spawned child processes
need to import their definitions; avoid relying on notebook-local functions or
non-serializable bound arguments. Decorated functions remain ordinary callables:
a direct call does not obtain runtime-managed claims, transactions, or processes.

`start()` initializes the pools and starts background scheduling. The service must
stay alive while processing is required. `stop()` stops new claims and waits for
active handlers; lease renewal continues while draining. It does not forcibly
terminate an indefinitely blocked handler.

## Is DBWorker a drop-in Celery replacement or always faster?

Neither conclusion follows from this project. DBWorker's integration point is a
SQLAlchemy source model and an eligibility query. Existing Celery publication,
routing, retry policy, and application integrations require their own migration
assessment. The application adapters in this repository cover declared workflows.

The [benchmark report](benchmark-results.md) retains per-workload results,
submission-window timing, repetitions, latency, and backlog where available.
One offered load is not a maximum-capacity search, and one favorable workload does
not establish performance for another. Use [Choosing DBWorker](choosing-dbworker.md)
to assess the fit before testing your own workload.

## Source of these guarantees

The transaction, claim predicates, status helpers, pool startup, and stop behavior
are implemented in [src/dbworker.py](../src/dbworker.py). The documentation above
is scoped to DBWorker 0.1.2 and should be reviewed when those implementations change.
