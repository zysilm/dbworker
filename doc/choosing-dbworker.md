# Choosing DBWorker for a SQLAlchemy application

DBWorker is a Python background worker whose inputs are existing SQLAlchemy model
rows. The coordinator discovers eligible rows, claims ownership, and runs your
handler in a spawned process. The supplied database session owns both business
writes and the recorded execution outcome.

DBWorker 0.1.2 fits applications where pending work can be expressed as a query
over existing business rows. It uses the database for coordination and requires
no separate message broker. Your application still owns its models, schema
migrations, service lifecycle, failure policy, and external-effect recovery.

It requires Python `>=3.12,<4.0`, SQLAlchemy `>=2.0,<3.0`, and synchronous handler
sessions. Start with the [SQLite quickstart](quickstart.md), or check the
[transaction and recovery FAQ](faq.md) before integrating external services.

References: [package requirements](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/pyproject.toml#L16-L18) and
[the supplied handler session and transaction](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L207-L239).

## When DBWorker fits

| Situation | Fit and responsibility |
| --- | --- |
| A database row needs a derived result, such as an asset hash | Select ready rows with `eligible` and write the result through the supplied session. |
| A later step waits for an earlier step's recorded completion | Express the prerequisite with `has_execution_status()` in a query. |
| A large comparison or import can commit progress in pages | Store a cursor or ledger in application data and return `Unfinished()`. |
| An API and a worker should be separate services sharing data | Run the coordinator in the worker service; let the API use the same database and status helpers. |
| Processing is triggered by database state, and a separate broker would add operational work | Use row discovery and claiming; account for the polling and coordination load on the database. |

## Requirements that need a different design

DBWorker's core does not expose a general cron scheduler, broker-routing API, or
`AsyncSession` handler interface. If one of those is essential, assess an
additional component or another worker design before adopting it. The
[Coordinator API](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L242-L337) defines its registration and configuration surface.

For emails, remote API calls, and file writes, define how to handle repetition and
ambiguous outcomes. Those effects cannot be included in the supplied-session
transaction. A requirement for atomic completion across an arbitrary external
service needs its own recovery design; see [the external-effects boundary](faq.md#does-this-guarantee-exactly-once-email-or-payment-delivery).

SQLite also remains a single-writer database. A deployment needing sustained
concurrent writes should assess PostgreSQL or MySQL with its real workload;
adding worker processes is not a substitute for database write capacity.

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

## Migrating from a Celery-style enqueue workflow

DBWorker changes the work-discovery and transaction boundary. To evaluate a
migration, map the existing workflow explicitly. The [usage reference](usage.md)
contains the corresponding registration, status, and reset APIs:

| Decision | DBWorker 0.1.2 behavior | What to establish in the application |
| --- | --- | --- |
| How does work appear? | The coordinator polls source rows matching `eligible`, with a separate execution-availability filter. | Find each existing publication path and identify the durable row or readiness condition that would replace it. |
| What identifies an execution? | Worker name plus the source row's single primary key. | Model new business inputs when the same operation must run again; changing a finished row does not clear its history. |
| What commits together? | Writes through the handler's supplied session and its execution outcome. | Find other sessions, existing commits, and external effects that fall outside that boundary. |
| What permits a retry? | `Unfinished()` allows later eligible work; `FAILED` needs explicit `reset_failed()`. | Preserve the intended failure policy and decide who authorizes resets. |
| How are prerequisites expressed? | SQL predicates over application data and recorded execution status. | Check that the existing dependencies can be represented by those queries and data. |
| What runs in production? | An application-managed coordinator with spawned handler processes and database access. | Plan startup, shutdown, process resources, database drivers, and contention alongside the API. |

The repository's application adapters demonstrate declared workflows, not
compatibility with every existing Celery integration. Preserve the business
outputs and recovery requirements when evaluating a migration. Relevant details
are in the [claim candidate query](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L344-L365),
[handler transaction](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L207-L239),
[reset operation](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L120-L127), and
[benchmark scope](../benchmarks/README.md#native-comparison-contract-and-validation-status).

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

The current full profile uses eight producers, eight total execution slots, a
60-second scheduled submission window, and three repetitions per backend. Image
hash building is a declared bulk-import exception. Each workload's report gives
its database, environment, timing boundary, and correctness evidence; those
conditions matter when applying a result to your application. See the
[fixed-load method](../benchmarks/README.md#fixed-concurrent-load) and the
[per-workload report](benchmark-results.md).

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
the [reviewed implementation](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py). The
[benchmark method](../benchmarks/README.md) explains the scope of the application
adapters. This page describes DBWorker 0.1.2 and the repository evidence available
with that implementation.
