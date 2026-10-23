# DBWorker usage: SQLAlchemy workers, transactions, and recovery

Use DBWorker with your existing SQLAlchemy models, session factory and database.
Requires Python `>=3.12,<4.0` and SQLAlchemy `>=2.0,<3.0`. Install with
`poetry add dbworker`. This reference describes DBWorker 0.1.2.

For a complete runnable example, start with the [SQLite quickstart](quickstart.md).
See the [FAQ](faq.md) for transaction, retry, and SQLite limits, or
[Choosing DBWorker](choosing-dbworker.md) to assess the fit for your application.

## Find the task you need

| I want to… | Read |
| --- | --- |
| Run a complete SQLite example | [Install and run the quickstart](quickstart.md#install) |
| Decide whether DBWorker fits my application | [Fit and migration decisions](choosing-dbworker.md#when-dbworker-fits) |
| Register a handler for existing rows | [Quick start](#quick-start) |
| Commit business writes with the task outcome | [Transaction ownership](faq.md#what-commits-together-and-who-owns-the-session) |
| Process work in pages | [What Unfinished saves](faq.md#what-does-unfinished-save) |
| Query progress or wait for another worker | [Progress and dependencies](#track-progress-and-coordinate-dependent-work) |
| Inspect and retry failed work | [Failure recovery](#understand-failures-and-retry-work) |
| Understand lease expiry and stale results | [Ownership and fencing](faq.md#how-are-stale-results-fenced-off) |
| Handle SQLite writer contention | [SQLite configuration](#sqlite-lock-contention) |
| Interpret the published Celery comparisons | [Benchmark scope](choosing-dbworker.md#evaluate-performance-on-your-workload) |

## Quick start

Use your existing SQLAlchemy `session_factory` and database URL:

```python
from dbworker import Coordinator

coordinator = Coordinator(session_factory, database_url=database_url)
```

You write a **handler**: a Python function containing the work you want to run. DBWorker runs handlers in separate processes.

Before calling your handler, DBWorker chooses the next task and marks it as “being worked on” in the database. This step is called a **claim**. It lets several processes share the tasks without choosing the same task at the same time.

DBWorker renews the claim while your handler runs. If the coordinator stops renewing it, the claim can be reclaimed after its lease expires. Handler exceptions are recorded as failures, as described below.

`source` is the database table that supplies input to your handler, specified as a SQLAlchemy model. It can store user requests, or any entries whose creation should automatically start a procedure. Each new entry gives DBWorker new work to run, and the selected entry is passed to your handler. Its primary key identifies that work so DBWorker can track its completion; the model must have a single primary-key column.

`eligible` controls which inputs can be claimed next. Without it, any new entry can be picked up. Add it when work should wait for a condition or run in a particular order.

Decorate your handler like this. The comments describe where your application logic goes:

**Complete the work in one invocation:**

```python
from sqlalchemy import select
from sqlalchemy.orm import Session
from dbworker import Finished

@coordinator.transactional_worker(
    name="process",
    source=YourModel,
    eligible=lambda: (
        select(YourModel)
        .where(YourModel.your_bool_property.is_(True))
        .order_by(YourModel.id)
    ),
    concurrency=4,
)
def process(source: YourModel, session: Session) -> Finished:
    # Your procedure goes here.
    # Return Finished() when the procedure is complete.
    return Finished()
```

- `name` identifies the worker for status queries.
- `concurrency=4` allows four handlers to run in child processes.
- `source` is the selected instance of your source model.
- `session` is a normal SQLAlchemy session supplied by DBWorker. The handler can leave either argument unused.

Before each claim, DBWorker calls `eligible` and uses its query to select the next item. If nothing can be claimed, it waits and checks again. If your application later enables an item, a subsequent check can select it. Finished, failed, and currently claimed items are excluded automatically; you do not need to put those checks in your query. Omitting `eligible` lets DBWorker select from all entries in `source`.

**Process part of the work, then continue in another invocation:**

```python
from dbworker import Finished, Outcome, Unfinished

@coordinator.transactional_worker(
    name="process_in_steps",
    source=YourModel,
    eligible=lambda: (
        select(YourModel)
        .where(YourModel.your_bool_property.is_(True))
        .order_by(YourModel.id)
    ),
    concurrency=4,
)
def process_in_steps(source: YourModel, session: Session) -> Outcome:
    # Your procedure goes here.
    # Return Unfinished() if it needs another invocation to continue.
    # Return Finished() instead when it is complete.
    return Unfinished()
```

`Finished()` means the work item is complete. `Unfinished()` means this invocation is done, but more work remains. Both save the execution status and commit any writes made through the supplied session; the handler does not have to make any database writes. After `Unfinished()`, DBWorker can invoke the handler again when eligible. Each invocation starts the function from the beginning. Your procedure decides how to continue; DBWorker does not save its position in the function. Use `Outcome` as the return annotation when a handler can return either result.

If the handler raises an exception, DBWorker rolls back its transaction and marks the work failed. If your procedure has prerequisites that can be checked in the database, `eligible` can delay claiming until they are met.

DBWorker owns the handler's commit and session cleanup. Do not commit or close this session yourself. For long computations, you can release a read transaction with `session.rollback()` first. Copy needed values before rollback, and do this before making writes you want to keep.

### Start and stop

Register handlers at module scope in an importable module. In your worker service, initialize DBWorker's tables and start the coordinator:

```python
if __name__ == "__main__":
    coordinator.create_worker_tables()
    coordinator.start()
```

`start()` returns while workers keep running. When a source entry matches `eligible`, DBWorker can claim its work and call the handler—there is no enqueue call.

On service shutdown, call:

```python
coordinator.stop()
```

`stop()` stops new claims and waits for active handlers to finish. Your API can run independently, using the same database. See the [complete example](../examples/imagededup_system_dbwork/README.md) for runnable API and worker commands with shutdown handling.

## Track progress and coordinate dependent work

Your application may need to show whether work is running or complete. Another worker may also depend on that information: one procedure prepares something, and a second can begin only after preparation finishes. If preparation fails, the application may need to report that failure instead of continuing.

DBWorker tracks execution separately for each worker and source entry. Access it through the coordinator using the worker's registered name and the source entry's primary key; you do not need to manage or query DBWorker's internal tables yourself.

Use `execution_status()` to get the current status in your API or handler:

```python
from dbworker import ExecutionStatus

with session_factory() as session:
    status = coordinator.execution_status(
        session, worker="process", source_id=source_id,
    )
```

This returns `None` before the first claim, or an `ExecutionStatus` enum: `WORKING`, `UNFINISHED`, `FINISHED`, or `FAILED`.

Sometimes you need to select inputs based on the status of their work—for example, list only inputs whose processing has finished. Calling `execution_status()` for each input would mean checking them individually.

`has_execution_status()` lets you include that check in a database query. It returns a SQLAlchemy condition meaning: **“Does this worker have one of these execution statuses for this input?”** It does not run a query or return a Python `True` or `False` when called.

```python
finished = coordinator.has_execution_status(
    worker="process",
    source_id=YourModel.id,
    statuses=(ExecutionStatus.FINISHED,),
)
```

- `worker` names the registered worker whose status you want to check.
- `source_id` identifies its input. Using `YourModel.id` checks the corresponding input for each entry considered by the query.
- `statuses` contains the acceptable statuses. The condition matches if any one applies; an input with no execution record does not match.

Use the condition in an ordinary SQLAlchemy query:

```python
query = select(YourModel).where(finished)

with session_factory() as session:
    inputs = session.scalars(query).all()
```

This returns inputs whose work under `process` is finished. The database checks their statuses as part of this query.

The same condition can be combined with other query filters, including in an `eligible` query. Register the named worker before calling `has_execution_status()`.

## Understand failures and retry work

When a handler raises an exception, DBWorker records the error and marks the work as `FAILED`. It will not automatically run that work again. Your application may need to show what went wrong and let someone retry after correcting the cause.

`state()` reads the execution details for one input, giving you more information than its status alone:

```python
with session_factory() as session:
    state = coordinator.workers["process"].state(session, source_id)
    error = state["error"] if state is not None else None
```

`"process"` is the worker's registered name, and `source_id` is the input's primary key. The result is a mapping containing `execution_status`, the recorded `error`, and `lease_expires_at` (the claim's expiration time). It returns `None` if that worker has never claimed this input. You can use the error to explain the failure in your API or logs.

After correcting the cause, use `reset_failed()` to allow another attempt:

```python
with session_factory.begin() as session:
    reset = coordinator.workers["process"].reset_failed(session, source_id)
```

`reset_failed()` changes a failed execution to `UNFINISHED` and clears its recorded error and claim. It returns `True` if it reset a failed execution, or `False` if there was no failed execution to reset. The `begin()` block commits this change.

Changing `eligible` alone cannot bypass the exclusion of recorded `FAILED` work.
The [availability predicate](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L344-L365) and
[reset operation](https://github.com/zysilm/dbworker/blob/c6652e904b29c597988b2d3ba65e612de24825d7/src/dbworker.py#L120-L127) are separate checks.

Resetting does not call the handler immediately. The running coordinator can claim the work again when it matches `eligible`. The handler starts from the beginning; resetting does not delete application results or progress. If the handler has effects outside the database transaction, make them safe to repeat.

## Configuration

| `Coordinator` argument | Purpose | Default |
|---|---|---|
| `session_factory` | SQLAlchemy session factory used for coordination. | Required |
| `database_url` | Database URL used by child processes; use the same database. | Required |
| `engine_options` | SQLAlchemy `create_engine()` options for child processes. | `None` |
| `lease_seconds` | Claim lifetime without renewal. Active claims are renewed automatically. | `30` |
| `poll_seconds` | Initial wait after finding no claimable work. | `0.25` |
| `max_poll_seconds` | Maximum wait after exponential backoff. Successful claims continue without waiting. | `10` |

For SQLite, use a file-backed database. Worker names must start with a lowercase letter and contain only lowercase letters, digits and underscores.

### SQLite lock contention

SQLite permits only one writer at a time, even when workers own different rows.
DBWorker retries transient SQLite `SQLITE_BUSY`, `SQLITE_BUSY_RECOVERY`, and
`SQLITE_BUSY_TIMEOUT` errors from the final execution-status UPDATE in the same
transaction. Retries use backoff from 10 ms to 100 ms, with a five-second retry
window after the first error. Each database call can additionally wait for the
connection's configured busy timeout. The handler is not rerun, pending business
writes are preserved, and every attempt retains the original claim-token check.
An exhausted retry window follows the normal rollback and failure path.

This same-transaction retry applies only to the final status UPDATE. It does not retry handler
queries, flushes, or commits. Other errors, invalidated
connections, and transactions that SQLite has rolled back are propagated.
`SQLITE_BUSY_SNAPSHOT` is also propagated: an invalidated WAL read snapshot needs
a new transaction, so retrying the UPDATE cannot repair it. DBWorker does not
automatically restart handlers that may already have performed external effects.

Separately, if claiming work encounters SQLite `BUSY`, `BUSY_RECOVERY`,
`BUSY_TIMEOUT`, or `BUSY_SNAPSHOT`, the coordinator rolls back the claim transaction
and logs the error. It uses the existing polling backoff before trying again with
a fresh session, without rerunning handlers or marking work failed. This recovery
applies only to claiming, not lease renewal or recording handler failures.

For an application encountering contention, start with a connection timeout such
as `connect_args={"timeout": 5.0}` on **both** the application's coordination
engine and `Coordinator(engine_options={"connect_args": {"timeout": 5.0}})` for
child engines. Five seconds is sqlite3's default; increase it only if the
application can tolerate the additional wait. A timeout cannot repair a stale
snapshot or a read-to-write upgrade deadlock.

WAL mode can reduce reader/writer contention, but still allows only one writer.
If appropriate for your deployment, configure `PRAGMA journal_mode=WAL` once
during database setup before starting services. DBWorker does not change journal
mode or connection timeouts. Keep write transactions short, release read
transactions before long computations when safe (see the session guidance above),
and reduce worker concurrency if write contention dominates. Even one execution
slot can contend with the API or coordinator; use PostgreSQL or MySQL when the
workload requires many simultaneous writers.

References: [SQLite transaction behavior](https://www.sqlite.org/lang_transaction.html),
[SQLite result codes](https://www.sqlite.org/rescode.html), and
[SQLAlchemy SQLite transaction control](https://docs.sqlalchemy.org/en/20/dialects/sqlite.html#transactions-with-sqlite-and-the-sqlite3-driver).
