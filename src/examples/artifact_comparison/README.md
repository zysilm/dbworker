# Durable worker example

A small FastAPI application demonstrating database-owned CPU work without a broker. Documents are converted to word-frequency features. Comparison requests score ready artifacts in the same workspace and retain only their best K matches.

## Run

From the repository root:

```sh
cd src/examples/artifact_comparison
poetry install
poetry run durable-worker-example-api
```

The API listens on `http://127.0.0.1:8001`. The default database is `example.db` in the current directory.

The example is an independent Poetry project. Its editable `dbworker` dependency points to the repository root (`../../..`); framework edits are used directly. FastAPI, the application models and the scoring ledger belong to this example. The parent `examples` directory is not a package.

## Registering a worker

The runtime has no knowledge of artifacts or comparisons. An application registers one handler per workflow:

```python
from dbworker import Coordinator, Worker

coordinator = Coordinator(
    session_factory,
    database_url=settings.database_url,
    poll_seconds=0.25,
    max_poll_seconds=10,
)
worker = Worker(
    name="artifact_build",
    source=FeatureArtifact,
    eligible=lambda: select(FeatureArtifact)
        .where(FeatureArtifact.feature_json.is_(None))
        .order_by(FeatureArtifact.id),
    handler=build_artifact,
    concurrency=2,
)
coordinator.register(worker)
coordinator.create_worker_tables()
coordinator.start()
```

`source` is a SQLAlchemy mapped model with one primary-key column. The generated `<name>_work` table has a unique `source_id` foreign key with the same column type, plus status, claim token, lease expiry, and error. Stable workflow names identify tables across restarts. Eligibility returns a SQLAlchemy SELECT of the source model and may use joins, subqueries, and application-defined ordering. With no eligibility callback, all source records are considered.

A source with no work row has not been claimed; its API status is `null`. Work rows and API responses use `working`, `unfinished`, `finished`, and `failed` directly. `Finished()` prevents further claims; `Unfinished()` releases ownership for a later eligible invocation. Failed work requires an explicit `worker.reset_failed(session, source_id)`; there are no automatic claim or task retry loops.

## One decorated handler, short transactions

```python
from dbworker import Finished, transactional
from sqlalchemy import update
from sqlalchemy.orm import Session


@transactional
def build_artifact(artifact: FeatureArtifact, session: Session) -> Finished:
    artifact_id = artifact.id
    document = session.get(Document, artifact.document_id)
    if document is None:
        raise ValueError("Artifact document no longer exists")
    text = document.text
    # Copy values before rollback expires ORM objects. No connection is held
    # while computing; the next SQL statement starts the final transaction.
    session.rollback()
    features = build_features(text)
    session.execute(update(FeatureArtifact).where(FeatureArtifact.id == artifact_id).values(feature_json=features))
    return Finished()

```

The handler is a normal importable function. `@transactional` declares that Worker owns its session and final commit; register it with `Worker(handler=build_artifact, ...)`. The decorator does not wrap the function or change calls made directly outside a Worker. Use `functools.partial` to bind serializable configuration, as the comparison handler does for its page size.

The entire handler runs in a child process: source loading, reads, JSON decoding, computation and writes. It receives `(source, session)`, with the source loaded into that session. The parent performs claiming, dispatch and lease renewal. Each child creates its engine and session factory once; sessions, connections, source objects and the parent Coordinator never cross process boundaries.

The session is a normal SQLAlchemy `Session`. The runtime owns closing and committing it. Return `Finished()` or `Unfinished()` instead of committing; attempts to commit the managed session inside a handler are rejected. Do not close it or bypass its transaction through raw connection commits. You can query, mutate ORM objects, execute SQL and flush normally.

A session can live for the invocation without holding a connection throughout. The example copies IDs and input data, then calls `session.rollback()` after its read-only phase. This releases the read transaction and connection before CPU work. Rollback expires ORM instances: retain plain values and do not read expired attributes while computing. It also discards pending writes, so this boundary must precede writes you intend to retain. The next SQL operation starts another transaction; there is no outer `session.begin()` spanning the function.

After the handler returns, the runtime conditionally updates task state using its ownership token in the final transaction, then commits application writes and outcome together. A replaced claim, exception or invalid outcome rolls back all writes in that transaction, including SQL already flushed. The ownership check no longer locks the work row before computation. Earlier rolled-back read transactions are not part of the final atomic save, and input data is not a frozen snapshot across those transactions.

Handlers and mapped source classes must be importable. Eligibility callbacks execute only in the parent and can remain lambdas or closures. Registered work-table metadata is reconstructed in children, including tables referenced by dependent workflows.

`database_url` must identify the same database used by FastAPI and the parent session factory. Optional serializable `engine_options` are passed to `create_engine()` in each child. SQLite must be file-backed. A handler that performs only external actions can still return `Finished()`, but external effects and independently committed transactions cannot be rolled back by the framework and must tolerate repetition.

## Application data and progress

- `FeatureArtifact` stores its document reference and feature output.
- `ComparisonRequest` stores query/workspace/K parameters and its scored count.
- `TopComparison` retains only the best K scores.
- `ScoredCandidate` records every completed request/candidate pair, including candidates discarded from the top-K.
- `artifact_build_work` and `comparison_work` contain execution ownership and state.

`domain/workflows.py` owns candidate selection, result aggregation, and completion tracking. The framework has no collection ledger or progress backend. Applications whose result tables already identify completed items can use those results directly.

A comparison claims one request, selects at most 50 ready unscored candidates, calculates a page, and saves the top-K, completion records, count, and outcome in one transaction. IDs order currently ready candidates but do not act as a cursor. A lower-ID artifact that becomes ready later will still be included.

Comparison eligibility requires a usable query and either ready unscored candidates or no remaining builds. This lets requests finish on empty pages and prevents requests waiting for data from blocking runnable requests behind them. Failed builds are excluded from outstanding builds; a failed query still needs a successful build before its comparison can run. Finished comparisons are not reopened for later imports or retried builds.

## Claims and execution

Supported PostgreSQL/MySQL/MariaDB versions use a source-row `FOR UPDATE SKIP LOCKED` while acquiring ownership. SQLite and other backends use a conditional update. A unique source key arbitrates simultaneous first work-row creation. Claims commit before handlers start. The final transaction verifies the claim token; a replaced owner cannot commit results. Expiry makes work reclaimable, and the current owner may still finish if no replacement has acquired it.

Polling adapts independently for each workflow. Successful claims fill available slots immediately, and job completion wakes the scheduler to refill capacity. An empty claim waits `poll_seconds` (default 0.25 seconds), then doubles the delay after each further empty claim: 0.5, 1, 2, 4, 8, up to `max_poll_seconds` (default 10 seconds). A successful claim resets the delay. Configure both values in `Settings` or the `Coordinator` constructor. New database entries are discovered at the next scheduled poll; lease renewal and shutdown do not wait for the backoff to expire.

Each registered workflow has one parent scheduling thread and a process pool sized by its concurrency. There are no handler thread pools or nested CPU pools. Scheduling threads renew leases while handlers run. Shutdown stops new claims and drains active handlers while renewing their leases, then closes pools. A handler that never returns can therefore delay graceful shutdown. Capacity is still per application process; there is no global CPU limit across API processes.

`engine.py` remains SQLite-oriented. The worker's locking path requires live integration tests and suitable connection configuration/transactional tables before deployment with another backend. Database infrastructure errors or a broken process pool can stop scheduling; there is no automatic pool restart or retry policy.

## Tests

From this directory:

```sh
poetry run python -m unittest discover -s tests -v
```

Tests cover file-backed SQLite claim races, lease reclamation, atomic result/ownership rollback, late candidates, top-K retention, application progress, generic noninteger source keys, child-process execution and reuse, failure rollback, shutdown lease renewal, and API behavior with real process pools. PostgreSQL/MySQL still require live integration testing.
