# Durable worker example

A small FastAPI application demonstrating database-owned CPU work without a broker. Documents are converted to word-frequency features. Comparison requests score ready artifacts in the same workspace and retain only their best K matches.

## Run

From the repository root:

```sh
source .venv/bin/activate
cd example
poetry install
poetry run durable-worker-example-api
```

The API listens on `http://127.0.0.1:8001`. The default database is `example.db` in the current directory.

## Registering a worker

The runtime has no knowledge of artifacts or comparisons. An application registers one handler per workflow:

```python
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

## One handler, short transactions

```python
def build_artifact(artifact: FeatureArtifact, coordinator: Coordinator) -> Finished:
    with coordinator.session_factory() as session:
        text = session.get(Document, artifact.document_id).text

    features = coordinator.run_cpu(build_features, text)

    session = coordinator.save_session()
    session.get(FeatureArtifact, artifact.id).feature_json = features
    return Finished()
```

The handler runs in a bounded control thread. `coordinator.run_cpu(function, *args)` sends plain inputs to that workflow's process pool and waits for the result. CPU functions must be importable top-level functions with serializable inputs and outputs. Database sessions and ORM objects never go to child processes. Close read sessions before calling CPU functions.

`coordinator.save_session()` starts the current invocation’s final save transaction and verifies ownership atomically before application writes. The coordinator commits those writes together with the handler's returned outcome. An exception or invalid outcome rolls back the save. The handler must not commit, roll back, or close this session itself, and must do no lengthy work once saving starts. `coordinator.run_cpu()` rejects calls after the save transaction opens. Concurrent invocations have independent claims and save sessions, held internally by the coordinator and cleared when each invocation exits. `run_cpu()` and `save_session()` are only available during a handler invocation. For a handler with no database output, simply returning `Finished()` is enough to persist completion.

Arbitrary actions such as file exports are allowed. Their effects are outside the database transaction and must tolerate repetition if an execution loses its claim or crashes before recording completion.

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

Supported PostgreSQL/MySQL/MariaDB versions use a source-row `FOR UPDATE SKIP LOCKED` while acquiring ownership. SQLite and other backends use a conditional update. A unique source key arbitrates simultaneous first work-row creation. Claims commit before handlers start. Every save verifies the claim token; a replaced owner cannot commit results. Expiry makes work reclaimable, and the current owner may still finish if no replacement has acquired it.

Each registered workflow has bounded handler threads and a CPU process pool sized by its concurrency. Scheduling threads renew leases while handlers run. Shutdown stops new claims and drains active handlers while renewing their leases, then closes pools. A handler that never returns can therefore delay graceful shutdown. Capacity is still per application process; there is no global CPU limit across API processes.

`engine.py` remains SQLite-oriented. The worker's locking path requires live integration tests and suitable connection configuration/transactional tables before deployment with another backend. Database infrastructure errors can still stop a scheduling thread; this refactor does not add a retry policy.

## Tests

From this directory:

```sh
PYTHONPATH=src poetry run python -m unittest discover -s tests -v
```

Tests cover file-backed SQLite claim races, lease reclamation, atomic result/ownership rollback, late candidates, top-K retention, application progress, generic noninteger source keys, and API behavior with real process pools. PostgreSQL/MySQL still require live integration testing.
