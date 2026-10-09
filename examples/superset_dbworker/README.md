# Superset asynchronous SQL Lab variation

The Celery baseline starts the original `superset.tasks.celery_app:app`, loads the
upstream Docker development configuration, and submits queries through the original
authenticated SQL Lab REST API. Its original `sql_lab.get_sql_results` task executes
the query, stores asynchronous results, and updates Superset's query state. Results
are retrieved through the authenticated native results API inside the timed interval.
No benchmark-defined Celery task wraps the SQL function.

The DBWorker variation changes only asynchronous executor selection in `executor.py`.
The original REST API and `ASynchronousSqlJsonExecutor` still prepare each query and
its native task arguments. `SQLLabDispatch.delay` stores one durable `SQLLabJob` per
query. The handler reuses the original SQL Lab task body with those arguments,
including its original result-storage and user-context options, and verifies durable
query success and a results key. It does not combine queries or substitute a direct
SQL execution shortcut. Unexpected nested broker publication is rejected.

The full profile uses 100 independently submitted aggregation queries per backend
and repetition, plus two untimed warmups, over a deterministic 10,000-row table.
Five repetitions are configured. Each measured operation has exactly one `sql_lab`
job on both backends. Both execute the same operation-specific query and return ten aggregate rows;
an independently calculated oracle checks retrieved results and status.
Each query scans all 10,000 rows and adds its operation marker to each sum, so
substituting a different operation's input or result cannot pass the oracle.

Each arm owns SQLite metadata and analytical databases and an isolated Redis service.
The original configuration's filesystem results cache is redirected to an owned
directory. DBWorker scheduling adds its own SQLite job database. Declared overrides
isolate Redis/cache paths, metadata storage, fixture credentials and local test-client
security settings; upstream task code remains unchanged. Both backends use a concurrency
budget of two processes. Redis also serves upstream application infrastructure in
the DBWorker arm; DBWorker replaces task scheduling, not every dependency of Superset.

Timing starts with the first authenticated async API submission and ends after all
query success states, native result storage, authenticated result retrieval and
scheduler terminal events. The DBWorker arm additionally waits for every completion
ledger commit. Application startup, migrations, fixture creation and warmup are
outside measurement.

Admission checks the original worker command and live task origin from the pinned
checkout, including an origin proof inside each actual task worker. Persisted JSONL
traces bind publication arguments to worker arguments without publishing their values.
DBWorker records publication intent before job eligibility and success after its
completion ledger transaction commits. Result JSON includes explicit warmup identities,
clock-bracketed monotonic measurement boundaries, trace paths and hashes, and query,
SQL, user, root-node, result-key and output-digest bindings. Independent replay
requires one complete job per query and an equal graph on both arms; missing,
duplicate, unexpected or failed jobs prevent admission.
An untimed receipt preserves actual authenticated result payloads and retrieval
timestamps. Independent replay checks the warehouse fixture fingerprint, each
query's ten-row aggregate against its arithmetic oracle, and both per-query and
combined output fingerprints. This verifies returned results, not physical scan
telemetry.

The previously published full native run validates the earlier evidence format;
the stronger worker, argument and timing evidence requires a fresh CI run. Older
paired-callable Superset results must not be presented as native SQL Lab timings.
The selected experiment covers successful asynchronous SELECT queries on SQLite.
It does not verify PostgreSQL, task timeout/cancellation equivalence, crash recovery,
publication failure, or atomicity across Superset's ORM and the DBWorker ledger.
Upstream early acknowledgements and native task limits are retained on the Celery
side; DBWorker does not claim equivalent soft/hard timeout behavior.

Run the full suite from the repository root:

```sh
python benchmarks/run_all.py --profile full --suite superset --provision --output-dir benchmarks/results/superset-native-full
```

Provisioning uses the pinned Superset source and dependencies in
`benchmarks/locks/superset.txt`. Result directories must be new for each invocation.
