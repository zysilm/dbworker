# Superset benchmark native Celery audit

## Verdict

The current SQL Lab suite does not meet the native application baseline requirement. It runs an original Superset SQL execution function, but schedules it through a benchmark-owned Celery application and writes completion into a benchmark-owned request table. The existing results are evidence for that paired function harness only, not for Superset's native asynchronous SQL Lab lifecycle.

The existing implementation does not reduce the number of DBWorker jobs relative to its Celery wrapper: each backend executes 100 measured jobs and two warm-up jobs per repetition. The more important mismatch is omitted asynchronous result storage and application submission/retrieval behavior on both sides. A native Celery migration must add that work to DBWorker as well.

## Sources and scope

- Pinned repository: `examples/superset`, commit `68f19947a8012ac587bb407c469abacbebae61ca`; `benchmarks/registry.json:24-39` records the pin, full profile of 100 requests, and five repetitions. The local submodule reports the same commit.
- This audit is a source inspection, not a new performance run or a fault-recovery test.
- References below are repository-relative paths and line numbers at audit time.

## Current execution and counts

| Property | Current Celery side | Current DBWorker side |
| --- | --- | --- |
| Entry point | `benchmark.execute_request` | Shared `integration` handler |
| Worker application | `benchmarks.upstream.celery_app:app` | `Coordinator` |
| Measured jobs per repetition | 100 request IDs, one `.delay()` each | 100 request rows, one handler execution each |
| Warm-up jobs per repetition | 2 | 2 |
| Repetitions | 5 | 5 |
| Measured executions across repetitions | 500 | 500 |
| All executions including warm-up | 510 | 510 |
| Execution body | `examples.superset_dbworker.adapter.execute()` | Same adapter |
| Requested result behavior | Task defaults: `return_results=True`, `store_results=False` | Same defaults |
| Completion oracle | `integration_request.result` is populated | Same result plus later ledger check |
| Concurrency | 2 worker processes on the Linux default pool | 2 DBWorker execution slots |

Evidence:

- The fixture creates 10,000 warehouse rows and 102 pre-created Superset `Query` rows per backend/repetition: `benchmarks/upstream/superset_backend.py:49-86`.
- Database `allow_run_async` is explicitly **false**: `benchmarks/upstream/superset_backend.py:75`.
- Worker launch selects the benchmark application and two workers: `benchmarks/upstream/superset_backend.py:113-117`.
- One `Request` is created per query ID; each Celery request is submitted individually: `benchmarks/upstream/superset_backend.py:121-132`.
- Warm-up and measured batches use disjoint query IDs: `benchmarks/upstream/superset_backend.py:144-145`.
- The shared adapter calls `get_sql_results.run(**payload)`, normalizes returned data/columns/status/rows, and does not publish the native task: `examples/superset_dbworker/adapter.py:12-24`.
- The payload only supplies `query_id` and `rendered_query`: `benchmarks/upstream/superset_backend.py:124-125`. The upstream task defaults are `return_results=True`, `store_results=False`: `examples/superset/superset/sql_lab.py:224-233`.
- The Celery wrapper creates/disposes an SQLAlchemy engine per task, reads the benchmark request, invokes the shared adapter, and writes its result in another transaction: `benchmarks/upstream/celery_app.py:24-36`.
- DBWorker uses the same adapter, releases its coordinator read transaction before calling the upstream ORM, and persists the result separately: `examples/dbworker_integration/runtime.py:87-104`.

### Nested task count

The selected original SQL Lab execution function has no direct `.delay()` or `.apply_async()` publication in `superset/sql_lab.py`. The native asynchronous executor publishes one `sql_lab.get_sql_results` task for each new query: `examples/superset/superset/sqllab/sql_json_executer.py:163-182`. For 100 distinct plain SELECT requests, the inspected native path therefore predicts 100 top-level SQL Lab tasks, not a 200-task fan-out.

This is a source-derived expectation, not a runtime measurement of all registered callbacks, custom hooks, or broker traffic. Neither side currently records an authoritative task-publication/completion trace. The DBWorker dispatch guard rejects nested Celery publication: `examples/dbworker_integration/runtime.py:37-55`. It must not silently substitute for implementing a real native fan-out if a future scenario adds one. Runtime traces are required before certifying counts.

## Native Superset behavior that the harness bypasses

### Worker setup and supported configuration

The native entry point is `superset.tasks.celery_app:app`. It initializes the Superset Flask application, exposes Superset's configured Celery app, resets database connection pools after worker fork, and connects task teardown: `examples/superset/superset/tasks/celery_app.py:18-70`.

Superset loads `CELERY_CONFIG` and installs its application-context task base: `examples/superset/superset/initialization/__init__.py:145-171`. The benchmark adapter provides its own context but does not preserve the native worker entry point and signal setup.

The upstream Docker worker command is `celery --app=superset.tasks.celery_app:app worker -O fair -l INFO --concurrency=${CELERYD_CONCURRENCY:-2}`: `examples/superset/docker/docker-bootstrap.sh:63-67`. Its default concurrency is already two.

There are multiple upstream profiles, and they must not be conflated:

- Default `superset/config.py:1985-2005`: SQLAlchemy/SQLite broker and result backend, prefetch 1, early acknowledgements, and a `100/s` annotation for SQL Lab.
- Upstream Docker `docker/pythonpath_dev/superset_config.py:63-114`: Redis broker/result backend, prefetch 1, early acknowledgements, and a filesystem SQL Lab results backend. Its local `CeleryConfig` does not contain the default `100/s` task annotation.
- Our wrapper `benchmarks/upstream/celery_app.py:19-21`: a new Celery application, Redis broker, late acknowledgements, worker-loss rejection, and ignored Celery results.

Changing acknowledgements, result behavior, or rate limits is a semantic/configuration change, not merely selecting a port. The replacement baseline should import the selected upstream Redis Docker configuration, with only explicit isolated environment paths and connection settings changed. No benchmark-owned Celery task or rewritten application should remain in this suite.

### Native submission and result storage

The SQL Lab API constructs an execution context, validates input, and runs the original `ExecuteSqlCommand`: `examples/superset/superset/sqllab/api.py:588-607`. It selects `ASynchronousSqlJsonExecutor` when `runAsync` is true: `examples/superset/superset/sqllab/sqllab_execution_context.py:83-85,121-122` and `examples/superset/superset/sqllab/api.py:640-653`.

The original command performs database resolution, query creation, access validation and SQL rendering: `examples/superset/superset/commands/sql_lab/execute.py:94-119,142-153`. These operations are bypassed by manually inserting `Query` objects before timing.

The asynchronous executor publishes the original task with:

```python
get_sql_results.delay(
    query_id,
    rendered_query,
    return_results=False,
    store_results=not execution_context.select_as_cta,
    username=get_username(),
    start_time=now_as_float(),
    expand_data=execution_context.expand_data,
    log_params=log_params,
)
```

It also invokes `task.forget()` and handles publication failure: `examples/superset/superset/sqllab/sql_json_executer.py:173-204`.

For the selected plain SELECT workload, native `store_results=True` triggers serialization, compression, results-backend writes, and recording `Query.results_key`: `examples/superset/superset/sql_lab.py:683-708,710-808`. Native asynchronous databases require a results backend: `examples/superset/superset/sql_lab.py:477-478`.

The current benchmark does not configure `RESULTS_BACKEND` and turns asynchronous database support off: `benchmarks/upstream/superset_backend.py:55-60,75`. Its returned result is instead written as benchmark JSON. This skips meaningful native business work. Counting equal jobs alone does not repair this mismatch.

Native result retrieval uses the SQL Lab `/results/` API and `SqlExecutionResultsCommand`: `examples/superset/superset/sqllab/api.py:490-542`. The results command validates the results backend, associated query, and access: `examples/superset/superset/commands/sql_lab/results.py:58-110`. That path is absent from the current timing and output oracle.

### Timeouts, lifecycle, and resources

The original task has a 21,600-second soft limit and 21,660-second hard limit: `examples/superset/superset/sql_lab.py:219-223`. Calling `.run()` inside a different task does not apply those task-level limits to the outer wrapper. DBWorker has a lease but no equivalent task-level soft/hard limits in this adapter. A success-only fixture may benchmark the shared normal path, but must not claim timeout parity.

The current timer includes benchmark request insertion, wrapper publication or DBWorker polling, and polling until every custom result appears: `benchmarks/upstream/superset_backend.py:121-142`. Superset `Query` creation is outside timing; native result retrieval is absent. The code checks business status and DBWorker ledger after stopping the timed interval: `benchmarks/upstream/superset_backend.py:146-158`.

Equal configured concurrency is proven. Equal total CPU, memory, and connection usage are not: the suite explicitly leaves CPU/RSS unavailable, `benchmarks/upstream/superset_backend.py:161-164`. Both stacks must receive the same container CPU/memory limits and an equivalent fresh warehouse/metadata/results fixture; total-stack accounting must include broker and coordinator costs.

## Proposed migration

1. Keep the pinned upstream checkout unchanged. Provision the original Redis Docker Celery configuration, native Superset worker entry point, SQL Lab results backend, and matched warehouse/metadata fixtures. Prefer a declared PostgreSQL metadata/warehouse profile for deployment realism; if SQLite is retained initially, state that choice explicitly.
2. Start the original worker command with the same two-process concurrency as the DBWorker variation. Record the imported upstream profile and any environment-only overrides. Do not define a benchmark Celery application/task or change upstream Celery task bodies.
3. Submit the same 100 distinct authenticated SQL Lab requests with `runAsync=True`, plus two warm-ups, through the original API. Use identical SQL, permissions, users, limits, expansion flags, and requested result semantics in both variants.
4. Implement the DBWorker sibling variation at the SQL JSON executor boundary. Reuse the original command, validation, rendering, and query creation. Replace only `ASynchronousSqlJsonExecutor` dispatch with one durable DBWorker job per original task and preserve its exact arguments. Never combine 100 original tasks into fewer larger handlers.
5. The DBWorker handler must invoke the unchanged SQL execution body with `return_results=False`, `store_results=True`, and the same username/start-time/expansion/log arguments. It must persist native results and `results_key` through the upstream business code rather than introduce a shortcut result table. Preserve submission-failure behavior where applicable and document unsupported timeout semantics.
6. Time the same boundary in both variants: first API submission through durable query success and successful native results retrieval for every request. Setup, warm-up and final equivalence assertions use the same policy on both sides. If a secondary worker-only timing is desired, report it separately rather than mixing boundaries.
7. Require exact operation IDs, top-level task count, observed child-task count, terminal success count, result-key count, and retrieved-result count. For this fixture the expected measured top-level count is 100 per side per repetition. Capture tracing without altering original task bodies and account for every unexpected child task.
8. Validate the independent ten-row aggregate oracle, columns, query rows, accessible persisted results, and no missing/duplicate business operations. Retain the original broker behavior and native output lifecycle in the Celery arm. Store all parity metadata in the suite JSON before admitting a performance summary.

## Blockers and unknowns

- The DBWorker SQL JSON executor variation does not exist yet; API wiring must be a sibling variation or injected executor selection that leaves the Celery arm untouched.
- Authentication, permission fixtures, async result storage, and retrieval must be provisioned before native API runs are possible. Current manually created queries do not validate these flows.
- Cross-ORM atomicity is not established. Superset's Flask ORM owns business transactions while DBWorker owns a separate SQLAlchemy session; the current handler explicitly releases its transaction. Do not describe this integration as atomically committing Superset business effects and the DBWorker ledger.
- Native retry, cancellation, worker-loss, publication failure, and timeout parity have not been proven. Run success-path performance only with explicit limitations until separate lifecycle checks exist.
- No runtime publication trace presently confirms the expected one-task-per-query native path. Custom extension hooks could add work; fail parity admission if observed task counts differ.
- Existing summaries already label `native_celery_lifecycle` untested: `benchmarks/upstream/suite.py:30-39`. They must not be relabeled as native results. A new run and comparison mode are required after migration.

## Audit status

Source audit complete. Implementation and native performance validation remain outstanding. Existing Superset performance numbers are not admitted as native application comparisons.
