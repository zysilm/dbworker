# Saleor native Celery and DBWorker parity audit

## Verdict

The current Saleor experiment does **not** measure Saleor's native Celery export workflow. Both backends execute the same upstream export body through a custom adapter, but the Celery worker belongs to the benchmark rather than Saleor. The measured top-level workload is numerically matched today: 100 export operations on each backend, each exporting the same 256 products. That equality does not establish parity with a normally configured Saleor workflow, because notification plugins, native task invocation, and native task lifecycle are bypassed or disabled.

This audit is based on local source inspection. No new benchmark was executed and no performance result was regenerated. References use the current checked-out source line numbers.

## Pinned scope and current task counts

- The registry pins Saleor commit `8385ca60ecefa3a068aed3d1a28188e096cdc543`: `benchmarks/registry.json:45`.
- The full profile has 100 requests, five repetitions, and 256 products: `benchmarks/registry.json:58`.
- Each repetition creates 102 successful export records, including two warmups: `benchmarks/upstream/saleor_backend.py:123`.
- Each backend processes two untimed warmup exports and 100 timed exports: `benchmarks/upstream/saleor_backend.py:205`.
- The Celery side publishes one custom `benchmark.execute_request` task per request row: `benchmarks/upstream/saleor_backend.py:190`, `benchmarks/upstream/celery_app.py:24`.
- The DBWorker side processes one durable request per export with the same payload and a concurrency of two: `benchmarks/upstream/saleor_backend.py:158`, `examples/dbworker_integration/runtime.py:100`.
- Across five repetitions, each backend executes 500 timed export operations and ten warmups. Each backend also runs five synchronous, untimed invalid-export checks, one per repetition, before its worker starts: `benchmarks/upstream/saleor_backend.py:135`.
- There are no configured notification plugin tasks in the current fixture: `PLUGINS = []` at `examples/saleor_dbworker/benchmark_settings.py:7`. Export records have neither a user nor an app: `benchmarks/upstream/saleor_backend.py:123`. The DBWorker publication guard also rejects nested Celery dispatch: `examples/dbworker_integration/runtime.py:37`.

There is no evidence of a 100-versus-ten top-level task discrepancy in the existing export scenario. Its important discrepancy is the omitted native workflow and side effects, not its existing top-level request count.

## Actual current execution paths

### Celery side

1. Create Saleor export records and pending events before timing: `benchmarks/upstream/saleor_backend.py:123`.
2. Start a custom worker with `-A benchmarks.upstream.celery_app:app`: `benchmarks/upstream/saleor_backend.py:175`.
3. During timing, insert all generic SQLAlchemy request rows into SQLite, then publish their identifiers individually: `benchmarks/upstream/saleor_backend.py:183`.
4. For every delivery, create and dispose an SQLAlchemy engine, read the request, call the shared adapter, and write a JSON completion result: `benchmarks/upstream/celery_app.py:25`.
5. The adapter calls `export_products_task.run`, then manually invokes `on_success` or `on_failure`: `examples/saleor_dbworker/adapter.py:25`.
6. End timing when every generic request has a non-null result: `benchmarks/upstream/saleor_backend.py:194`.

The custom app explicitly enables late acknowledgement, rejection on worker loss, ignored results, and prefetch one: `benchmarks/upstream/celery_app.py:19`. Those choices are not inherited from Saleor's Celery app. The custom request database, per-task engine creation, and completion write add work that a native Saleor export task does not require.

### DBWorker side

1. Use the same pre-created export records and SQLite request inserts.
2. Scan and execute each request separately through the integration handler: `examples/dbworker_integration/runtime.py:87`.
3. Call the same Saleor adapter and manually reproduce the export hooks.
4. Save the adapter result and complete the DBWorker ledger; end timing using the same generic result predicate.

Saleor's Django writes use PostgreSQL while coordination uses SQLite: `benchmarks/upstream/saleor_backend.py:95`. The integration handler rolls back its SQLAlchemy read transaction before invoking Django: `examples/dbworker_integration/runtime.py:89`. This is not a shared transaction across the Saleor business writes and the DBWorker completion ledger.

## Native upstream workflow

The upstream export mutation creates an `ExportFile` with its real requester, records the pending event, and publishes the original task directly: `examples/saleor/saleor/graphql/csv/mutations/export_products.py:123`.

The original worker app is `saleor.celeryconf:app`, with its own task base, Django configuration, autodiscovery, logging signals, and process telemetry initialization: `examples/saleor/saleor/celeryconf.py:14`, `examples/saleor/saleor/celeryconf.py:25`, `examples/saleor/saleor/celeryconf.py:32`. The upstream development worker command is `celery --app saleor.celeryconf:app worker -E`: `examples/saleor/pyproject.toml:183`.

The original task is registered as `export-products` with `ExportTask`, which derives from `RestrictWriterDBTask`: `examples/saleor/saleor/csv/tasks.py:20`, `examples/saleor/saleor/csv/tasks.py:58`. Native Celery invokes the task and its hooks. Failure clears the artifact, stores the failed status, emits the failure event, and notifies plugins; success stores the success status and event: `examples/saleor/saleor/csv/tasks.py:26`, `examples/saleor/saleor/csv/tasks.py:47`.

The export body preserves upstream product querying, batch size, prefetching, CSV generation, storage, and plugin notification: `examples/saleor/saleor/csv/utils/export.py:24`, `examples/saleor/saleor/csv/utils/export.py:47`, `examples/saleor/saleor/csv/utils/export.py:59`. Product batches are ordinary synchronous work within one export task, not additional Celery tasks: `examples/saleor/saleor/csv/utils/export.py:138`.

### Auxiliary task graph

Success notification calls `manager.notify` and `manager.product_export_completed`: `examples/saleor/saleor/csv/notifications.py:42`. Default built-in plugins include webhook and admin-email plugins: `examples/saleor/saleor/settings.py:944`.

- With an active admin-email plugin, a nonempty template, and an export user email, one successful export schedules `send_email_with_link_to_download_file_task`: `examples/saleor/saleor/plugins/admin_email/notify_events.py:42`. That task renders/sends mail and records the file-sent event: `examples/saleor/saleor/plugins/admin_email/tasks.py:22`.
- Consequently, a controlled 100-export fixture with exactly one applicable admin-email notification per export entails **100 export jobs plus 100 separate email jobs**, before any webhook jobs. DBWorker must retain those 200 logical jobs, rather than send mail synchronously inside only 100 export handlers.
- Failure notification similarly schedules a separate email job when the same conditions apply: `examples/saleor/saleor/plugins/admin_email/notify_events.py:92`.
- Configured `PRODUCT_EXPORT_COMPLETED` webhooks create another asynchronous path: `examples/saleor/saleor/plugins/webhook/plugin.py:739`, `examples/saleor/saleor/plugins/webhook/plugin.py:1868`. Transport can schedule payload-generation or webhook-delivery jobs: `examples/saleor/saleor/webhook/transport/asynchronous/transport.py:594`, `examples/saleor/saleor/webhook/transport/asynchronous/transport.py:714`.

Webhook task counts are conditional on subscriptions, payload mode, and delivery outcomes. An exact count for a restored plugin fixture is unknown until that fixture is defined and observed. Do not assume that restoring the plugin list automatically produces one additional task per export, or that the plugin list alone activates email without a recipient. Scheduled maintenance tasks are outside this export workload and should not run as uncontrolled background load on one backend only.

## Critical migration blocker

`examples/saleor_dbworker/benchmark_settings.py:18` sets `CELERY_RESTRICT_WRITER_METHOD = None`. In this pinned upstream version, `RestrictWriterDBTask.__call__` returns `None` without invoking its body when that setting is false: `examples/saleor/saleor/core/tasks.py:35`.

The current `.run()` adapter bypasses this behavior. Simply changing the worker and submission imports while retaining that setting can produce successful-looking task deliveries with no export work. Restore the upstream default `saleor.core.db.connection.log_writer_usage` from `examples/saleor/saleor/settings.py:294`, and prove that native tasks create artifacts and exact lifecycle events. Preserve the corresponding database-access behavior in the DBWorker variation.

## Required migration plan

1. Replace the custom Celery worker with the unmodified `saleor.celeryconf:app`. Set `CELERY_BROKER_URL` before Django initialization, use the owned Redis service, and keep eager execution restricted to explicitly identified untimed database setup. Record the native effective Celery configuration; do not silently impose the custom app's acknowledgement policy.
2. Submit the unmodified `saleor.csv.tasks.export_products_task.delay(...)` with the upstream argument structure. Do not place a benchmark-defined Celery task around it. Prefer exercising the actual upstream mutation for an application-level scenario; if measuring the task boundary directly, state that boundary and reproduce the same requester, export record, and pending event on both sides.
3. Restore the relevant upstream plugins and configure a deterministic export requester with an email address plus owned SMTP capture. Decide and record webhook subscriptions. Preserve upstream defaults unless an environment-specific override is necessary. An export-only fixture with notifications absent is a narrower scenario, not a substitute for a configured notification workflow.
4. Implement DBWorker job variations for export and every enabled auxiliary task. Preserve one durable queued job for each original Celery delivery. Reuse unchanged business functions, apply the equivalent writer restriction, and retain success/failure status updates, export events, email rendering/delivery, and webhook effects. Replace asynchronous publication only on the DBWorker side, not in the native baseline. Do not collapse child jobs into synchronous parent work or suppress them with the existing publication guard.
5. Keep the same top-level export count, products, variants, scope, fields, batch size, storage backend, business database topology, process budget, and fixture for both backends. Expose workload counts by task kind and parent/child correlation in result JSON, including auxiliary jobs and retries. Validate those counts before admitting a speed comparison.
6. Remove generic SQLite request/JSON-completion overhead from the native Celery side. Measure from the same business boundary on both backends: either request handling and submission through all required effects, or queued submission through all required effects. Export artifacts and pending records cannot be included in timing for only one side.
7. End the timed run after all requested business effects and queued child jobs complete, not just after CSV files exist. Validate CSV rows and hashes, terminal export states, lifecycle events, exact email count and content, and configured webhook delivery records. Email and export-success events can race in the native workflow; compare required event membership and legitimate ordering constraints rather than inventing an order that upstream does not guarantee.
8. Exercise failure through each real asynchronous backend. The current failure test is synchronous and bypasses the native Celery lifecycle for both sides; it does not validate original delivery behavior. Keep failure experiments outside the successful throughput result unless both backends receive the identical failure workload and retry policy.
9. Mark the comparison mode as native upstream application/task workflow only after the migration and task-graph assertions pass. Preserve current JSON as historical custom-wrapper results; do not relabel existing timings as native Saleor results.

## Validation gaps and unknowns

- Native worker startup and the restored plugin fixture have not been executed in this audit.
- The complete number of webhook auxiliary jobs requires a concrete subscription fixture and native execution tracing.
- Crash recovery, external side-effect deduplication, replica lag, and cross-database completion atomicity remain unverified; they are separate from successful throughput parity.
- Saleor's native default task invocation and telemetry/database hooks add behavior currently skipped. Its actual configuration must be captured from the running native app, rather than inferred from the custom benchmark worker.
- The fixture is small enough for one synchronous product batch per export; it does not exercise multibatch exports, richer product relations, or a full commerce workload.

## Acceptance criteria

The Saleor suite is ready to publish as a native comparison only when the Celery side contains no benchmark-defined Celery task; the original app and task execute normally; equal configured requesters and plugins are used; both sides execute identical counts of export, email, and webhook logical jobs; independent business-result checks pass; and both timers cover the same end-to-end boundary. Any unsupported child-task replacement must block that scenario rather than silently reduce the DBWorker workload.
