# Paperless-ngx native Celery benchmark audit

## Verdict

The current suite is a real OCR ingestion experiment, but its Celery baseline is a benchmark-owned durable-request wrapper, not the upstream Paperless Celery task lifecycle. Existing successful results establish business-output agreement for the restricted fixture. They do not establish a native Celery replacement comparison and must remain labeled `paired_durable_request`.

This is a source audit only. No experiment was rerun and no implementation, submodule, Git metadata or existing result artifact was changed. The pinned upstream source is commit `8adbff1423af58575bc5a08eee7a6d833fd95651`.

## Current execution and task counts

The harness constructs `N + 2` one-page, raster-only PNG scans, including two warm-up scans (`benchmarks/upstream/paperless_ngx_backend.py:96`). It starts the benchmark-owned `benchmarks.upstream.celery_app:app`, with two worker slots (`benchmarks/upstream/paperless_ngx_backend.py:112`). The measured loop inserts one SQLAlchemy `Request` per scan and publishes one `benchmark.execute_request` per request (`benchmarks/upstream/paperless_ngx_backend.py:123`). That wrapper reads the request, invokes the adapter, and persists the adapter result (`benchmarks/upstream/celery_app.py:24`).

The adapter extracts `consume_file.run.__func__`, supplies a synthetic request identity, and calls the bound task body directly (`examples/paperless_ngx_dbworker/adapter.py:21`). The DBWorker side invokes exactly the same adapter through one durable request handler (`examples/dbworker_integration/runtime.py:87`). Both therefore perform genuine upstream ingestion work, but neither submits the native `documents.tasks.consume_file` task through its normal lifecycle.

| Restricted fixture, per repetition | Current Celery | Current DBWorker | Required native baseline |
|---|---|---|---|
| Measured input scans | N | N | N |
| Warm-up scans | 2 | 2 | 2, separately attributed |
| Primary measured queue jobs | N benchmark wrapper tasks | N request claims | N native `documents.tasks.consume_file` publications/executions |
| Ingestion business body executions | N | N | N on successful unsplit scans |
| Native consume-file task publications | 0 in this primary path | 0 | N |
| Native tracked consumption lifecycle records | Not created by the wrapper task name | Not provided by the adapter | N tracked tasks with publication/start/terminal transitions |
| Synchronous successful index writes | One normal ingestion write per resulting root document | Same business path | Same business path; these are not additional queue jobs |
| Conditional child jobs | Not instrumented; fixture avoids them | Dispatch rejected if encountered | Observe and account for every child publication and execution |

These counts are derived from source, not an event trace. The current suite validates document count and completion rows (`benchmarks/upstream/paperless_ngx_backend.py:154`), but does not record the actual native task graph or task-attempt counts. In particular, do not infer that every Paperless deployment requires a separate indexing task for every scan: normal indexing is synchronous through `add_to_index` (`examples/paperless_ngx/src/documents/signals/handlers.py:800`).

## Native application and lifecycle omitted

The actual Celery application is `paperless.celery:app`: it registers the HMAC-signed pickle serializer, imports Django `CELERY_*` settings and autodiscovers tasks (`examples/paperless_ngx/src/paperless/celery.py:21`). Native producers submit `consume_file.apply_async` with `ConsumableDocument`, metadata overrides and a trigger-source header. The API upload producer is at `examples/paperless_ngx/src/documents/views.py:3521`; the folder consumer producer is at `examples/paperless_ngx/src/documents/management/commands/document_consumer.py:360`.

Native settings include signed-pickle task/results, Redis result storage, task-start tracking, task events, a hard time limit, and replacement of each child after one task (`examples/paperless_ngx/src/paperless/settings/__init__.py:682`). The benchmark application instead explicitly chooses late acknowledgement, prefetch one, worker-lost rejection and ignored results (`benchmarks/upstream/celery_app.py:19`). Those are benchmark policy choices, not evidence that Paperless uses that configuration.

Paperless tracks native task names, including `documents.tasks.consume_file` (`examples/paperless_ngx/src/documents/signals/handlers.py:1058`). Publication creates `PaperlessTask` with PENDING state and input/owner/trigger metadata (`examples/paperless_ngx/src/documents/signals/handlers.py:1163`). Prerun marks STARTED (`examples/paperless_ngx/src/documents/signals/handlers.py:1214`); postrun records terminal state, document result, duration and waiting time (`examples/paperless_ngx/src/documents/signals/handlers.py:1235`). Failure and revocation handlers maintain their own terminal paths (`examples/paperless_ngx/src/documents/signals/handlers.py:1296`, `examples/paperless_ngx/src/documents/signals/handlers.py:1361`). The wrapper task name is not tracked, and the direct body call does not emit the native consume-file lifecycle.

Parser initialization and Django connection-pool cleanup are genuine upstream worker lifecycle operations (`examples/paperless_ngx/src/paperless/celery.py:41`, `examples/paperless_ngx/src/documents/signals/handlers.py:1396`). They should remain in the baseline; they are not extra business jobs.

## Business stages, fanout and legitimate no-ops

The selected body retains preflight, ASN checks, collation, barcode handling, workflow trigger and consumer plugins, invoking each eligible plugin's setup/run/cleanup (`examples/paperless_ngx/src/documents/tasks.py:198`). The consumer performs parser/OCR, metadata extraction and file persistence (`examples/paperless_ngx/src/documents/consumer.py:530`, `examples/paperless_ngx/src/documents/consumer.py:597`), then emits consumption-finished signals (`examples/paperless_ngx/src/documents/consumer.py:673`). Signal registration includes metadata matching, workflows, search indexing and optional AI indexing (`examples/paperless_ngx/src/documents/apps.py:24`). These business operations are not to be replaced with benchmark-specific approximations.

Actual native fanout depends on the fixture and configuration:

- Barcode splitting creates K additional native `consume_file` tasks for K split documents, then stops the parent consumption (`examples/paperless_ngx/src/documents/barcodes.py:209`). An unsplit scan has no such child jobs.
- Index-lock exhaustion publishes `index_document` with a 60-second countdown (`examples/paperless_ngx/src/documents/search/_backend.py:687`). Its real body updates the index and has up to five autoretries with backoff/jitter (`examples/paperless_ngx/src/documents/tasks.py:91`). Successful synchronous indexing does not create this job.
- Enabled AI indexing publishes `update_document_in_llm_index` (`examples/paperless_ngx/src/documents/signals/handlers.py:1425`). The current scenario excludes AI; it must not claim AI parity.
- A configured webhook action publishes the real `send_webhook` job, including document bytes when requested (`examples/paperless_ngx/src/documents/workflows/actions.py:255`). Empty workflow configuration legitimately creates no webhook job; deleting that job from a populated workflow would remove business work.
- Mail ingestion constructs consumption signatures and a chord with a mail-action callback and error callback (`examples/paperless_ngx/src/paperless_mail/mail.py:366`). It is a distinct ingestion graph, not covered by submitting ordinary PNG uploads.

`index_optimize` is explicitly an upstream no-op because Tantivy manages merging (`examples/paperless_ngx/src/documents/tasks.py:85`). Broker ping, gossip, mingle and monitoring are control traffic, not document jobs. Lifecycle status writes are also not queue jobs, but are observable application behavior and cannot be replaced by a successful SQLAlchemy request row when evaluating lifecycle parity.

## Timing, outputs and resources

The current timer begins before request insertion and ends after adapter results are durable (`benchmarks/upstream/paperless_ngx_backend.py:123`). The adapter adds original/thumbnail checks, SHA256 hashing and search polling before its return (`examples/paperless_ngx_dbworker/adapter.py:30`); those extra oracle operations therefore run inside both measured task bodies. A native baseline should execute its unchanged task normally, while output validation occurs in the harness. Define the shared timed endpoint explicitly: all submitted native-equivalent jobs and continuations terminal, expected business output durable, index search-ready. Avoid ending at primary-task success while deferred work remains pending.

Current output checks cover OCR markers, original SHA256, rendered thumbnails and search hits. They omit native `PaperlessTask` records, full metadata/permissions, page count and archive content/integrity. Add those checks for the selected native API/folder fixture; normalize nondeterministic task IDs and timestamps without discarding status or relationships.

Only wall time and documents per second are reported (`benchmarks/upstream/paperless_ngx_backend.py:162`). There is no measured stack CPU, RSS, process-count, I/O or per-task latency. Two worker slots do not prove equivalent resource use. macOS chooses Celery threads while DBWorker uses spawn processes (`benchmarks/upstream/paperless_ngx_backend.py:110`). Native Paperless also recycles each worker child after a single task; the benchmark wrapper currently does not preserve that setting. Existing local ratios are therefore unsuitable as native process-pool conclusions.

## Concrete migration and admission plan

1. Preserve existing results as wrapper diagnostics. Introduce a distinct native-lifecycle scenario and schema metadata; do not relabel old JSON.
2. Launch pristine `paperless.celery:app` on a Linux runner with actual upstream settings, migrations, signed serializer, result backend, task signals, time limits and child recycling. Keep two explicitly configured worker slots; record every nondefault override. Do not silently disable child recycling for faster warm-up results.
3. Choose a precise producer boundary. Start with actual API-upload-equivalent arguments and trigger metadata, using native `consume_file.apply_async`; if claiming API or folder-consumer end-to-end coverage, invoke that real producer rather than reproducing its business setup. Match input files, user/permissions, overrides and initial application state across backends.
4. On DBWorker, keep the same upstream business body and introduce explicit persistent task identity, input metadata and application lifecycle transitions equivalent to `PaperlessTask`. Record one job per native business task, rather than embedding future child jobs into the primary handler. Map known continuation boundaries to durable DBWorker jobs while rejecting unknown Celery dispatch. Provide scheduled eligibility/retry state for native delayed indexing; preserve business work and declared attempt policy.
5. Add a task-graph observer recording task names, parent/root IDs, publication/start/terminal events, attempt counts and outputs. First admit the no-fanout scan fixture with N measured primary jobs per backend. Then separately admit deterministic barcode splitting and one workflow webhook against a real local HTTP receiver. Deferred-index and mail/chord cases need explicit fixtures and equal completion conditions; do not assume zero native fanout from a passing primary task.
6. Move validation into the harness and instrument all owned processes, including worker replacements, OCR child processes, Redis and database service. Apply the same CPU/thread caps and measurement interval. Report baseline-native versus DBWorker pool/recycling differences instead of presenting mismatched pools as equivalent.
7. Admit only after both sides match primary/child job counts, attempts, output relationships and lifecycle state for the declared fixture, and leave no business continuation pending. Then run suites sequentially through the common runner and generate a new result JSON with the native scenario label.

## Blockers and decisions before implementation

Native prefork parsing crashed on the previous macOS environment, so Linux native admission is required; substituting threads is a diagnostic fallback. DBWorker currently needs persistent delayed/retry and explicit task-lifecycle mapping for the broader graph. The body-only adapter cannot satisfy these by itself. Cross-ORM document and ledger transactions remain independent and require an explicit recovery/idempotency design for fault scenarios. Dependency/native-tool provisioning, worker-replacement observability and a deterministic continuation fixture also need implementation. This audit does not establish that native admission is currently runnable.
