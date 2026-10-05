# Extension 1: Upstream Application Benchmark Plan

Date: 2026-10-05
Branch: `extension1`
Status: all six real feature integrations passed one complete sequential smoke invocation with provisioning. The default full profile subsequently passed on the local macOS host; fixed-runner CI validation remains outstanding; see [implementation status](extension1-implementation.md).

## Objective

Benchmark selected real application features with their existing Celery + Redis execution path and a DBWorker variation. Preserve upstream business logic, models, parsers, query engines, serializers, and validation wherever possible. Add small sibling packages named `<project>_dbworker`; do not fork or rewrite whole applications.

Every repository-level suite must produce one independently usable result JSON containing all its scenarios, backends, repetitions, validation results, and failures. The existing image comparison counts as one suite. A single Python entry point runs all registered suites sequentially and generates their JSON reports plus an index. It invokes the deterministic renderer automatically after a successful run to produce result-directory `report.md`; smoke/partial Markdown is labeled diagnostic. A render-only command regenerates Markdown from retained JSON and supplies the dedicated publication document after complete full validation.

The accepted CI policy is: small correctness experiments on pull requests; complete performance experiments on a fixed runner, on a schedule or manual trigger; automated Markdown updates from successful complete runs.

## Findings from the second source review

The original source review followed execution boundaries, transaction ownership, nested publication, fixtures, and declared dependencies. Its findings below are historical design evidence. Subsequent individual admission runs now execute all six selected operations; they do not establish that expanded scenarios, the full profile or CI have passed.

| Repository | Candidate boundary | Important observation | Initial choice |
|---|---|---|---|
| Existing image examples | Hash building and paged comparisons | Two isolated stacks and correctness checks already exist; monitoring is SQLite-specific | Reuse the current suite through an adapter |
| Superset | SQL Lab `get_sql_results` / `execute_sql_statements` | Real execution commits its own Flask SQLAlchemy session; wrappers establish request/user context | Read-only SQL Lab queries |
| Saleor | `export_products_task` / `export_products` | Export already has durable `ExportFile`; export utility calls plugin notifications | Product CSV export; selected webhook delivery later |
| Paperless-ngx | `consume_file` / ingestion plugins | Bound task uses request identity; plugins own Django transactions and file operations; progress uses channels | Actual document ingestion and reindexing |
| PostHog | `_send_email` / `_send_email_now` | Explicit undecorated synchronous implementation exists; `EmailMessage.send(retry=False)` can avoid eager Celery execution | Notification delivery; real query export later |
| Sentry 24.1.0 | `send_email` / email utility, then outbox draining | Historical Celery version, old Python environment; current mainline is not the historical Celery backend | Historical delivery suite with compatibility admission first |

Concrete findings affecting the architecture:

- Superset's reviewed requirements use SQLAlchemy 2.x, which is compatible in principle with DBWorker's dependency family. Its SQL Lab routine nevertheless calls `db.session.commit()` independently; sharing the library does not make it share DBWorker's supplied session.
- Saleor's reviewed Python range is `>=3.12,<3.13`. Product exports use the configured replica connection, generate nondeterministic filenames, and invoke plugin notifications. Seed data must be committed before dispatch; file contents, not UUID filenames, are the comparison oracle.
- Paperless-ngx declares Python `>=3.11`; select and validate a Python 3.12 environment. Its consumer tests contain fake parsers: those are useful fixture references but must not substitute for OCR/parsing in performance runs. Its defaults include child recycling and Redis-backed progress, so process lifecycle and auxiliary Redis use must be reported.
- PostHog's selected commit declares Python `==3.14.7`. Give it its own environment. Its export code is migrating toward Temporal and its full query path imports HogQL/ClickHouse machinery; start with the genuine synchronous email implementation rather than presenting a mock query renderer as an application export benchmark.
- Sentry 24.1.0's setup recommends Python 3.8, but its frozen public dependency set was generated on Python 3.10 and does not resolve on 3.8. The admitted integration uses Python 3.10.20 for the real historical app and Python 3.12 for both queue backends, through an identical synchronous bridge. A documented xmlsec wheel compatibility adjustment is outside the SMTP feature.
- Existing GitHub CI runs package tests in Python 3.12; it does not initialize submodules or execute live application benchmarks. The current benchmark's root-process checks also cannot detect a live process with a dead scheduling thread.

Pinned source evidence: [Superset execution](https://github.com/apache/superset/blob/68f19947a8012ac587bb407c469abacbebae61ca/superset/sql_lab.py), [Superset requirements](https://github.com/apache/superset/blob/68f19947a8012ac587bb407c469abacbebae61ca/pyproject.toml), [Saleor export utility](https://github.com/saleor/saleor/blob/8385ca60ecefa3a068aed3d1a28188e096cdc543/saleor/csv/utils/export.py), [Saleor requirements](https://github.com/saleor/saleor/blob/8385ca60ecefa3a068aed3d1a28188e096cdc543/pyproject.toml), [Paperless consumer](https://github.com/paperless-ngx/paperless-ngx/blob/8adbff1423af58575bc5a08eee7a6d833fd95651/src/documents/consumer.py), [Paperless settings](https://github.com/paperless-ngx/paperless-ngx/blob/8adbff1423af58575bc5a08eee7a6d833fd95651/src/paperless/settings/__init__.py), [PostHog email implementation](https://github.com/PostHog/posthog/blob/526d64dd82340b1bf4293d6d9baea7e965997048/posthog/email.py), [PostHog requirements](https://github.com/PostHog/posthog/blob/526d64dd82340b1bf4293d6d9baea7e965997048/pyproject.toml), [Sentry historical build](https://github.com/getsentry/sentry/blob/94bf1b8aad4c21840829be1f362b096d12f782d0/setup.py).

## Repository layout and source pinning

Implemented layout:

```text
examples/
  superset/                         # Git submodule, pinned commit
  superset_dbworker/                # Small integration package
  saleor/                           # Git submodule, pinned commit
  saleor_dbworker/
  paperless_ngx/                    # Git submodule, pinned commit
  paperless_ngx_dbworker/
  posthog/                          # Git submodule, pinned commit
  posthog_dbworker/
  sentry/                           # Historical Git submodule
  sentry_dbworker/
  imagededup_system_dbwork/          # Existing application
  imagededup_system_redis_celery/     # Existing application

benchmarks/
  run_all.py                        # Single public orchestration entry point
  render_results.py                 # JSON -> Markdown, no application startup
  registry.json                     # Suite/source/profile/environment definitions
  common/                          # Reporting, process ownership, measurement
  imagededup_benckmark/             # Existing project; retain current CLI
  superset_benchmark/
  saleor_benchmark/
  paperless_ngx_benchmark/
  posthog_benchmark/
  sentry_benchmark/
  results/<run_id>/
    imagededup.json
    superset.json
    saleor.json
    paperless_ngx.json
    posthog.json
    sentry.json
    index.json

doc/
  extension1-benchmark-plan.md
  benchmark-results.md              # Generated performance summary
```

Use official repository URLs and full commit gitlinks. Do not follow upstream heads during a run. Initially use the exact reviewed commits recorded in [the research manifest](../docs/research/celery-replacement-sources.json); upgrade a pin only through an explicit compatibility change. For Sentry use the historical 24.1.0 commit and label it in every result.

Keep submodule worktrees clean. Put adaptations in sibling packages, using composition, reviewed task-boundary wrappers, supported configuration, or small subclasses. If a source-level hook is unavoidable, store a minimal patch outside the submodule, apply it to a disposable build copy, record its hash, and apply equivalent shared changes to both backends. Do not duplicate upstream task bodies wholesale.

Independent virtual environments are the default execution architecture. Each suite may use a different Python version, and its Celery and DBWorker variants may use separate venvs. The orchestration/measurement environment is independent of every application environment. No repository-wide application dependency resolution or shared Python version is required.

The registry records explicit `benchmark_python`, `celery_python`, and `dbworker_python` executable paths, plus an optional `upstream_python` for a synchronous compatibility bridge. Launch commands use those executable paths directly; they do not depend on shell activation or a global `PATH`. Resolve relative paths against the repository root and pass an explicit working directory and environment to each subprocess. Spawned handler children must inherit the selected backend interpreter.

Derive each environment from locked upstream requirements plus the small adapter's dependencies. Prefer the same interpreter and shared business dependency versions for both backends within one comparison; separate venvs need not imply different versions. If compatibility requires different interpreter/dependency versions, report that difference as a comparison factor rather than attributing its effect solely to the worker framework. Record executable paths, interpreter versions, lock hashes, external binaries, and native extensions in JSON.

Environment provisioning is a separate untimed step. CI caches are keyed by suite, backend, interpreter, source pin, and dependency lock. Containers remain optional for service provisioning or native system dependencies; they are not required to isolate Python applications. Existing image-example venvs can be reused through the same executable-path contract.

## Integration contract for `_dbworker` variations

### Allowed changes

Each variation supplies only the required integration surface:

1. Application initialization in spawned handlers, with no worker startup during import.
2. A durable request model with one primary key, linking to the upstream business object and storing arguments/version where necessary.
3. A submission path that commits the request instead of sending a Celery message.
4. Importable DBWorker handlers invoking the selected upstream synchronous function or command.
5. Required follow-up request creation, dependency gates, status projection, and bounded retry bookkeeping.
6. Service entry points and health reporting.

Business algorithms, database queries, template rendering, OCR/parsers, export formats, and authorization remain upstream implementations. Fixtures initialize real upstream models. Fake business functions or mocked transport completions are excluded from performance measurements.

### Proving the execution backend

The DBWorker variation must not start Celery workers, Beat, Temporal workers, or another asynchronous task runtime for the selected workload. Audit reachable task publication, lifecycle signals, and plugin hooks. Any necessary asynchronous continuation must create DBWorker-owned durable work instead.

Celery may remain an installed upstream dependency because importing a module constructs task objects. This does not authorize dispatching work through Celery. Prefer existing undecorated synchronous routines. If a wrapper is reused, verify the exact call path and establish that no Celery retry/eager-execution machinery runs. A narrow compatibility context can supply the task identity needed by an upstream bound body; it must not emulate the entire Celery API.

Fail the suite if unexpected broker publication occurs in the variation. Do not turn on eager Celery mode and call that a DBWorker implementation. Auxiliary Redis used for cache, locks, or channels can remain when required by the selected feature; enumerate those roles and include their resources in both stacks.

### Transaction and recovery contract

Keep upstream ORM writes in their existing transactions initially. DBWorker owns only its supplied SQLAlchemy request/execution transaction. Before long upstream operations, release unnecessary reads and retain primitive identifiers. Never pass DBWorker's supplied session into a routine that commits independently.

For Django applications, request submission should use a small Django request model plus an explicitly mapped SQLAlchemy view of the same table, or another clearly documented shared-table contract. Insert business/request rows in one Django transaction when possible. Give DBWorker generated work tables separately. Do not use `transaction.on_commit()` to publish the only durable representation of requested work.

For each operation define what happens if upstream success commits but DBWorker completion does not. Reconcile durable output by request identity before replay. Do not assume business `SUCCESS` alone proves a newer or older request completed. If the upstream operation is not safely repeatable, report the fault case as unsupported until an adequate request-keyed guard or reconciliation path exists.

This initial integration does not claim atomicity across Django, Flask scoped sessions, SQLAlchemy coordination, remote databases, files, or receiver acceptance.

## Implemented experiment suites and planned expansion

Every suite writes exactly one JSON per invocation. All selected operations below
have passed individual Celery + Redis and DBWorker admission runs. Complete unified
and default full-profile runs must be verified separately. A scenario marked as
planned is not represented by a successful current result.

### 1. Image deduplication

**Implemented.** Reuse both existing applications, the real MIRFLICKR corpus and
existing hash/top-K validators. Build-only, comparison-only and mixed workload
scenarios are projected into the common schema while the original raw report and
resource measurements are retained. Smoke uses 8 images, 2 warm-up inputs and one
repetition. Full uses 1,000 images, 8 warm-up inputs and five repetitions.

**Planned expansion.** Idle-to-burst, additional corpus scales and PostgreSQL
monitoring remain separate changes. Quadratic comparison/storage cost must be
recorded when adding larger corpora.

### 2. Superset SQL Lab

**Implemented.** Both queue backends receive the same durable request and invoke
the original SQL Lab operation with real Flask context. A deterministic analytical
fixture contains 10,000 rows, category=i%10 and value=i. The selected aggregation
returns ten ordered rows; an independent oracle checks every total and count.
Durable Query success and DBWorker finished ledger entries are checked.

The current mode is `paired_durable_request`, using SQLite metadata, request and
analytical databases. It is not the complete native `sql_lab.get_sql_results`
Celery lifecycle. Upstream SQL Lab commits its own session independently.

**Planned expansion.** PostgreSQL, mixed query sizes, idle arrivals, permission-denied
requests, cold startup and native Celery lifecycle comparison are not implemented.
Do not infer permission/replay guarantees from successful aggregate queries.

### 3. Saleor product CSV export

**Implemented.** Reuse the original `export_products_task.run` body and its exact
`ExportTask.on_success/on_failure` hooks as synchronous callables. Product querying,
batching, field extraction and file storage remain upstream implementations. Each
backend owns a PostgreSQL business cluster initialized with the complete upstream
migration history and a separate SQLite request database. The replica alias points
to the same primary, so replica lag is not part of this scenario.

The fixture has one variant per product and exports ID, name, product type and SKU.
Smoke uses 24 products; full uses 256. Independently constructed expected headers
and rows verify every CSV, persisted hash, success state and exact pending/success
event history. An untimed invalid-field operation checks failed state and exact
pending/failed events. Both backends disable plugins equally through `PLUGINS=[]`.
The real notification manager still executes against its empty plugin set.

Historical maintenance tasks run eagerly only during empty-database migration
setup, outside timing. No eager Celery operation is allowed in the DBWorker handler.
The comparison is `paired_durable_request`, including the same original task hooks.

**Planned expansion.** Channel/attribute selections, large multi-batch exports,
notification delivery, selected webhooks, replica lag and retry/replay behavior are
not implemented. UUID filenames are excluded from output parity; content is checked.

### 4. Paperless-ngx raster document ingestion

**Implemented.** Invoke the unchanged `consume_file` body with only its reviewed
request identity context. The original preflight, ASN, collation, barcode, workflow,
consumer, parser, metadata, filesystem and search signal code remains upstream.
Deterministic raster-only PNG scans contain independent invoice/text markers;
real Tesseract/OCRmyPDF performs OCR. Validate persisted documents, extracted markers,
original SHA256, thumbnails, Tantivy content search and finished ledger entries.

The paired durable-request profile uses SQLite, English OCR, PDF output, no cleaning,
rotation or deskew, and no configured classification/workflow/AI operations. Redis
is an auxiliary progress channel on both backends; only Celery uses it as a broker.
The scoped dependency lock omits unused lazy AI embedding providers.

On macOS, upstream native parsers crash in Celery fork children, so development
admission uses two Celery threads against two DBWorker spawned workers. Results
record this difference. It is not an equivalent process-pool timing comparison;
the Linux process profile needs its own validation.

**Planned expansion.** PostgreSQL, text PDFs, mixed formats, duplicate/malformed
inputs, barcode splitting, workflow continuations, deferred lock-exhaustion indexing,
classification and AI are not admitted by this selected workload.

### 5. PostHog pre-rendered notification SMTP

**Implemented.** Both queue environments use the pinned Python 3.14.7 interpreter.
A scoped application bootstrap loads the original email module, MessagingRecord,
InstanceSetting and templates unchanged. Selected support utility functions and
UUIDTModel are compiled from original AST nodes; projections and source hashes are
recorded. This avoids starting unrelated analytics products without replacing any
selected notification algorithm, model field or SMTP operation.

Upstream EmailMessage renders the real 2FA template and inlines CSS before timing.
Independent semantic assertions check fixture content, and a real local SMTP sink
checks full envelopes, Unicode text/HTML, subject, sender/recipient, Reply-To, Date,
Message-ID uniqueness and custom headers. Original `_send_email_now` executes during
timing and writes real PostgreSQL MessagingRecord delivery rows. Receiver message
counts and per-campaign/recipient records must agree. The comparison is
`paired_durable_request`; `rendering_timed=false` is explicit.

**Planned expansion.** Whole PostHog initialization, ClickHouse/HogQL exports,
campaign deduplication, salt rotation, backpressure, rejection, retries and ambiguous
acceptance recovery are not verified. Native Celery autoretry/lifecycle comparison
would be a separately labeled scenario, not the current paired send boundary.

### 6. Historical Sentry 24.1.0 SMTP bridge

**Implemented.** Modern Python 3.12 Celery and DBWorker handlers invoke the same
synchronous launcher in the historical Python 3.10.20 application environment.
The launcher initializes the actual historical app and calls the original
`sentry.utils.email.send_messages` utility. Real options, logging and metrics remain
active. There is no second task queue. Both measurements include per-operation
historical initialization and bridge overhead: mode `paired_subprocess_bridge`.

A real SMTP receiver checks independently generated multipart Unicode content,
subject, envelopes, visible sender/recipient, Message-ID, custom headers, accepted
counts and exactly one message per request. Generated MIME boundaries and Date are
excluded from normalized parity; stable content and identities remain checked.

The historical public dependency graph is locked. Its xmlsec 1.3.17 compatibility
adjustment replaces 1.3.13 for an available wheel; XML signing is outside this
scenario. The old PyYAML build constraints are retained. The pinned source stays
clean. This does not establish native notification/task/model/template lifecycle
replacement or any fact about current Sentry's taskbroker.

**Planned expansion.** Backpressure, production notification models/templates,
native Celery task callbacks and durable outbox draining require separate real
fixtures and validation. The suite remains mandatory in complete runs.

## Experimental fairness

### Named comparisons and current coverage

1. **Native execution comparison:** upstream Celery dispatch and its documented business lifecycle versus the DBWorker variation. Record differences in retry, timeout, result storage, child recycling, and durability. This measures practical replacement cost for the selected feature.
2. **Paired durable-request comparison:** both backends receive the same durable request representation, inputs, synchronous business callable, and validation. The current upstream baseline publishes once after durable request commit; it does not implement crash-safe outbox recovery. Add a durable outbox before publication-recovery claims. This isolates execution/coordination differences more closely.

The current image suite uses its native example stacks; the four same-process upstream suites use paired durable requests, and Sentry uses a paired synchronous subprocess bridge. A full native upstream lifecycle comparison is planned separately. Do not combine these into one unlabeled speedup. A third synchronous reference can measure business-operation time without a worker; it is diagnostic and does not replace the async baseline.

### Shared conditions

- Match useful handler capacity, thread limits, fixture contents, service settings, batch sizes, and output semantics within a suite.
- Run backends sequentially on the performance runner and alternate order across repetitions. Each repetition gets fresh namespaced databases, storage, and sink state.
- Prepare environments, migrations, fixtures, warm-up, and final full validation outside workload timing. Record startup and cold behavior separately.
- Time from before request submission until durable business completion is observable. Also measure submission, queue wait, service time, and post-submission drain time where reliable instrumentation exists.
- Saleor and PostHog currently use PostgreSQL business data; image, Superset and Paperless profiles use SQLite. PostgreSQL Superset/Paperless profiles are future work. Record these distinctions and never average SQLite and PostgreSQL measurements together.
- Declare auxiliary Redis separately from broker/result Redis. Count all necessary services, including PostgreSQL, ClickHouse, SMTP/HTTP sinks, and subprocess children in total-stack resources.
- Preserve existing image resource metrics. Upstream suites currently record wall time and operation throughput; CPU by role, complete stack memory, connection/query contention, foreground latency and latency distributions are future instrumentation. Use stable runner identity before performance-baseline claims.
- Validate application outputs in addition to execution status. A swallowed application exception or missing artifact is a failed case, even when Celery or DBWorker reports a normal return.
- A fault run does not participate in ordinary success-throughput averages. Missing policies are explicit unsupported capabilities rather than artificially matched behavior.

## Suite result contract

Use a common outer schema (`schema_version: 1`) without discarding suite-specific detail. Its required fields are:

| Field | Meaning |
|---|---|
| `suite_id`, `run_id`, `profile` | Stable suite identity and invocation identity |
| `status` | `passed`, `failed`, `blocked`, `skipped`, or `running` |
| `source` | Official URL, pinned commit/tag, adapter version/hash, dirty state |
| `environment` | Runner class, hardware/OS, interpreters, package/lock/image versions |
| `configuration` | Backends, comparison mode, services, durability, concurrency, retry/timeout policy |
| `dataset` | Selection, size, generation seed or corpus version, fingerprints |
| `runs` | Scenario/backend/repetition records with metrics and validation |
| `summary` | Aggregates over successful comparable samples only |
| `capabilities` | Verified, excluded, unsupported, and untested behavior |
| `errors` | Setup/run/cleanup errors with phase, diagnostics, and artifact location |
| `artifacts` | Logs, raw upstream reports, outputs, and checksums |

Represent unavailable metrics with `null` plus a reason. Store units explicitly. Reject NaN/infinity. Mark timed samples with their actual repetition and comparison mode. Include normalized output digests and sample counts. Avoid committing real credentials, email addresses, or production data.

Write the JSON atomically at suite start and after every scenario. Per-suite environment provisioning and setup failures still produce the suite file, preserve logs, and allow later suites to report. The existing image result is retained as a raw artifact and projected into this schema through a compatibility adapter.

Each invocation owns an immutable result directory; new runs do not overwrite old experiments. An `index.json` lists the six suite reports, their statuses/hashes, and overall completion. It is an index, not a replacement for per-suite files. Full runs with missing or failed required suites are incomplete and cannot update the official performance table.

## Unified Python runner

Implemented commands (all selected experiments execute sequentially):

```sh
python benchmarks/run_all.py --profile smoke --provision --output-dir benchmarks/results/local-smoke
python benchmarks/run_all.py --profile full --provision --output-dir benchmarks/results/local-full
python benchmarks/run_all.py --profile smoke --suite saleor --provision --output-dir benchmarks/results/saleor-smoke
python benchmarks/render_results.py --run-dir benchmarks/results/local-full --output doc/benchmark-results.md
```

The registry defines each suite's source pin, venv provisioning recipe, explicit interpreter paths, entry point, profiles, fixture requirements, required services, output path, and admission status. The runner performs these steps:

1. Validate the manifest, pinned source availability, environment metadata, and empty/new output directory.
2. Invoke each suite's benchmark and backend commands through their configured venv interpreter paths. Keep the orchestration environment independent; use an additional upstream interpreter only when a declared compatibility bridge requires it.
3. Apply setup/run/cleanup deadlines, track owned roots and descendants, and collect progress/liveness separately from API availability.
4. Run selected suites strictly sequentially; keep diagnostic artifacts and write a valid JSON for each suite even when another fails.
5. Validate each schema, require every profile-requested repetition, compare backend outputs, aggregate eligible results, and write the index atomically. Generate `report.md` automatically after a successful invocation.
6. Exit nonzero if a required suite is failed, blocked, unexpectedly skipped, or invalid. An explicit partial selection is labeled partial and cannot masquerade as a complete full run.

The implemented render-only command regenerates Markdown from retained JSON without downloading datasets or restarting applications. Cleanup must release only services owned by that invocation, never an existing developer PostgreSQL/Redis instance. Container service IDs and process ownership are explicit.

## Profiles and CI

### Pull-request smoke

Use GitHub-hosted Linux runners and read-only repository permissions. The requested experiment contract is sequential: use the same runner entry point without parallel live benchmark jobs. Independent package tests may remain separate. Check out pinned submodules, restore caches keyed by source/environment hashes, and start real required services. The checked-in PR workflow uses one all-six-suite job and the public sequential entry point; it has no live benchmark matrix.

The checked-in PR workflow selects all six suites in a single sequential invocation, with per-suite isolated environments and real required services. Its Linux execution remains unverified until the workflow actually runs. Validate real outputs and upload JSON/log artifacts on success or failure. Include inexpensive negative cases and no-hidden-publication checks. Do not enforce small timing deltas on shared hosted hardware or infer successful CI from the workflow definition.

### Fixed-runner full performance

Run scheduled and manual jobs for trusted revisions on a dedicated labeled runner with an exclusive concurrency group. Invoke the same `python benchmarks/run_all.py --profile full --provision` entry point used locally, or omit provisioning on a pre-provisioned runner. Suite and backend timing runs remain sequential. Reserve database/sink resources, keep a stable CPU/memory configuration, and record runner identity.

Start with five repetitions per scenario after warm-up; allow per-suite sizes because workload units differ. Retain medians, ranges, and raw samples. Performance-regression gates need a compatible historical distribution and a practical threshold; do not compare a new source pin, hardware class, or workload shape directly against an incompatible baseline.

### Automatic Markdown updates

The renderer consumes validated complete-run JSON only and deterministically generates `doc/benchmark-results.md`. Include source pins, runner/profile, freshness, sample counts, validation verdicts, per-suite metrics, and links to raw JSON. State a ratio's direction, such as `Celery wall time / DBWorker wall time`. Never average image pairs, exported rows, SQL queries, and emails into a synthetic overall throughput score.

Keep generated content in a clearly delimited section or generate the entire dedicated report file. A failed/incomplete run uploads its diagnostics and does not overwrite the last valid performance summary. Preserve old results with links rather than silently publishing a partial success table.

The scheduled/manual reporting job can commit only the new validated result directory and generated Markdown to a dedicated results branch and open/update an automated pull request. The workflow and publication script implement this mechanism, but no remote execution or writeback validation is claimed; PR smoke does not write to branches or run arbitrary PR revisions on the performance runner. Repository permission and runner-label configuration are deployment inputs still to be supplied when wiring the workflow.

## Implementation milestones and exit criteria

| Milestone | Deliverable | Exit criterion |
|---|---|---|
| M1 | Common schema, image adapter, runner, renderer, smoke workflow | Existing image suite yields one valid suite JSON; deterministic Markdown and failure reporting work |
| M2 | Superset/Saleor submodules and variations | Real single-operation parity, no hidden task dispatch, then smoke/full cases |
| M3 | Paperless/PostHog submodules and variations | Real parser/render/send parity; pinned environments; documented transaction and auxiliary-service boundaries |
| M4 | Sentry historical submodule and variation | Compatible same-process or honestly labeled paired bridge; real utility parity; blocked state resolved |
| M5 | Complete performance matrix and report writeback | One full command produces six validated suite JSONs plus index; fixed-runner job updates Markdown automatically |
| M6 | Selected retries, revisions, dependencies, and faults | Explicit capability policies and barrier-based validations before adding performance claims |

The order is incremental, but the target remains all registered suites. Sentry cannot disappear behind an optional default without a recorded scope change. Adding a new feature to a repository extends its existing suite file rather than changing the one-file-per-repository contract.

## Remaining validation and expansion

Individual backend admission has resolved the selected operation's installation,
fixture and integration boundaries for all six repositories. Remaining work is:

- Linux/fixed-runner native provisioning, Paperless process-pool validation, exact
  service/interpreter locks, and all-six-suite PR execution validation.
- Remote CI report writeback and scheduled execution after runner validation.
- PostgreSQL Superset/Paperless profiles and complete resource/latency measurement.
- Request-keyed replay reconciliation, equivalent retry/publication policies and
  fault cases before adding recovery or production delivery claims.
- Expanded application scenarios explicitly listed above, without substituting
  fake business functions or mocked transport acceptance.

Individual admissions establish real selected feature execution, not completion of
all expanded design scenarios. A missing or failed suite still prevents a complete
run from publishing the official performance table.

## Complete smoke validation

On 2026-10-05, the single public runner provisioned independent environments and
ran all six suites sequentially with profile smoke. Every suite passed; the process
exited with code 0 and produced six suite JSONs, a complete index, and report.md.
See [index](../benchmarks/results/all-smoke-validated/index.json) and
[smoke results](benchmark-smoke-results.md). The default full profile subsequently passed locally. Fixed Linux execution and
remote CI/writeback validation remain outstanding.

## Complete local full validation

The default full registry ran sequentially on the local macOS host, exited with
code 0, and retained six passed suite JSONs, a passed complete index and Markdown.
All 80 samples passed; every scenario has five samples per backend. Approximate
orchestration wall time was 95.5 minutes. See
[full index](../benchmarks/results/all-full-validated/index.json) and
[local full results](benchmark-full-local-results.md). The documented macOS
Paperless pool mismatch and Sentry bridge/variation limit interpretation.
Fixed Linux CI performance and remote report writeback remain unvalidated.
