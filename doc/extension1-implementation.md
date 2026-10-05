# Extension 1 Implementation Status

Branch: `extension1`
Date: 2026-10-05
Status: all six real application operations passed complete sequential smoke and
full runs on the local macOS host; fixed-runner CI validation remains outstanding.

## Unified execution

The public entry point is:

```sh
python benchmarks/run_all.py --profile smoke --provision
python benchmarks/run_all.py --profile full --provision
```

It provisions independent application environments when requested, then runs every
registry suite in order. No suite or backend experiment runs in parallel. Each
experiment can have two concurrent application workers. Backend order alternates
across repetitions. One repository produces one suite JSON; an atomic `index.json`
records the complete invocation, suite identities, statuses and checksums.
Successful runs automatically generate `report.md` in their result directory;
smoke/partial Markdown is clearly diagnostic.

All six selected operations passed the same complete sequential invocation:

```sh
python benchmarks/run_all.py --profile smoke --provision --output-dir benchmarks/results/all-smoke-validated
```

It exited with code 0 and retained six passed suite reports, 16 validated samples,
a complete passed index, and an automatically generated Markdown report. See
[run index](../benchmarks/results/all-smoke-validated/index.json) and
[smoke results](benchmark-smoke-results.md). This is correctness evidence on a
macOS developer host. The complete full profile subsequently passed on this host; see
[local full results](benchmark-full-local-results.md). Fixed Linux runner and GitHub
CI execution have not been validated.

## Actual six-suite coverage

| Suite | Implemented operation | Verified output | Deliberate limits |
|---|---|---|---|
| Images | Existing hash build, top-K comparison and mixed work | Existing full hash/comparison oracle and completion checks | Original SQLite example profile; no new recovery policy |
| Superset | Actual SQL Lab read-only aggregation over 10,000 fixture rows | Independent aggregates, row count, durable Query success, finished ledger | SQLite; paired durable requests; no permission-denied case or full native Celery lifecycle |
| Saleor | Original product export body and original ExportTask hooks | Exact product/variant CSV rows and headers, file hash, successful status, exact pending/success events, untimed failed lifecycle | Real PostgreSQL business database; same-primary replica alias; empty plugin manager; notifications and webhooks excluded |
| Paperless-ngx | Unchanged consume_file plugin pipeline on raster scans | Real Tesseract/OCRmyPDF text, document rows, original hashes, thumbnails and real Tantivy content search | SQLite; auxiliary Redis progress; no AI/classification/workflow continuations; macOS pool difference recorded |
| PostHog | Original 2FA template/CSS rendering and SMTP business implementation | SMTP envelope and MIME recipients/sender/subject/text/HTML/headers, exact accepted message count, MessagingRecord sent state | Rendering outside timing; scoped notification model application; no ClickHouse, dedup/retry/rejection or full app initialization |
| Sentry 24.1.0 | Original historical SMTP utility after real app initialization | Independent multipart/Unicode fixture, envelope/headers, accepted counts, exactly one receiver message per request | Same synchronous subprocess bridge on both backends; no native notification task/model/template lifecycle |

The four same-process upstream suites use `paired_durable_request`: both backends
receive the same durable request and call the same real operation. Sentry uses
`paired_subprocess_bridge`: both queue backends invoke the same historical utility
launcher and include equal bridge initialization cost. Images retain the existing
native application comparison. These modes must not be conflated.

The sibling variations reuse upstream functions and hooks; no entire application
or business algorithm is rewritten. Five submodules remain pristine and pinned to
the reviewed commits. Historical Sentry does not represent current Sentry's queue.

## Environments and compatibility choices

- Benchmark orchestration and each backend have explicit independent interpreter
  paths. Most application queue environments use Python 3.12.
- Saleor executes complete upstream PostgreSQL migrations outside timing. Empty-DB
  maintenance tasks run eagerly during setup only; eager execution is disabled
  before measured backends begin. Both backends disable payment/email/webhook
  plugins using the same supported setting. The original notification dispatcher
  still runs against the actual empty plugin manager.
- Paperless runs real native OCR and search libraries. Its scoped lock omits unused
  lazy AI/embedding providers. Redis is a progress channel on both backends and a
  task broker only for Celery. On macOS, parser libraries crash in fork children;
  the local Celery diagnostic uses two threads while DBWorker uses two spawned
  processes. This is recorded and is not an equivalent process-pool performance
  comparison. The Linux process profile still requires validation.
- PostHog uses exact Python 3.14.7 for both queue backends. A scoped bootstrap loads
  the original email module, notification models and templates unchanged. Selected
  support utilities and the UUIDT base model are compiled from their original AST
  nodes to avoid starting unrelated analytics products; projection provenance is
  recorded. This is a real notification feature experiment, not full platform
  installation or analytics compatibility evidence.
- Sentry's frozen public requirements were generated on Python 3.10 and fail on
  Python 3.8 despite the older setup recommendation. Its real historical app uses
  Python 3.10.20, separately from modern Python 3.12 queue workers. The resolved
  historical lock changes xmlsec 1.3.13 to 1.3.17 for wheel compatibility; XML
  signing is outside this workload. Old PyYAML build constraints are retained.
- `--provision` installs Python dependencies in private environments. Native tools,
  supported interpreter builds and fixed-runner hardware remain host prerequisites.

## Harness and validation

The runner records source and implementation hashes, actual interpreter/package
metadata, suite configuration, dataset identity, samples, logs and errors. Provisioning failure or a missing
runtime produces an unsuccessful per-suite report, and subsequent suites still run. Partial
selection is explicit; it cannot masquerade as complete coverage.

The shared DBWorker integration rejects hidden Kombu publication and eager Celery
execution. Real output and durable business state are checked before declaring a
sample passed. Finished DBWorker ledger entries are validated. Resource ownership
and descendant creation identities support timeout cleanup, including children in
separate process sessions. Databases, services and storage are invocation-owned;
existing developer services are not reconfigured.

Renderer validation rejects malformed/nonfinite metrics, wrong identities,
unpaired or missing profile-requested repetitions, digest mismatches and changed JSON checksums. The official
Markdown renderer accepts only a successful complete full invocation. Smoke and
partial results can generate explicitly diagnostic Markdown with `--allow-partial`.
Different workload units are never combined into a global throughput score.

Warm-up, fixtures, migrations, environment installation and final validators are
outside timed work. PostHog rendering also occurs outside timing because the
selected send boundary receives an already-rendered payload. Sentry includes its
per-request bridge initialization cost on both backends. Upstream suites measure
wall time and feature throughput; complete stack resource/latency instrumentation
is still missing. Image stack resource measurements are preserved.

## Execution evidence and earlier milestone

The complete sequential smoke invocation establishes all six operations described
above. Developer-host smoke timing is correctness evidence, not a fixed-runner
performance conclusion. Final validation passed 81 tests: 17 core, 11 original
image benchmark, 28 DBWorker image application, 14 Celery image application, and
11 harness tests. The harness was repeated after final report changes and passed.

Earlier retained evidence predates the remaining integrations:

- [Image smoke JSON](../benchmarks/results/extension1-image-smoke-4/imagededup.json)
  and [diagnostic Markdown](benchmark-image-smoke.md).
- [Superset smoke JSON](../benchmarks/results/extension1-superset-smoke-3/superset.json)
  and [diagnostic Markdown](benchmark-superset-smoke.md).
- [First six-suite attempt](../benchmarks/results/extension1-all-smoke/index.json)
  recorded images/Superset passed and four then-pending suites blocked. This is
  historical first-milestone evidence, not the current admission state.

The first milestone ran 17 existing DBWorker tests, 11 image benchmark tests and
8 harness tests successfully. Subsequent validation must be reported with its
actual invocation rather than treating this old count as new evidence.

## Corrections discovered through real execution

- Image imports must authorize the invocation-owned work directory, where prepared
  input links actually reside, rather than only the corpus parent directory.
- Image/Superset Celery prefork needed an explicit fork start method on this macOS
  host. Paperless's parser fork crashes require a separately disclosed thread pool.
- Superset initialization precedes importing encrypted application models, and the
  migration CLI needs rich and the repository-local superset-core package in its
  admission recipe. The constraint file must not retain machine-specific editable
  paths.
- Saleor's migrations and workers need the original GraphQL initialization order;
  production schema imports also require pytest at the pinned source. Real
  migrations publish maintenance jobs, handled outside measured work during setup.
- PostHog's light SMTP feature still requires real notification settings/model
  initialization and exact interpreter compatibility. Dependency slicing is
  explicit and shared across backends rather than substituting fake sends.
- Historical Sentry dependency resolution established Python 3.10 compatibility,
  so the original Python 3.8 draft bridge choice was replaced and documented.

## Remaining milestones

1. Validate a complete sequential six-suite smoke and the default full profile;
   retain one JSON per repository plus an index and generated report.
2. Validate native provisioning and experiments on the dedicated Linux runner,
   including Paperless's process pool and complete service/interpreter version locks.
3. Validate the all-six-suite single-job PR workflow and full-run report writeback,
   then enable scheduling after runner validation.
4. Add PostgreSQL profiles for Superset and Paperless, complete resource/latency
   metrics, and separately label cold/startup measurements where required.
5. Implement and validate equivalent replay, retry, publication/outbox and fault
   policies before claiming recovery or production delivery guarantees.

The PR workflow now uses one sequential all-six-suite job through the same public
entry point; there is no parallel experiment matrix. The workflow files exist,
but no remote CI run or report publication is claimed.
Upstream ORM/file/SMTP effects commit independently from DBWorker's completion
transaction; durable request persistence does not make these effects atomic.

## Complete local full execution

The existing isolated environments ran the default full registry sequentially:

```sh
python3 benchmarks/run_all.py --profile full --output-dir benchmarks/results/all-full-validated
```

The process exited with code 0. All six suite reports passed, with 30 image samples
and 10 samples for each of five upstream projects (80 total). Every scenario has
five samples per backend. Report checksums, implementation source hashes, source
identity, sequential suite timestamps, repetition completeness and output digests
were verified. Approximate orchestration wall time was 95.5 minutes.

See [index](../benchmarks/results/all-full-validated/index.json) and
[local full report](benchmark-full-local-results.md), including medians, observed
ranges, environment, configuration and capability limits. Renderer checks passed
after adding observed ranges to the Markdown table. No runtime source changed
during the measurement run. This is local full-profile evidence; Paperless's
macOS worker-pool mismatch and Sentry's bridge/large variation remain explicit.
Fixed Linux performance and remote CI/writeback still require validation.
