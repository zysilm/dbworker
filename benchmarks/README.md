# Application Benchmark Harness

Run all six repository experiments sequentially from one Python entry point:

```sh
python benchmarks/run_all.py --profile full --provision
```

The runner never launches suites or backend measurements in parallel. It writes
one JSON per repository, an `index.json`, and a generated `report.md` after success. Application workers
can execute requests concurrently within one experiment; this is distinct from
running experiments concurrently. Backend order alternates between repetitions.

## Implemented experiments

The complete full profile passed on a local macOS host with 80 validated samples.
See [full index](results/all-full-validated/index.json) and
[local full results](../doc/benchmark-full-local-results.md).
GitHub-hosted Linux execution must be confirmed by the first CI run.
Capability research and planning are preserved separately on the `extension1-research` branch.

| Suite | Actual operation and checks | Current comparison scope |
|---|---|---|
| `imagededup` | Image hashes, paged top-K comparisons, mixed workloads; original full output validation | Existing two native example stacks |
| `superset` | Real SQL Lab aggregation of 10,000 rows; independent aggregate oracle, durable query success, DBWorker ledger | Paired durable requests; SQLite metadata, request and analytical databases |
| `saleor` | Real product/variant CSV export; exact rows, file hash, job status, pending/success events; untimed failure lifecycle | Paired durable requests; PostgreSQL business database, SQLite request database; plugins disabled equally |
| `paperless_ngx` | Unchanged ingestion pipeline; raster scans, real Tesseract/OCRmyPDF, persisted documents, originals, thumbnails and Tantivy search | Paired durable requests; SQLite; auxiliary Redis progress on both backends |
| `posthog` | Original template rendering and CSS inlining, original SMTP send, real MessagingRecord rows and complete receiver content validation | Paired durable requests; PostgreSQL; scoped real notification application; rendering occurs before timing |
| `sentry` | Original historical SMTP utility with real app initialization; multipart content, envelopes, headers and accepted counts | Historical 24.1.0; identical synchronous subprocess bridge on both backends |

The upstream suites measure specific application boundaries rather than complete
native Celery lifecycle replacement. They reuse pinned upstream code through
sibling `*_dbworker` packages. Nested Kombu publication and Celery eager execution
are rejected in DBWorker operations. A result is successful only after real output
and durable completion checks pass.

On macOS, Paperless's native parser libraries crash in fork children. Its diagnostic
Celery worker therefore uses two threads, while DBWorker uses spawned processes.
This difference is recorded and prevents interpreting local timing as an equivalent
process-pool performance comparison. Linux uses the configured process pool.

## Prerequisites and isolated provisioning

Initialize the pinned sources, install `uv>=0.12.23`, and expose required native binaries:

```sh
git submodule update --init --depth 1
python benchmarks/run_all.py --profile full --provision --output-dir benchmarks/results/my-full
```

Provisioning creates separate benchmark, Celery and DBWorker environments under
`benchmarks/environments/`; it does not activate shells or overwrite existing user
venvs. A private orchestrator environment is bootstrapped if its dependencies are
absent. Installation, migrations, fixtures and warm-up are outside workload timing.
Omit `--provision` when the environments already exist.

Required native inputs include Redis, PostgreSQL (`initdb`, `postgres`, `psql`),
Tesseract with English and OSD data, Ghostscript, ImageMagick, Poppler and libmagic.
PostgreSQL clusters run as an unprivileged user. Native binary installation and
interpreter builds are host prerequisites, not performed by the Python provisioner.
The first image run can download its real MIRFLICKR corpus.

Most queue environments use Python 3.12. PostHog's two queue environments require
exact Python 3.14.7; supply that interpreter when `uv` cannot download it on the
host platform. Its scoped dependency lock excludes unrelated analytics products
but executes the original notification models, templates and business module.
Selected support exports are compiled from their original AST nodes; source
projection boundaries are recorded in results.

Sentry uses modern Python 3.12 queue environments and a separate historical Python
3.10.20 application environment. The historical frozen requirements were generated
on 3.10 and do not resolve on 3.8. The lock includes an explicitly documented xmlsec
wheel compatibility adjustment, outside the measured SMTP feature. Both queue
backends include the same per-operation legacy initialization and bridge cost.
This experiment does not describe current Sentry's taskbroker.

Dependency constraints are retained in `benchmarks/locks/`; actual interpreter,
package, source and implementation identities appear in JSON. Native versions and
runner hardware remain inputs that must be fixed before performance baselines.

## Selection and interpreter overrides

An output directory must be new. Without `--suite`, all six registry entries run
in their listed order. Provisioning and experiment failures preserve per-suite JSON/log reports and do not silently exclude suites; later suites are still attempted.
For development, select a subset explicitly:

```sh
python benchmarks/run_all.py --profile full --suite saleor --provision --output-dir benchmarks/results/my-saleor-full
```

Override executable paths with `--interpreters PATH_TO_JSON`:

```json
{
  "superset": {
    "benchmark_python": "/opt/bench/superset-harness/bin/python",
    "celery_python": "/opt/bench/superset-celery/bin/python",
    "dbworker_python": "/opt/bench/superset-dbworker/bin/python"
  },
  "sentry": {
    "upstream_python": "/opt/bench/sentry-legacy/bin/python"
  }
}
```

The Sentry backend passes the configured upstream interpreter to the bridge
explicitly. It is a synchronous utility invocation, not a second task queue.

## Profiles, results and limitations

Full uses five repetitions: 1,000 measured images and 8 warm-up images,
or 100 upstream requests per backend and repetition. Saleor exports 256 products
each time. Only the full profile is supported; the registry is authoritative.

Reports retain configuration, source pins/hashes, datasets, samples, validators,
errors and artifacts. Comparable samples require identical backend repetition sets, all repetitions
requested by the registry profile, and matching normalized output digests. Nonfinite metrics, duplicate samples,
wrong identities and altered JSON checksums are rejected.

Image reports preserve stack resource measurements. Upstream experiments currently
record wall time and their own throughput unit; they do not provide a complete
stack CPU/memory or latency-distribution comparison. App-owned ORM/file/SMTP effects
are independent of DBWorker's completion transaction. A durable request alone does
not establish crash-safe publication, atomic remote effects or equivalent retry.
These capabilities are explicitly untested. Superset and Paperless remain SQLite
profiles; their planned PostgreSQL profiles are not implemented.

## Markdown and CI

Successful invocations automatically generate result-directory `report.md`.
Regenerate Markdown or write the dedicated publication document from retained JSON
without starting applications:

```sh
python benchmarks/render_results.py --run-dir benchmarks/results/RUN_ID --output doc/benchmark-results.md
```

Official rendering requires every registered suite to pass in a complete `full`
invocation. Add `--allow-partial` only for clearly labeled diagnostic partial
Markdown. Incomplete results do not overwrite the official performance document.
Ratios are Celery wall time / DBWorker wall time; different workload units are not
averaged into one score.

The benchmark workflow runs only on pushes to `main` that change executable
inputs; README, documentation and result-only updates are excluded. It does not
run on pull requests. Six matrix jobs run concurrently on separate fresh
`ubuntu-24.04` VMs, each building a new Docker image and provisioning only its
own suite. Celery and DBWorker measurements remain sequential within each suite.
The standalone Python entry point still runs every suite sequentially.

All jobs share a run ID and measured source revision. The final job accepts only
complete, checksum-valid full results matching the registered workload and source
pins. It replaces `benchmarks/results/latest`, writes `doc/benchmark-results.md`,
and replaces the final README table with median timings and a bold faster backend.
Only JSON evidence and generated Markdown enter the result commit. Failure logs
are uploaded separately with seven-day artifact retention. Failed or incomplete
runs never update README. Publication skips stale runs if `main` has advanced;
normal pushes honor branch protection and never force-update `main`. The workflow
requires permission for `GITHUB_TOKEN` to push to `main`.

GitHub-hosted VM hardware can vary between runs. The paired measurements share
one VM and container within each experiment; results include environment metadata.

Run harness checks in an environment containing psutil, Celery and SQLAlchemy:

```sh
PYTHONPATH=.:src benchmarks/environments/superset/dbworker/.venv/bin/python -m unittest discover -s benchmarks/tests
```
