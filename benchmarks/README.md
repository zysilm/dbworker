# Application benchmark harness

Run all six repository experiments sequentially from one Python entry point:

```sh
python benchmarks/run_all.py --profile full --provision
```

The standalone runner never launches suites or backend measurements in parallel.
Workers may process jobs concurrently inside an experiment. Backend order alternates
between repetitions. Each suite produces a result JSON; the invocation also produces
`index.json` and generates `report.md` only from admitted results.

## Native comparison contract and validation status

The implemented comparisons use each pinned application's original Celery app and
task entry points. DBWorker variations replace scheduling while preserving business
work and individual job granularity. No benchmark-defined Celery task wraps an
extracted business function. Child jobs remain separate jobs on both backends.

**All six native workflows passed full-profile validation in
[GitHub Actions run 37860143841](https://github.com/zysilm/dbworker/actions/runs/37860143841).**
The same run passed independent aggregate admission for all 80 measured samples.
The measured source revision is `7b9435d43976819a3c19bd92e4a62af08a955edb`.
Previously retained paired-callable and subprocess-bridge JSON files are historical
results; they must not be relabeled or published as native workflow measurements.
Capability research remains on the separate `extension1-research` branch.

| Suite | Implemented native workflow | Per-backend full workload in each repetition |
| --- | --- | --- |
| `imagededup` | This repository's original Celery example; image hashing, bounded comparison pages and mixed workloads | 1,000 measured images per scenario; actual build and scoring-page jobs are checked against matching inputs and page bounds |
| `superset` | Authenticated SQL Lab REST submission, original async task, result storage and authenticated retrieval | 100 query jobs against 10,000 fixture rows; one `sql_lab` job per query |
| `saleor` | Original export task and lifecycle, upstream plugins, separately queued admin-email notification | 100 exports of 256 products plus 100 email jobs; one export-to-email edge per operation; no webhook subscriptions |
| `paperless_ngx` | Original folder producer and unsplit ingestion task, tracked lifecycle and real OCR pipeline | 100 scans and 100 ingestion jobs; original documents, OCR text, archives, thumbnails and search are checked |
| `posthog` | Original 2FA notification task and child delivery task, with full native application/settings | 100 notification jobs plus 100 delivery jobs; rendering and SMTP delivery remain inside timing |
| `sentry` | Historical original `MessageBuilder.send_async` and native email tasks with persistent workers | 100 operations, each with two distinct recipients, producing 200 individual delivery jobs |

The upstream suites use sibling `*_dbworker` packages without editing the pinned
sources. Unexpected stages, unsupported continuations, missing jobs, duplicate
attempts and unequal task graphs prevent admission. A deliberately unsplit or
subscription-free fixture is a declared workload boundary, not evidence of support
for every workflow in that application.

## Provisioning and execution environments

Initialize the pinned sources and expose the required native binaries:

```sh
git submodule update --init --depth 1
python benchmarks/run_all.py --profile full --provision --output-dir benchmarks/results/my-full
```

Install `uv>=0.12.23`. Provisioning creates separate benchmark, Celery and DBWorker
environments under `benchmarks/environments/`; it does not activate shells. A private
orchestrator environment is bootstrapped when needed. Omit `--provision` to reuse
already provisioned environments. Installation, migrations, fixture generation,
worker startup and warmup are outside workload timing on both arms.

Native prerequisites include Redis, PostgreSQL (`initdb`, `postgres`, `psql`),
Tesseract with English and OSD data, Ghostscript, ImageMagick, Poppler and libmagic.
PostgreSQL runs as an unprivileged user. Native binary installation and interpreter
availability are host prerequisites. The image suite can download the real
MIRFLICKR corpus on its first run.

Most queue environments use Python 3.12. PostHog queue environments require exactly
Python 3.14.7 and provision the complete frozen upstream dependency graph rather
than a projected notification-only application. Its source imports, settings and
business functions remain original; environment overrides isolate local services.

Sentry is pinned to historical 24.1.0. Its native application must initialize inside
each backend's supported interpreter, including Python >=3.12 for DBWorker. Historical
Python 3.10 is not a substitute for a supported DBWorker runtime. Dependency or
initialization incompatibility produces a structured blocker; no per-operation
application subprocess bridge is used. This suite does not describe modern Sentry's
taskbroker.

The native Paperless Celery pool retains prefork and child recycling. Linux is
required for its admitted comparison; macOS threads are not used as a substitute.
Pool and lifecycle differences between the native app and DBWorker are recorded,
not silently normalized away.

Dependency constraints and provisioning rules live in `benchmarks/locks/` and
`benchmarks/provision.py`. JSON records interpreter, package, source and implementation
identities together with infrastructure overrides and available resource metrics.

## Suite selection and interpreter overrides

Use a new output directory for each invocation. Without `--suite`, the registry
entries run in their listed order. Failed provisioning or experiments retain error
reports and logs; later suites are still attempted rather than silently excluded.
To select a suite explicitly:

```sh
python benchmarks/run_all.py --profile full --suite saleor --provision --output-dir benchmarks/results/my-saleor-full
```

Override interpreter paths with `--interpreters PATH_TO_JSON`:

```json
{
  "superset": {
    "benchmark_python": "/opt/bench/superset-harness/bin/python",
    "celery_python": "/opt/bench/superset-celery/bin/python",
    "dbworker_python": "/opt/bench/superset-dbworker/bin/python"
  }
}
```

The registry is authoritative: only `full` is supported, with five repetitions,
1,000 measured images and eight warmup images for the image suite, or 100 measured
operations and two warmup operations for each upstream suite. Auxiliary jobs are
additional work, not replacements for the top-level operation count.

## Evidence, admission and measurement boundaries

Native admission checks the worker launch AST, the live original application/task
objects and their source identities, and the pristine pinned checkout. Retained
append-only JSONL traces describe submitted, started and terminal business jobs,
including operation identities and parent/child relationships. Result JSON includes
the trace path and SHA-256. Independent report admission replays these traces,
checks the expected graph per operation, and compares the two backend graphs.
Submission observations record publication intent before transport, so a fast
worker can correlate protocol-1 tasks. Intent alone is insufficient: every job
must also have matching execution and successful completion evidence.

Reports retain configuration, datasets, samples, validators, errors and artifacts.
Admission also checks source pins, full profile coverage, matching backend repetition
sets, normalized business-output digests and artifact checksums. Uncorrelated tasks,
unexpected stages, duplicated business work, terminal failures, invalid identities
and nonfinite metrics cannot become an official successful comparison. The image
suite retains native zero-write redeliveries and proven recovered SQL retries as
separate diagnostics. Attempts remain in the evidence, and recovery required
before terminal business outcomes contributes to wall time. The image suite also
uses an untimed final queue-drain barrier.
Recovered retries require exception evidence and a subsequent successful attempt
of the same original task. Exact committed input and candidate coverage remains
mandatory.

Timing begins with real submission and includes all required workflow stages and
business effects. Superset includes async result retrieval; notification workflows
include actual SMTP acceptance and their required application records. Each scenario
checks independent business outputs, not just queue acknowledgement or a completion
flag. Warmup identities are explicitly excluded from measured graphs.

Metrics vary by suite. Wall time and throughput are reported in the suite's own
units; unavailable CPU/RSS metrics are declared rather than fabricated. App-owned
ORM, file and SMTP effects may be independent of DBWorker's completion transaction.
These successful-work experiments do not establish crash-safe publication, atomic
remote side effects, equivalent retry policy, timeout handling or cancellation.
Superset and Paperless use declared SQLite business fixtures; PostgreSQL coordination
coverage is provided by the PostHog fixture rather than every suite.

## Generated Markdown and CI

Generate Markdown from retained evidence without starting applications:

```sh
python benchmarks/render_results.py --run-dir benchmarks/results/RUN_ID --output doc/benchmark-results.md
```

Official rendering requires every registered suite to pass a complete admitted
`full` invocation. `--allow-partial` is for clearly labeled diagnostic Markdown;
partial results do not replace the official performance document. Ratios use Celery
wall time / DBWorker wall time. Different workload units are not averaged into one
score.

The GitHub workflow runs on executable-input pushes to `main`, including merged
changes, and excludes documentation-only and result-only changes. It does not run
on pull requests. Six matrix jobs execute concurrently on separate fresh
`ubuntu-24.04` VMs. Each builds a new Docker image and provisions its own suite.
Celery and DBWorker measurements remain sequential within each matrix job; the
standalone Python entry point also runs all suites sequentially.

A final aggregation job waits for all six experiments. It independently validates
full workload coverage, source revision and pins, result checksums, native execution
evidence and persisted JSONL task traces. Only a complete admitted run replaces
`benchmarks/results/latest`, generates `doc/benchmark-results.md`, and updates the
compact final README table with median timings and a bold faster backend. Published
evidence includes result JSON and JSONL traces. Failure diagnostics are uploaded
separately with seven-day artifact retention.

Failed, blocked, incomplete or stale runs never update the official table. Publication
skips a run if `main` has advanced and uses a normal push with `GITHUB_TOKEN` write
permission; it never force-updates `main`. Paired measurements share one VM/container
within a suite, while hosted hardware can vary across runs.

Run harness checks in an environment containing the applicable dependencies:

```sh
PYTHONPATH=.:src benchmarks/environments/superset/dbworker/.venv/bin/python -m unittest discover -s benchmarks/tests
```
