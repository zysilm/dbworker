# Native application benchmark audit

These findings describe the pre-migration implementation. The migration changes
are documented in the current example and benchmark READMEs; runtime admission
and newly measured results must be established independently.

This audit evaluates whether each experiment compares the upstream application's
native Celery execution with a DBWorker variation of the same application workflow.
The existing paired-callable results do not establish native application parity.
No new performance measurements are produced by this audit.

## Required comparison contract

- Start the pinned upstream Celery application and use its existing task entry
  points, routing, task classes and lifecycle. Do not introduce a replacement
  Celery task that calls an extracted business function.
- Use the upstream documented deployment configuration as the baseline. Record
  any environment overrides and their reason. Baseline validity and performance
  tuning are separate questions; native configuration is not proof of optimality.
- Submit identical input identities and quantities. Match individual task
  granularity and workflow stages. If the application publishes 200 independent
  tasks, the DBWorker variation must execute the corresponding 200 independent
  jobs, rather than fold them into a smaller number of batches.
- Account for fan-out, continuation tasks, hooks, result persistence and auxiliary
  operations. Equal top-level request counts alone do not prove equal work.
- Preserve business semantics, output artifacts, application transaction boundaries
  and completion criteria. Unsupported behavior is a blocker, not permission to
  omit that behavior from the DBWorker variation.
- Use equivalent hardware budgets and declared concurrency. Preserve native task
  lifecycle settings and report semantic differences rather than silently replace
  them with shared harness defaults.
- Define timing boundaries before measurement: publication begins inside the timed
  interval, and every required business outcome and continuation completes before
  it ends. Separate fixture generation, startup and warmup consistently.
- Verify submitted, completed, failed, missing and duplicate jobs by input identity
  and stage, and validate real business outputs independently of queue acknowledgments.

## Per-suite audits

Each repository has a separate audit, including the image example suite:

| Suite | Audit |
| --- | --- |
| Image deduplication | [imagededup.md](imagededup.md) |
| Superset | [superset.md](superset.md) |
| Saleor | [saleor.md](saleor.md) |
| Paperless-ngx | [paperless_ngx.md](paperless_ngx.md) |
| PostHog | [posthog.md](posthog.md) |
| Sentry | [sentry.md](sentry.md) |

## Findings

All six suites were inspected separately. The image suite uses this repository's
native Celery example. The five external-project suites use the benchmark-owned
`benchmark.execute_request` task instead of their native application task entry.

| Suite | Primary issue | Required business-job accounting |
| --- | --- | --- |
| Image deduplication | Native example retained; actual page execution and lifecycle parity remain unverified | Match image builds, comparison requests, scored pairs and page bounds; report control overhead separately |
| Superset | Native async result storage and retrieval are bypassed | One native SQL Lab job per selected query, with native result persistence and retrieval |
| Saleor | Native task invocation is bypassed and notification plugins disabled | Export jobs plus every enabled email/webhook child job; a configured email fixture predicts two business jobs per export |
| Paperless-ngx | Native task tracking, serialization and worker lifecycle are bypassed | One ingestion per unsplit scan, plus any actual split/deferred/workflow child jobs |
| PostHog | Notification task and timed rendering are omitted | The selected 2FA path predicts one notification job plus one delivery job per notification |
| Historical Sentry | A per-request app subprocess replaces native persistent workers | One delivery job per distinct recipient message, plus any selected preceding business stages |

These are source-derived workflow requirements, not newly observed native runtime
counts. No suite has newly passed native-comparison admission during this audit.
The existing successful single-recipient/request fixtures generally have equal
counts between their current two arms; that does not repair the omitted native
workflow stages or prove parity for richer fixtures.

This is a source audit. Unknown runtime task counts and unexecuted migration plans
must be labeled explicitly. Existing benchmark JSON files remain historical
paired-callable measurements and must not be relabeled as native baseline results.
