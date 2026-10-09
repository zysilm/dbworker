# Saleor export and email workflow variation

Both arms POST to the original `/graphql/` URL using Django's in-process client,
with upstream JWT authentication and real `MANAGE_PRODUCTS` permission checks.
The original `ExportProducts` mutation normalizes public GraphQL IDs and enums,
creates each `ExportFile` and its pending event, and publishes `export-products`.
The native baseline starts the original `saleor.celeryconf:app`. It retains upstream task classes, writer access
restriction, task hooks, plugin discovery, and the separate admin-email task.
No benchmark-defined Celery task wraps the business function.

The DBWorker variation intercepts only task publication while the original
producer runs and stores one durable job per original task delivery in `runtime.py`.
It does not recreate the mutation's objects, lifecycle events, or normalized input. An export invokes the unchanged upstream task body and its hooks;
notification publication inserts a separate email job instead of running the
email synchronously. The email job runs the original rendering, SMTP delivery,
and sent-event code. Both stages share a concurrency budget of two processes.
Unsupported continuation task names or delayed/linked dispatch fail closed.

The fixture restores upstream plugins, associates every export with a deterministic
staff user and email address, and uses an owned local SMTP receiver. It configures
no webhook subscriptions, so there are no webhook delivery jobs. The full workload
has 100 exports and 100 separate email jobs per backend/repetition, plus two
untimed export/email warmups. Five repetitions are configured by the registry.

Both arms own a PostgreSQL cluster with the complete upstream migrations and
filesystem artifact storage. The replica alias targets the same primary. DBWorker
coordination uses SQLite; Django business writes and job completion do not share a
transaction. Migrations may run maintenance tasks eagerly only outside measurement.
The upstream writer restriction remains enabled rather than bypassed.

Timing begins before the first authenticated GraphQL request, including permission
checks, input normalization, export creation and task publication. It ends after
all submission calls return and all exports, SMTP acceptance,
email sent events, and both job stages complete. The suite validates CSV rows
against an independent fixture oracle, email recipients, subjects and download
links, exact event membership, and an observed per-operation parent/child task
graph. Shared trace admission rejects missing, duplicate, failed, unexpected, or
uncorrelated business jobs. Warmups have explicit known identities and are excluded.
Captured Unix/monotonic boundaries cover every measured task event. Actual task
bodies are admitted inside the worker process, including the DBWorker handler.
A denied staff user's GraphQL request is checked outside timing and must not create
an export. Signing keys are generated only in memory for each isolated arm.

This request boundary supersedes the earlier task-publication-only measurement;
previous scores must not be described as measurements of this revised boundary.
A new GitHub full run is required to publish revised scores.

The comparison is scoped to the configured export-and-email workflow. Webhook
subscriptions, replica lag, retry/failure delivery, crash recovery, and external
side-effect deduplication are not measured. Historical paired-callable results
must not be relabeled as native workflow results.

Provisioning requires the pinned Python environment, Redis, PostgreSQL executables
`initdb`, `postgres`, and `psql`, and the dependencies in `benchmarks/locks/saleor.txt`.
PostgreSQL runs as an unprivileged user. Infrastructure overrides are in
`benchmark_settings.py`; the upstream plugin list and writer restriction are retained.
