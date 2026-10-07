# Saleor export and email workflow variation

The Celery baseline starts the original `saleor.celeryconf:app` and publishes the
original `export-products` task. It retains upstream task classes, writer access
restriction, task hooks, plugin discovery, and the separate admin-email task.
No benchmark-defined Celery task wraps the business function.

The DBWorker variation stores one durable job per original task delivery in
`runtime.py`. An export invokes the unchanged upstream task body and its hooks;
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

Timing begins with task publication and ends after all exports, SMTP acceptance,
email sent events, and both job stages complete. The suite validates CSV rows
against an independent fixture oracle, email recipients, subjects and download
links, exact event membership, and an observed per-operation parent/child task
graph. Shared trace admission rejects missing, duplicate, failed, unexpected, or
uncorrelated business jobs. Warmups are explicitly correlated and excluded.

The comparison is scoped to the configured export-and-email workflow. Webhook
subscriptions, replica lag, retry/failure delivery, crash recovery, and external
side-effect deduplication are not measured. Historical paired-callable results
must not be relabeled as native workflow results.

Provisioning requires the pinned Python environment, Redis, PostgreSQL executables
`initdb`, `postgres`, and `psql`, and the dependencies in `benchmarks/locks/saleor.txt`.
PostgreSQL runs as an unprivileged user. Infrastructure overrides are in
`benchmark_settings.py`; the upstream plugin list and writer restriction are retained.
