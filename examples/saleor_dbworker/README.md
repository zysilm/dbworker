# Saleor product export variation

This sibling variation executes the pinned Saleor product-export task body as a
synchronous callable inside a DBWorker handler. It reuses the original CSV query,
batching, field conversion, file storage, and `ExportTask` success/failure hooks.
There is no Celery eager execution or broker publication in the DBWorker handler.
The common integration runtime rejects either operation if upstream code tries it.

The benchmark uses the same callable and lifecycle boundary for its Celery + Redis
baseline (`paired_durable_request`). This measures replacement of queue execution
for this operation; it does not claim compatibility with every native Saleor task.

Each backend owns a PostgreSQL cluster, initialized with the complete upstream
migration history, and a file-backed SQLite durable-request database. The replica
alias points to the same PostgreSQL primary: replica lag is outside this experiment.
Migrations run before timing. Their empty-database maintenance tasks execute eagerly
only during setup; eager mode is disabled before initializing either timed backend.

The deterministic fixture has products and variants with names, types, and SKUs.
The oracle independently constructs expected CSV headers and rows, and verifies
the stored file, exact pending/success event history, durable job status, and the
DBWorker finished ledger. An untimed invalid-field export validates the original
failed-state hook and pending/failed event history.

Payment, email, and webhook plugins are explicitly disabled through `PLUGINS=[]`
on both backends. The original notification dispatch function still runs against
the real empty plugin manager. Notification delivery, webhook continuation,
replication, retries, and crash recovery are not measured.

Provisioning requires Python 3.12, Redis, and PostgreSQL executables `initdb`,
`postgres`, and `psql` on `PATH`. PostgreSQL clusters must run as an unprivileged
user. Dependencies are constrained by `benchmarks/locks/saleor.txt`.
