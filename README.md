# dbworker

**Your database rows are already the queue.**

A portable, single-script background worker for SQLAlchemy. No broker, no enqueue, no duplicate job model.

```text
Celery / Huey / Graphile Worker:
    event → job → worker

dbworker:
    row → worker
```

Requires Python 3.12+. Install into your application:

```sh
poetry add dbworker
```

## Usage

Register a handler for an existing SQLAlchemy model. DBWorker discovers eligible
rows, runs the handler in a child process, and commits its database changes with
the execution status.

```python
from sqlalchemy import select
from sqlalchemy.orm import Session
from dbworker import Coordinator, Finished

coordinator = Coordinator(session_factory, database_url=database_url)

@coordinator.transactional_worker(
    name="process",
    source=YourModel,
    eligible=lambda: (
        select(YourModel)
        .where(YourModel.your_bool_property.is_(True))
        .order_by(YourModel.id)
    ),
    concurrency=4,
)
def process(source: YourModel, session: Session) -> Finished:
    # Apply your application logic using the supplied session.
    return Finished()

if __name__ == "__main__":
    coordinator.create_worker_tables()
    coordinator.start()
```

`YourModel`, `session_factory`, and `database_url` come from your application.
`eligible` selects which rows can be claimed and in what order.
It is optional: omitting it selects from the whole model. DBWorker automatically
excludes completed, failed, and actively claimed work.
Return `Finished()` when processing is complete; no enqueue call is needed.

See the [Usage Instructions](https://github.com/zysilm/dbworker/blob/main/doc/usage.md) for eligibility queries, incremental
processing, service startup and shutdown, status tracking, dependencies,
failure handling, and configuration.

## Examples

- [Image deduplication](examples/imagededup_system_dbwork/README.md): independent FastAPI and worker services, artifact building, paged comparisons, and worker dependencies.
- [Redis + Celery equivalent](examples/imagededup_system_redis_celery/README.md).
- [Benchmarks](benchmarks/imagededup_benckmark/README.md) with structured JSON results.

## License

[MIT](LICENSE) © 2026 Ziyang Song.

<!-- benchmark-results:start -->
## Benchmark Results

Median wall time in seconds; **bold** marks the faster backend. Each experiment runs in a fresh Docker container on its own GitHub-hosted Ubuntu VM.

| Experiment | Scenario | Celery (s) | DBWorker (s) | Celery / DBWorker |
|---|---|---:|---:|---:|
| imagededup | build | **5.704** | 7.928 | 0.72 |
| imagededup | comparison | 71.883 | **66.507** | 1.08 |
| imagededup | mixed | 79.311 | **77.313** | 1.03 |
| superset | sql_lab_group_by | **4.719** | 4.968 | 0.95 |
| saleor | product_csv_export | **15.876** | 17.671 | 0.90 |
| paperless_ngx | native_unsplit_scan_ingestion | 127.933 | **49.188** | 2.60 |
| posthog | native_two_factor_notification | **3.425** | 4.504 | 0.76 |
| sentry | historical_native_email_fanout | **0.808** | 1.887 | 0.43 |

Run: `github-37916530400-1`. [Details and scope](doc/benchmark-results.md) · [JSON results](benchmarks/results/latest/index.json).
Scoped application workloads; Sentry uses historical 24.1.0. Ratios above 1 favor DBWorker; no cross-project average is computed.
<!-- benchmark-results:end -->
