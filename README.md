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

Median wall time in seconds; **bold** marks the faster backend. Each experiment runs in a fresh Docker container on its own GitHub-hosted Ubuntu VM. Fixed profile: 8 producers, a 60-second scheduled submission window and 8 total execution slots; image hash building uses one native bulk import. Three repetitions per backend. Actual schedule delays and backlog are recorded in JSON; this is one load point, not a maximum-capacity search.

| Experiment | Workload | Celery wall (s) | DBWorker wall (s) | C / D | Task P95 / peak outstanding (C; D) |
|---|---|---:|---:|---:|---|
| imagededup | Build 1,000 image hashes (one bulk import) | 6.459 | **6.005** | 1.08 | n/a; n/a |
| imagededup | Compare 1,000 images / 999,000 directed pairs | 81.712 | **77.167** | 1.06 | n/a; n/a |
| imagededup | Build + compare 1,000 images / 999,000 directed pairs | 92.450 | **88.584** | 1.04 | n/a; n/a |
| superset | SQL Lab: 3,000 queries / 10,000 rows | **153.735** | 168.862 | 0.91 | 42.354s / 1462; 48.725s / 1511 |
| saleor | Export: 1,000 x 256 products + 1,000 emails | **137.652** | 167.635 | 0.82 | 39.572s / 630; 54.428s / 812 |
| paperless_ngx | OCR: 200 scans, archive and index | 142.059 | **81.849** | 1.74 | 78.973s / 118; 57.944s / 61 |
| posthog | 2FA: 5,000 requests / 10,000 tasks | 184.240 | **164.843** | 1.12 | 0.161s / 14; 0.179s / 30 |
| sentry | Email: 2,000 requests / 4,000 deliveries | **60.314** | 60.336 | 1.00 | 0.057s / 16; 0.479s / 149 |

Run: `github-38401468222-1`. [Details and scope](doc/benchmark-results.md) · [JSON results](benchmarks/results/latest/index.json).
Scoped application workloads; Sentry uses historical 24.1.0. Wall time includes the fixed submission window, so a ratio near 1 does not prove equal processing capacity. Task P95 starts at publication; peak outstanding counts already published tasks. Ratios above 1 favor DBWorker; no cross-project average is computed.
<!-- benchmark-results:end -->
