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
| imagededup | Build 1,000 image hashes (one bulk import) | **5.781** | 6.066 | 0.95 | n/a; n/a |
| imagededup | Compare 1,000 images / 999,000 directed pairs | 82.087 | **77.527** | 1.06 | n/a; n/a |
| imagededup | Build + compare 1,000 images / 999,000 directed pairs | 91.669 | **90.453** | 1.01 | n/a; n/a |
| superset | SQL Lab: 3,000 queries / 10,000 rows | **130.809** | 146.255 | 0.89 | 34.381s / 1384; 41.335s / 1494 |
| saleor | Export: 1,000 x 256 products + 1,000 emails | **132.599** | 160.993 | 0.82 | 35.885s / 606; 48.863s / 792 |
| paperless_ngx | OCR: 200 scans, archive and index | 134.649 | **75.436** | 1.78 | 71.852s / 114; 47.910s / 51 |
| posthog | 2FA: 5,000 requests / 10,000 tasks | 115.049 | **110.402** | 1.04 | 0.109s / 15; 0.594s / 65 |
| sentry | Email: 2,000 requests / 4,000 deliveries | **60.263** | 60.300 | 1.00 | 0.033s / 20; 0.147s / 26 |

Run: `github-38214437264-1`. [Details and scope](doc/benchmark-results.md) · [JSON results](benchmarks/results/latest/index.json).
Scoped application workloads; Sentry uses historical 24.1.0. Wall time includes the fixed submission window, so a ratio near 1 does not prove equal processing capacity. Task P95 starts at publication; peak outstanding counts already published tasks. Ratios above 1 favor DBWorker; no cross-project average is computed.
<!-- benchmark-results:end -->
