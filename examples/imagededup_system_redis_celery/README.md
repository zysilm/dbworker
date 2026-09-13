# imagededup_system_redis_celery

FastAPI image deduplication using imagededup PHash, Redis and Celery. This is an independent Poetry project with no DBWorker dependency. Supply existing image files through the API; there is no dataset downloader or benchmark implementation.

## Install and run

Use Python 3.12, which has a prebuilt imagededup 0.3.3.post2 wheel. Imagededup's dependencies include PyTorch, although this example uses its perceptual hash and downloads no model weights.

```sh
cd examples/imagededup_system_redis_celery
poetry env use python3.12
poetry install
```

Start a Redis instance you control. For example, using an unused port and a local Redis installation:

```sh
mkdir -p /tmp/imagededup-redis
redis-server --port 6380 --dir /tmp/imagededup-redis --appendonly yes --appendfsync everysec
```

In each application terminal, enter the example directory and set the **same absolute database URL and broker URL**:

```sh
export IMAGE_DATABASE_URL="sqlite:////absolute/path/to/images.db"
export IMAGE_BROKER_URL="redis://127.0.0.1:6380/0"
```

Start the API first to create the SQL tables:

```sh
poetry run imagededup-system-redis-celery-api
```

Run the following commands in three additional terminals. These use separate queues so comparison tasks can run while image builds continue:

```sh
poetry run celery -A imagededup_system_redis_celery.celery_app:app worker \
  -Q image_build --pool=prefork --concurrency=2 --hostname=build@%h --loglevel=INFO
```

```sh
poetry run celery -A imagededup_system_redis_celery.celery_app:app worker \
  -Q image_compare,image_control --pool=prefork --concurrency=2 --hostname=compare@%h --loglevel=INFO
```

```sh
poetry run celery -A imagededup_system_redis_celery.celery_app:app beat \
  --schedule /tmp/imagededup-celerybeat --loglevel=INFO
```

Run one Beat scheduler per deployment. Beat recovers committed SQL outbox messages that have not reached Redis; normal API submissions and ready-page continuations publish immediately after their SQL transaction commits. The API does not embed or start workers. Stop workers gracefully so Celery can finish active tasks.

## API

Swagger UI: http://127.0.0.1:8002/docs

The endpoint and response shapes match the DBWorker image example. Use returned workspace, artifact and request IDs:

```sh
curl -s http://127.0.0.1:8002/workspaces \
  -H 'Content-Type: application/json' -d '{"name":"photos"}'

curl -s http://127.0.0.1:8002/workspaces/1/imports \
  -H 'Content-Type: application/json' \
  -d '{"directory":"/absolute/path/to/existing/images","limit":100}'

curl -s http://127.0.0.1:8002/comparisons/1 \
  -H 'Content-Type: application/json' \
  -d '{"retained_max_k":10,"max_distance":10}'

curl -s http://127.0.0.1:8002/comparisons/1
curl -s http://127.0.0.1:8002/comparisons/1/results
```

| Endpoint | Behavior |
|---|---|
| `POST /workspaces` | Create a workspace from `name`. |
| `POST /workspaces/{id}/imports` | Import existing files and enqueue builds; return `imported_images` and `artifact_ids`. |
| `GET /workspaces/{id}/artifacts?after_id=0&limit=100` | List artifact IDs, image IDs, execution status and errors. |
| `GET /artifacts/{id}` | Inspect one build. |
| `GET /artifacts/{id}/image` | Serve its original file. |
| `POST /workspaces/{id}/build-all` | Resubmit failed builds after fixing their files. |
| `POST /comparisons/{artifact_id}` | Submit a query, including one whose hash is not built yet. |
| `GET /comparisons/{id}` | Read status, `candidates_scored_count` and error. |
| `GET /comparisons/{id}/results` | Read current top-K `{candidate_artifact_id, distance}` pairs. |

Status is initially `null`, then `working`, `unfinished`, `finished` or `failed`. SQL status is read together with its corresponding progress; the API does not query Celery result objects. Results remain available while a request is unfinished.

Distance is the 0–64 Hamming distance between perceptual hashes. The default threshold is 10; zero means identical hashes, not necessarily identical files. Results sort by distance and then artifact ID. `max_distance=64` includes nearest neighbors even when they are not plausible duplicates. At most `retained_max_k` matches are stored.

Import supports JPEG, PNG, WebP, BMP and TIFF, skips previously imported paths in the same workspace, and accepts up to 25,000 files per call. This local-directory API assumes trusted local use. Files must remain accessible at the same absolute paths in all build workers and should be immutable; Redis messages contain IDs, never file contents.

## Task design and delivery guarantees

- **One image per build task:** load a file, compute `PHash.encode_image()` in a prefork child, and persist its hash/status. SQL connections are released during image processing. The API and comparison workers do not import the heavy image/ML stack.
- **One page per comparison task:** load ready unscored hashes, compute `(query ^ candidate).bit_count()`, and atomically save top-K results, completion ledger, progress, revision and the next task's outbox record. The integer operation is the same 64-bit Hamming metric used by imagededup. There is no nested multiprocessing or task per image pair.
- **Dependency waits:** if the query or candidates are still building, commit an unfinished outcome and publish a continuation with a short countdown. This releases the worker process. Ready pages continue immediately; no task blocks waiting on another task's result. Waiting requests query readiness once per configured interval, a deliberate simple polling tradeoff.
- **SQL revisions:** every message includes the source ID and revision. Conditional final updates reject stale or duplicate deliveries. The application-owned `ScoredCandidate` ledger includes nonmatches and candidates evicted from top-K. No candidate cursor is used, so a late build with a lower ID is still scored.
- **Outbox publication:** SQL commits precede Redis sends. Messages are deleted only after publication; a crash between send and deletion may duplicate delivery. Revision checks make duplicate final writes harmless. The dispatcher reuses one broker producer per batch and holds no SQL transaction during network I/O.
- **Failure handling:** transient SQL operational errors receive up to five retries with exponential backoff and jitter. Corrupt/missing images and exhausted task errors are recorded as failed against the matching revision. A failed query fails its comparison; failed candidates do not prevent other comparisons from finishing. If SQL itself remains unavailable, recording the terminal error can also fail; consult worker logs and resubmit after recovery.

Celery uses JSON messages, late acknowledgments, prefetch multiplier 1, and redelivery on worker loss. Its 3,600-second Redis visibility timeout exceeds the 120-second hard task limit; the soft limit is 110 seconds. Hard crashes can cause repeated redelivery, so repeatedly crashing inputs require operator intervention. Redis persistence configuration controls broker durability; deleting Redis data after publication is not recoverable from already-deleted outbox rows.

New imports are considered while a comparison remains unfinished. A finished request is not a live subscription; submit a new request for later imports or repaired images. Results and ledger rows are retained. Each request remains O(N) comparisons and requesting all images remains O(N²).

These choices follow Celery's guidance on [task granularity, idempotence and database transactions](https://docs.celeryq.dev/en/stable/userguide/tasks.html), [prefetch and queue separation](https://docs.celeryq.dev/en/stable/userguide/optimizing.html), and [Redis visibility timeouts](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html).

## Configuration and tests

| Environment variable | Default |
|---|---|
| `IMAGE_DATABASE_URL` | SQLite `example.db` in the current directory |
| `IMAGE_BROKER_URL` | `redis://127.0.0.1:6379/0` |
| `IMAGE_COMPARISON_PAGE_SIZE` | 250 candidates |
| `IMAGE_DEPENDENCY_WAIT_SECONDS` | 1 second |

Redis broker keys use the `imagededup:` prefix. SQLAlchemy engines are created lazily per process and cleared at prefork lifecycle boundaries. SQLite is the default local database; another SQLAlchemy URL needs its corresponding database driver and deployment testing.

```sh
poetry run python -m unittest discover -s tests -v
poetry run mypy --strict src
```

Automated tests use tiny local images and mocked broker publication; they do not require downloading a dataset. A separate functional check was also run against real Redis, Celery prefork workers, Beat and HTTP. Dataset selection and performance measurements are deferred.
