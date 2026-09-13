# imagededup_system_dbwork

A FastAPI image duplicate finder using [imagededup](https://github.com/idealo/imagededup) and DBWorker. Each image gets a 64-bit perceptual hash. Comparisons scan candidates in pages, keeping top-K matches and a separate completion ledger.

This example is an API application. Dataset downloading, test-data preparation and performance runs belong to [the benchmark project](../../benchmarks/imagededup_benckmark/README.md).

## Install and run

Use Python 3.12, which has a prebuilt wheel for imagededup 0.3.3.post2. Its dependencies include PyTorch even though this example uses PHash and downloads no model weights.

```sh
cd examples/imagededup_system_dbwork
poetry env use python3.12
poetry install
export DBWORKER_DATABASE_URL="sqlite:////absolute/path/to/example.db"
poetry run imagededup-system-dbwork-api
```

In a second terminal, use the same project directory and database URL:

```sh
export DBWORKER_DATABASE_URL="sqlite:////absolute/path/to/example.db"
poetry run imagededup-system-dbwork-workers
```

The API and worker service are independent processes. The API performs CRUD and reads status; the worker service runs the coordinator and its handler child processes. Importing worker definitions starts no processing. Requests remain pending when the worker service is stopped and can be discovered after it starts. Ctrl+C or SIGTERM stops the worker service gracefully, draining active handlers and renewing their leases during shutdown.

For a new local database, start the API first so schema creation completes before starting workers. Each process has its own engine/session factory. Use the same absolute SQLite path even if the processes run from different directories. Both services must also access images at the stored absolute paths. These two commands can be assigned to separate containers later with a shared database and accessible image storage; no Docker setup is required for this example.

Swagger UI: http://127.0.0.1:8001/docs

## API

Supply existing image files accessible to the API and worker processes. Use the returned IDs in subsequent calls:

```sh
curl -s http://127.0.0.1:8001/workspaces \
  -H 'Content-Type: application/json' -d '{"name":"photos"}'

curl -s http://127.0.0.1:8001/workspaces/1/imports \
  -H 'Content-Type: application/json' \
  -d '{"directory":"/absolute/path/to/images","limit":100}'

curl -s http://127.0.0.1:8001/comparisons/1 \
  -H 'Content-Type: application/json' \
  -d '{"retained_max_k":10,"max_distance":10}'

curl -s http://127.0.0.1:8001/comparisons/1
curl -s http://127.0.0.1:8001/comparisons/1/results
```

Import returns `imported_images` and `artifact_ids`; hash building starts automatically. A comparison can be submitted before its query hash is built. Reimporting a file path into the same workspace skips it. Files should be immutable and must remain accessible at their original absolute paths. This local-directory API is intended for trusted local use.

| Endpoint | Behavior |
|---|---|
| `POST /workspaces` | Create a workspace from `name`. |
| `POST /workspaces/{id}/imports` | Import existing images; optionally limit the count. |
| `GET /workspaces/{id}/artifacts?after_id=0&limit=100` | List artifacts with execution status. |
| `GET /artifacts/{id}` | Inspect a build's status/error. |
| `GET /artifacts/{id}/image` | View an original image, including result candidates. |
| `POST /workspaces/{id}/build-all` | Reset failed builds after fixing their input files. |
| `POST /comparisons/{artifact_id}` | Submit a comparison. |
| `GET /comparisons/{id}` | Read status, `candidates_scored_count` and errors. |
| `GET /comparisons/{id}/results` | Read current top-K matches, including partial results. |

Hamming distance ranges from 0 to 64; lower is closer. Zero means identical hashes, not necessarily identical files. Results are ordered by distance, then artifact ID. The default threshold is 10; `max_distance=64` includes nearest neighbors even when they are not plausible duplicates.

## Worker behavior

`workers.py` declares two ordinary decorated handlers. `main_fastapi.py` serves the API; `main_worker_service.py` starts and stops the coordinator. Their application logic lives in `domain/artifact_build.py` and `domain/comparison.py`.

1. The build worker copies the image path, releases the read transaction, computes `PHash.encode_image()` in a child process, then commits the hash and `Finished()` outcome together.
2. The comparison worker copies a page of ready, unscored hashes and releases its read transaction. It computes integer XOR/popcount, the same 64-bit Hamming metric used by imagededup and the Redis/Celery example. Comparison processes do not load the image/ML stack.
3. A final transaction writes top-K matches, `ScoredCandidate` ledger rows for all processed candidates, progress, and `Finished()` or `Unfinished()`.
4. Dependency checks use `coordinator.has_execution_status()`. Unfinished builds keep comparisons open; failed candidates are skipped, and a failed query fails its comparison.

Generated work-table names are not used by the application. The separate application-owned ledger is necessary because top-K results omit other processed candidates. There is no candidate cursor; late builds with lower IDs are still processed.

New images are considered while a request remains unfinished. A finished request stays finished; submit a new one for later imports or repaired builds. Ledger rows are retained. Each request performs O(N) comparisons; requesting every image performs O(N²).

## Configuration and tests

Set the shared database URL in both commands; worker counts and page size configure the worker service:

| Variable | Default |
|---|---|
| `DBWORKER_DATABASE_URL` | SQLite `example.db` in the working directory |
| `DBWORKER_BUILD_WORKERS` | 2 child processes |
| `DBWORKER_COMPARISON_WORKERS` | 2 child processes |
| `DBWORKER_COMPARISON_PAGE_SIZE` | 250 candidates |

```sh
poetry run python -m unittest discover -s tests -v
poetry run mypy --strict src
```

Tests use small local fixtures and do not download a dataset.
