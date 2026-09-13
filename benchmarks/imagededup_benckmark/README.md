# imagededup_benckmark

This independent Poetry project owns the image dataset downloader, benchmark preparation, sequential runner and structured JSON results. Both image examples remain API applications.

## Setup and first run

Install the framework/example Poetry environments first, following their READMEs. The runner uses each example's own `.venv/bin/python`, so each application runs with its declared dependencies. Redis/Celery also requires a local `redis-server` executable. No already-running API or Redis instance is used or stopped.

From the repository root:

```sh
cd benchmarks/imagededup_benckmark
poetry env use python3.12
poetry install
poetry run imagededup-benchmark-download --limit 100
poetry run imagededup-benchmark --images 100 --repetitions 1 --output results/validation_100.json
```

Alternatively, add `--download` to the benchmark command to download/check the selected images before any application stack starts. The default dataset location is `data/mirflickr25k` under this project, ignored by Git. Images are extracted from the [official MIRFLICKR-25K archive](https://press.liacs.nl/mirflickr/mirdownload.html) with HTTP ranges and ZIP CRC checks; the full archive is not stored. Dataset licensing and attribution remain applicable.

The runner selects precisely `im1.jpg` through `imN.jpg`, creates a deterministic numbered input directory using hard links where possible, and records every input's size and SHA-256. Both apps receive those exact files in the same import order. No images are synthesized or transformed by this benchmark.

## Workloads

Each stack has **four build workers and four comparison workers**. DBWorker starts its API and worker service as independent sibling processes; monitoring includes both roots and all handler children, and shutdown stops both services. The Redis/Celery comparison pool also handles the application's lightweight outbox-dispatch task. Both apps use imagededup PHash for builds and integer XOR/popcount for Hamming distance. Scientific libraries are limited to one thread per process, avoiding nested CPU parallelism.

For each backend and repetition:

1. **Warm-up:** eight real images and their comparisons, excluded from timing. This exercises image-library imports and process initialization.
2. **Build-only:** import N images into a fresh workspace and wait for all hashes.
3. **Comparison-only:** on those built images, submit one request per image and wait for all N × (N−1) directed comparisons.
4. **Mixed:** import N images into another fresh workspace and immediately submit one comparison per image while building proceeds.
5. Validate completed statuses, hashes, every top-K result and each completion-ledger count outside the timed interval. Stop the entire stack before starting another.

The two backends never run concurrently. Every repetition gets new processes and a new SQLite database for each backend. Backend order alternates between repetitions to reduce a consistent order bias. Workspace creation, downloading, input preparation, warm-up, startup, correctness verification and shutdown are outside scenario timings.

Timing starts before image import or comparison submission and ends when committed application data shows completion. JSON also separates submission time from the remaining completion time. HTTP submissions are sequential and identical for both apps. This measures the complete example applications—including their API enqueue paths, SQL writes, coordination and broker overhead—not a synthetic broker-only workload.

Mixed runs record both whether requests were submitted before builds finished and whether scoring was observed before all builds finished. Small, fast workloads can finish building before scoring begins; that outcome is reported rather than artificially delaying a worker. Use more images when examining sustained overlap.

## Isolation and measurements

The runner starts an isolated Redis instance on an unused local port with AOF enabled and `appendfsync=everysec`. Its database, AOF files, Beat schedule and process logs live in the run's temporary working directory. SQLite uses each example's existing default settings. Redis AOF and SQLite commits are different durability boundaries; the results are not an equal-durability broker comparison.

A monitor samples shared SQL application tables with a read-only aggregate query every 0.1 seconds by default. It never changes worker state or scans the completion ledger during timing. This avoids repeatedly issuing N artifact-status requests merely to detect completion. A separate small HTTP probe requests one artifact approximately every 0.25 seconds. Probe frequency and counts are recorded; this monitoring has a small cost for both apps.

Process-tree measurements include the API and every owned child, plus Redis, Celery worker parents and Beat for the Celery stack:

- Wall time, submission time, images/second and comparisons/second.
- CPU seconds, CPU by process role, and average occupied CPU cores.
- Peak sampled sum of RSS, which can double-count shared memory.
- HTTP submission and probe latency counts, median, p95 and maximum.
- Build/scoring progress, first-scored time, mixed-overlap observations and SQL-busy probe count.
- SQL file size, input fingerprints and host/configuration metadata.

Completion observation is quantized by the sampling interval. Very short runs and sparse latency samples should be treated as validation, not stable performance estimates. Default runs use three repetitions; the initial 100-image validation uses one. Final full top-K validation also performs O(N²) distance calculations, outside measured time.

## Larger or customized runs

```sh
poetry run imagededup-benchmark --download --images 1000 --repetitions 3 \
  --timeout-seconds 3600 --output results/1000_images.json
```

Other options:

| Option | Default / meaning |
|---|---|
| `--backend` | `both`; select `dbwork` or `redis_celery` to run only one application |
| `--images` | 100; range 2–25,000 |
| `--repetitions` | 3 |
| `--warmup-images` | 8; use 0 for a cold-process run |
| `--page-size` | 250, applied to both apps |
| `--top-k` | 10 |
| `--max-distance` | 10 |
| `--dataset-dir` | This project's `data/mirflickr25k` |
| `--poll-interval` | 0.1 seconds |
| `--timeout-seconds` | 300 per scenario |
| `--output` | Timestamped file in `results/` |
| `--work-dir` | A new temporary directory; explicit paths must not exist |
| `--dbwork-python`, `--celery-python` | Override the respective example interpreter |
| `--redis-server` | `redis-server` on PATH |

At 25,000 images, each comparison/mixed case performs 624,975,000 directed comparisons and stores that many ledger rows. Storage and run time grow substantially; the 100-image validation does not establish full-dataset throughput. Input count and repetition count are explicit in every JSON report.

## JSON output

Reports use `schema_version: 1`. Top-level fields include `status`, `configuration`, `host`, `dataset`, `stacks`, `runs` and `summary`. Each run identifies its backend, repetition and scenario and contains `metrics` and `validation` objects. Normalized result digests must match between backends and repetitions.

Results are written atomically after every completed scenario. A failure preserves completed runs and an `error` object, shuts down owned processes, and exits nonzero. Logs and private databases remain in the recorded `work_directory` for diagnosis. JSON results can be kept in Git; images and runtime databases are not included.

The `historical/` report documents an earlier exploratory smoke test and is not a result of this runner or the aligned four-worker configuration.

```sh
poetry run python -m unittest discover -s tests -v
poetry run mypy --strict src
```

Run the separated DBWorker services without starting Celery or Redis:

```sh
poetry run imagededup-benchmark --backend dbwork --images 1000 --repetitions 1 --output results/separated_dbworker_1000.json
```
