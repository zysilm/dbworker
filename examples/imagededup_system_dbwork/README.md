# imagededup_system_dbwork

A FastAPI image duplicate finder using [imagededup](https://github.com/idealo/imagededup) and DBWorker. Each image gets a 64-bit perceptual hash (PHash). Comparison requests scan other images in the workspace in pages, retaining the closest top-K matches within a Hamming-distance threshold. Lower distance is better; zero means identical hashes, not necessarily identical files.

## Install and download real images

Use Python 3.12: imagededup 0.3.3.post2 provides prebuilt wheels for that version. Its dependencies include PyTorch/torchvision even though this example uses PHash, not its CNN model. No model weights are downloaded.

```sh
cd examples/imagededup_system_dbwork
poetry env use python3.12
poetry install
poetry run imagededup-system-dbwork-download --limit 100
poetry run imagededup-system-dbwork-api
```

The backend's download command reads the official [MIRFLICKR-25K archive](https://press.liacs.nl/mirflickr/mirdownload.html) using HTTP byte ranges. It extracts the first N numbered real images, validating downloaded ZIP entries with their CRCs. It stores images in `~/.cache/dbworker/mirflickr25k`, outside the repository, without retaining the 3.1 GB archive. Completed files are reused when rerunning the command. Interrupted files are not imported as images.

To download all 25,000 images:

```sh
poetry run imagededup-system-dbwork-download
```

Allow roughly 3 GB for the extracted dataset, plus database growth. `--directory /path/to/images` changes the destination. The download is an explicit backend CLI operation, not an automatic API-startup operation. MIRFLICKR is real Flickr photography; consult the [dataset's licensing and attribution information](https://press.liacs.nl/mirflickr/) before redistributing images. This repository contains no dataset images.

## Use the API

Swagger UI: http://127.0.0.1:8001/docs

Create a workspace, then import downloaded images. Use the returned IDs in subsequent requests:

```sh
curl -s http://127.0.0.1:8001/workspaces \
  -H 'Content-Type: application/json' -d '{"name":"mirflickr"}'

curl -s http://127.0.0.1:8001/workspaces/1/imports \
  -H 'Content-Type: application/json' \
  -d '{"directory":"~/.cache/dbworker/mirflickr25k","limit":100}'

curl -s http://127.0.0.1:8001/comparisons/1 \
  -H 'Content-Type: application/json' \
  -d '{"retained_max_k":10,"max_distance":10}'

curl -s http://127.0.0.1:8001/comparisons/1
curl -s http://127.0.0.1:8001/comparisons/1/results
```

Import returns `imported_images` and `artifact_ids`. Hash building starts automatically; comparison requests can be submitted before hashes are ready. Reimporting existing file paths into the same workspace skips them. Imported files must remain accessible at their original absolute paths and should be treated as immutable. Importing directories is intended for a local example server.

- `GET /workspaces/{id}/artifacts?after_id=0&limit=100`: list artifact IDs, image IDs and execution status.
- `GET /artifacts/{id}`: inspect one build's status/error.
- `GET /artifacts/{id}/image`: view the source image, including result candidates.
- `GET /comparisons/{id}`: execution status, number of scored candidates, and errors.
- `GET /comparisons/{id}/results`: current top-K matches, ordered by distance then artifact ID; available while processing continues.
- `POST /workspaces/{id}/build-all`: reset failed builds after fixing their input files.

Distance ranges from 0 to 64. The default duplicate threshold is 10, matching imagededup's default; `max_distance=64` returns nearest neighbors even when they are not plausible duplicates. MIRFLICKR does not provide duplicate-pair ground truth. Controlled copies or re-encodings of real images are useful for correctness checks; natural nearest neighbors need visual review.

## How the workers cooperate

`main.py` declares two ordinary decorated handlers. Their application logic lives in `domain/artifact_build.py` and `domain/comparison.py`.

1. The build worker reads the image path, releases its read session with `rollback()`, and computes `PHash.encode_image()` in a child process. The final hash and `Finished()` outcome commit together.
2. The comparison worker waits for the query hash, then reads one page of ready, unscored candidate hashes. It releases the connection before computing `PHash.hamming_distance()` for each candidate.
3. One final transaction writes top-K matches, every page's `ScoredCandidate` completion rows, the progress count, and `Finished()` or `Unfinished()`.
4. Eligibility and completion checks use `coordinator.has_execution_status()` for the build worker. Unfinished builds keep comparisons open; failed candidate builds are skipped. A failed query build causes its comparison to fail rather than wait forever.

No generated work-table names appear in the application. The application owns the completion ledger because top-K results omit other processed candidates. It records nonmatches too, so they are not repeatedly scored. No candidate cursor is used.

A request includes new images that become visible while it remains unfinished. Once finished, it stays finished; submit a new request to include later imports or repaired builds. Completion is not a permanently live subscription. Ledger rows are currently retained for inspection and grow with the number of requests and candidates.

Each request performs O(N) comparisons; requesting every image performs O(N²). The example deliberately uses paged exhaustive comparisons, not imagededup's optional search indexes or nested process pools.

## Configuration and tests

Environment variables, set before starting the API:

| Variable | Default |
|---|---|
| `DBWORKER_DATABASE_URL` | SQLite `example.db` in the working directory |
| `DBWORKER_BUILD_WORKERS` | 2 child processes |
| `DBWORKER_COMPARISON_WORKERS` | 2 child processes |
| `DBWORKER_COMPARISON_PAGE_SIZE` | 250 candidates |

Use a fresh database for this replacement example. The framework keeps its existing claiming and transaction implementation.

```sh
poetry run python -m unittest discover -s tests -v
poetry run mypy --strict src
```

The tests use small local image fixtures and mocked HTTP responses; they do not download MIRFLICKR.
