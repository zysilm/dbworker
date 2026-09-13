# MIRFLICKR API smoke test

Measured on 2026-09-13 on the development machine, using Python 3.12.13, imagededup 0.3.3.post2, SQLAlchemy 2.0.52, SQLite, and two child processes for each worker. These are individual smoke runs, not statistically controlled benchmark estimates or measurements of the full 25K dataset.

## Data and correctness

The example's download command fetched MIRFLICKR images `im1.jpg` through `im100.jpg` from the official archive. Images remain outside the repository in `~/.cache/dbworker/mirflickr25k`.

The HTTP test imported 50 images, submitted a comparison before building had finished, then imported the other 50 and three controlled variants of `im1.jpg`: an exact file copy, a half-size JPEG, and a JPEG re-encoding at quality 65. All 103 hashes finished. The initial request scored all 102 candidates and returned the three variants at Hamming distance zero.

A second stage submitted one comparison per image: 103 requests, 10,506 directed comparisons. Every stored top-K result matched a separate exhaustive calculation using `PHash.hamming_distance()`, including the threshold and deterministic tie ordering. The completion ledger contained exactly 10,506 rows for these requests, including nonmatches. Image retrieval through the API succeeded.

With page size 20, the API exposed partial counts before final completion. At the default page size 250, each request fit within one page, so this run did not observe partial counts. Automated tests additionally cover late lower-ID candidates, top-K eviction, nonmatches, failed query/candidate builds, stale claims, transactional rollback and process execution.

## Timings

All requests went over local HTTP to a real Uvicorn process. The two runs were sequential. Warm build time includes image import and polling until all builds finish. All-pairs time includes creating the requests and polling their completion. Cold time includes initial child-process imports and the first comparison, but excludes downloading the dataset and starting Uvicorn.

| Measurement | Default: 250 candidates/page | Progress test: 20 candidates/page |
|---|---:|---:|
| Cold build of 103 images + first comparison | 8.68 s | 10.66 s |
| Warm build of another 100 images | 1.17 s | 1.70 s |
| Warm 103 requests / 10,506 comparisons | 3.32 s | 3.84 s |
| Overall HTTP latency, median | 0.84 ms | 1.10 ms |
| Overall HTTP latency, p95 | 2.49 ms | 4.10 ms |
| Overall HTTP latency, maximum | 121.70 ms | 58.87 ms |
| Peak sampled sum of process RSS | 832 MiB | 864 MiB |

RSS includes the API and its children and can double-count shared memory. Imagededup imports its broader scientific/ML dependencies even for PHash; the first request is substantially slower than warmed processing. HTTP latency includes both reads and writes, with 1,562 and 2,058 requests respectively.

The default-page run used an unchanged copy of the source in `/tmp` after a preceding attempt stalled before server startup while accessing the repository. The stalled attempt is excluded. The temporary scripts and raw reports are in `/tmp/dbworker-image-battle*` on the development machine; they are not required to run the example.

The results support using this as a responsive small-scale example. PHash comparisons are cheap, so task coordination, database writes and HTTP polling contribute substantially to these timings. These measurements do not establish 25K all-pairs throughput: 25,000 independent queries would perform 624,975,000 directed comparisons and retain a correspondingly large completion ledger.
