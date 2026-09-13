# Separate API and worker service: 1,000-image validation

DBWorker only; no Celery/Redis benchmark was launched. The runner started API and worker service as sibling processes. The coordinator and four build/four comparison child processes belong to the worker service; the API starts no processing. Resource measurements include both trees.

Original polling (0.25–2 seconds), SQLite DELETE mode, page size 250, top-K 10, maximum Hamming distance 10, eight-image warm-up and one repetition. Input images and their perceptual hashes are the same as the preceding 1,000-image run.

| Scenario | Previous combined process | Separate services |
|---|---:|---:|
| build | 5.067s | 4.254s |
| comparison | 40.332s | 37.050s |
| mixed | 47.705s | 45.687s |

All three cases passed, including 999,000 scored pairs for comparison and mixed work. Full validation outputs match the previous combined-service run. Timings show no observed regression; one repetition is not sufficient to claim a speed improvement.

Run the example in two terminals with the same DBWORKER_DATABASE_URL and image paths:

```sh
poetry run imagededup-system-dbwork-api
poetry run imagededup-system-dbwork-workers
```

Run this benchmark:

```sh
poetry run imagededup-benchmark --backend dbwork --images 1000 --repetitions 1 --output results/separated_dbworker_1000.json
```

The validation used `/tmp/dbworker-image-312/bin/python` through `--dbwork-python`; default runs use the example’s Poetry environment.

[Structured results](separated_dbworker_1000.json).
