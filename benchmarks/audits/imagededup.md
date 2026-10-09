# Image deduplication benchmark audit

> Historical pre-migration audit. References to the current implementation below
> describe the implementation inspected at audit time, not the repaired native
> benchmark. See [the audit scope](README.md) and
> [the current benchmark contract](../README.md) for status and validation limits.

## Verdict

The image suite already runs the native Celery application maintained in this repository. It does not use the shared Celery wrapper used by the external-project suites. The two applications submit equal image and comparison request counts and verify equal business results. There is no evidence of a reduction from hundreds of Celery business tasks to a handful of DBWorker business tasks.

However, exact delivered-task and executed-handler parity is not measured. The mixed scenario has different dependency scheduling, and operational failure semantics are different. The current results establish successful business-work parity, not complete queue lifecycle equivalence. This audit is a static inspection of source and existing JSON; no new performance experiment was run.

## Native baseline and source ownership

- `benchmarks/imagededup_benckmark/src/imagededup_benckmark/runtime.py:125` launches `imagededup_system_redis_celery.celery_app:app`, with its existing build, comparison, and dispatcher tasks. This is the actual image example application, rather than `benchmarks.upstream.celery_app`.
- `examples/imagededup_system_redis_celery/src/imagededup_system_redis_celery/tasks.py:39` defines native `images.build`; line 44 defines `images.compare`; line 54 defines `images.dispatch`.
- `examples/imagededup_system_redis_celery/src/imagededup_system_redis_celery/celery_app.py:11` retains JSON serialization, late acknowledgement, worker-loss rejection, prefetch multiplier 1, queue routing, limits, and Beat configuration. These are example application configuration, not an independently maintained third-party baseline.
- `examples/imagededup_system_dbwork/src/imagededup_system_dbwork/workers.py:20` registers an individual artifact handler, and line 29 registers an individual comparison handler. No handler batches the entire input dataset.
- The native baseline is a repository-owned Celery example using imagededup's hash library. It should not be described as an independently maintained upstream imagededup Celery deployment: the application, SQL schema, paging, and Celery integration are ours.

## Counts derived from execution code

`benchmarks/registry.json:17` selects 1,000 images, eight warm-up images, and five repetitions. `benchmarks/imagededup_benckmark/suite.py:30` forwards these values to the actual runner. The default page size is 250, top-K is 10, and maximum Hamming distance is 10 (`benchmarks/imagededup_benckmark/src/imagededup_benckmark/run.py:158`).

For each backend in each repetition:

| Scenario | New image build jobs | Initial comparison requests | Required scored pairs | Minimum nonempty scoring invocations |
| --- | ---: | ---: | ---: | ---: |
| Build | 1,000 | 0 | 0 | 0 |
| Comparison | 0; reuse completed build workspace | 1,000 | 999,000 | 4,000 |
| Mixed | 1,000 | 1,000 | 999,000 | 4,000 |
| Warm-up, untimed | 8 | 8 | 56 | 8 |

The runner imports exactly the selected image count and checks the returned artifact count (`run.py:111`). For comparison and mixed scenarios it posts one comparison request for every artifact (`run.py:117`). The comparison scenario reuses the build workspace (`run.py:243`), while mixed uses a fresh workspace. Both backends see the same loop and inputs.

On the Celery side, import creates one outbox `images.build` message per artifact (`examples/imagededup_system_redis_celery/src/imagededup_system_redis_celery/api/routes.py:102`), and comparison creation creates one initial `images.compare` message (`api/routes.py:170`). On the DBWorker side, import creates one eligible `FeatureArtifact` row per image (`examples/imagededup_system_dbwork/src/imagededup_system_dbwork/api/routes.py:101`), and comparison creation creates one `ComparisonRequest` row (`api/routes.py:163`).

Both implementations limit an individual scoring invocation to 250 ready, unscored candidates: Celery `domain/comparison.py:47` and DBWorker `domain/comparison.py:49` in their respective example packages. Each request has 999 candidate images, so scoring a completed workspace requires `ceil(999 / 250) = 4` nonempty invocations per request. In a healthy comparison-only run, each backend therefore needs 4,000 nonempty page executions. This does not establish 4,000 actual Celery deliveries: duplicates, retries, stale deliveries, and control tasks are not counted. Mixed execution can process smaller pages as hashes become ready and can require more nonempty page executions.

Across five repetitions, each backend must complete 10,000 timed image builds, 10,000 timed comparison requests, and 9,990,000 timed scored pairs. Warm-up adds 40 builds, 40 comparisons, and 280 pairs per backend, outside timing.

## Business computation and output parity

- Both applications run `PHash(verbose=False).encode_image(image_file=file_path)` per image: DBWorker `domain/artifact_build.py:20`, Celery `domain/artifact_build.py:27`.
- Both applications compute the same integer XOR/popcount Hamming kernel: DBWorker `domain/comparison.py:56`, Celery `domain/comparison.py:54`.
- Both persist a scored-candidate ledger and sorted, threshold-filtered top-K results per page: DBWorker `domain/comparison.py:62`, Celery `domain/comparison.py:66`.
- Input paths are ordered consistently, and exact bytes are linked or copied into the shared input manifest (`run.py:43`).
- Untimed validation checks every completed hash, API statuses, each comparison's scored count, exact expected top-K, and ledger count (`run.py:58`). It normalizes artifact IDs before hashing results and compares the full validation object across backends and repetitions (`run.py:253`). These are substantive business-output checks.
- The oracle recomputes Hamming top-K from each backend's produced hashes and compares hash digests between backends. It does not independently recompute PHash for all images, so it proves mutual equality rather than an independent PHash implementation reference.

## Timing and resources

- Each repetition starts new stacks and SQL databases (`run.py:227`). Backend order alternates by repetition (`run.py:228`). Services do not overlap between backends; shutdown drains or terminates process groups (`runtime.py:144`).
- Timed work begins before import and comparison API submission (`run.py:105`). Workspace creation, stack startup, input preparation, warm-up, validation, and shutdown are excluded. Comparison-only timing excludes the prerequisite image builds.
- Completion polling reads the same business tables, with the same interval, on both applications (`benchmarks/imagededup_benckmark/src/imagededup_benckmark/measurement.py:92`). It observes completed hashes and scored counts rather than waiting for all background control messages or worker acknowledgements to drain (`measurement.py:117`). Terminal API statuses are checked afterward, outside timing (`run.py:65`, `run.py:75`).
- Four build processes and four comparison processes are configured on each side (`runtime.py:95`, `runtime.py:127`). Celery's dispatcher shares the comparison worker pool. This is equal configured business concurrency, not equal total process count.
- Scientific libraries receive the same single-thread limits (`runtime.py:79`). CPU/RSS measurement includes all backend process trees, including Redis and Beat on the Celery side (`measurement.py:49`). Extra service overhead is included rather than hidden.
- Both business databases use SQLite with the same default SQLAlchemy engine configuration. Redis separately uses AOF with `appendfsync everysec` (`runtime.py:105`). These persistence boundaries are different; the experiment does not prove equal crash durability.
- The image Celery README documents two processes per queue, whereas the harness configures four for both implementations (`examples/imagededup_system_redis_celery/README.md:38`, `runtime.py:127`). This is a matched experiment resource choice, not unmodified README deployment capacity.

## Fairness gaps and unsupported claims

### Actual task counts and mixed granularity are unverified

Celery executes an initial comparison even if its query hash is not ready, then schedules a countdown continuation (`examples/imagededup_system_redis_celery/src/imagededup_system_redis_celery/domain/comparison.py:47`, `domain/comparison.py:90`). DBWorker's SQL eligibility filters out requests without ready work (`examples/imagededup_system_dbwork/src/imagededup_system_dbwork/domain/comparison.py:81`). Celery also runs a dispatcher every second (`celery_app.py:23`); DBWorker performs coordinator polling rather than equivalent dispatcher task executions.

This is a genuine architecture distinction, not evidence of reduced business work. It nevertheless means that equal logical request counts do not imply equal total handler invocations or broker messages. The current JSON does not count page executions, empty dependency invocations, dispatcher executions, retries, duplicate deliveries, or per-request processed page sizes. Strict execution-count parity cannot be certified from current results. In mixed workloads, readiness timing can change page occupancy and therefore page counts on each backend.

### Failure lifecycle parity is absent

Celery's native task base automatically retries SQL `OperationalError` up to five times with backoff and jitter (`tasks.py:19`). DBWorker marks a failed handler as failed (`src/dbworker.py:400`) and has no equivalent automatic retry policy in the image variation. Celery also applies soft/hard task limits (`celery_app.py:17`), while the DBWorker image handler does not implement equivalent execution deadlines. Successful runs do not validate these differences. A full parity claim needs either a DBWorker variation implementing equivalent behavior or an explicitly narrower successful-work contract; it must not silently omit the native behavior.

### Business completion differs from queue drain

Timing stops when persisted business counts match (`measurement.py:117`). Dispatcher messages, duplicate deliveries, or acknowledgement work may remain. The build workspace is reused for the next scenario, and stacks continue across build, comparison, and mixed (`run.py:241`). Without per-scenario queue/handler counters and an untimed quiescence barrier, residual control work can spill into the following scenario. The audit did not observe whether this occurred in previous runs.

### Optimum Celery performance is not established

The baseline retains its native configuration and employs connection reuse in outbox publication (`examples/imagededup_system_redis_celery/src/imagededup_system_redis_celery/outbox.py:38`). That is better evidence than a synthetic wrapper, but source inspection cannot certify that the application configuration is optimal. Describe the result as this repository's native example under recorded settings, not the fastest achievable Celery implementation or a representative upstream production deployment.

## Concrete follow-up steps

1. Keep launching the image example's existing Celery app and native tasks. No migration to the shared upstream Celery wrapper is appropriate. Preserve task bodies, paging, outbox publication, and native lifecycle behavior.
2. Emit a parity manifest for every scenario: ordered image input digests, initial build IDs, comparison request IDs, candidates per request, page size, top-K, threshold, and business worker concurrency. Fail publication unless both sides match the logical workload exactly.
3. Add observation-only instrumentation for actual native task lifecycle and DBWorker handler lifecycle. Record logical submissions separately from deliveries, successful page executions, empty dependency executions, retries, stale executions, dispatcher tasks, and page occupancy. Do not substitute counting wrappers as Celery tasks.
4. Require one DBWorker page execution to process no more candidates than one native Celery comparison task. Never replace 1,000 requests or 4,000 page computations with one dataset-wide handler. The current implementation already respects the maximum page size; instrument mixed execution to establish the actual distribution.
5. Add an untimed idle/quiescence check between scenarios, while retaining the existing common business-completion timing boundary. Report queue drain separately if measured. Keep startup and warm-up outside the performance interval on both sides.
6. Implement and verify corresponding retry/deadline behavior in the DBWorker variation if comparing complete lifecycle semantics. Keep native Celery policies intact. Until then, mark retry, timeout, crash recovery, and broker/database outage parity as unverified.
7. Report dependency and dispatcher differences explicitly. Matching business jobs and stages does not require manufacturing redundant DBWorker polling handlers to reproduce Celery's empty dependency or dispatcher invocations. SQL eligibility is a legitimate architectural difference. Keep those control counts separate from the shared build and scoring work; verify that DBWorker does not merge business jobs, skip candidates, increase the page limit, or omit required stages.
8. Regenerate performance results only after workload and lifecycle counters pass admission checks. Preserve prior JSON as historical successful-business-work results rather than reinterpreting them as new parity-certified measurements.
