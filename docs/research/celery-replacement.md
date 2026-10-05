# DBWorker replacement scope for Celery and Redis

Research date: 2026-10-04. Local baseline: `9e3e63f58b589b0502e5dd6f914a80f036de44ec`, DBWorker 0.0.6. This is a source review and benchmark proposal, not a production migration or a performance result.

## Conclusion

DBWorker is a credible alternative for durable, database-owned background work: artifact generation, exports, incremental processing, and SQL-defined dependencies. Its strongest property is that application writes through the supplied session and execution completion commit together, without a separate publication step.

It is not a drop-in Celery replacement. Celery schedules invocations; DBWorker records one execution state per `(worker name, source primary key)`. Repeated business events, revisions, scheduled occurrences, and task arguments must therefore have explicit representations when they cannot be reduced to that identity.

Most missing Celery features are implementable, rather than fundamentally impossible. Scheduling, retries, workflow joins, cancellation, and monitoring require additional application code or runtime changes. The boundaries that cannot be removed while preserving the existing model are accepting work without durable SQL representation, executing independently of the coordination database, and preserving independent invocation histories using just one worker/source state. Exactly-once effects against an arbitrary external service are unavailable to both systems without cooperation from that service.

## Method and project selection

GitHub repository metadata and commit snapshots were fetched directly from the GitHub API. Source files were fetched from `raw.githubusercontent.com` at those commits. Stars identify substantial projects, not the prevalence of a feature or production throughput. Current branches can contain unreleased code. Selected task paths establish usage patterns; they are not an exhaustive inventory of every task in each project.

The companion [source manifest](celery-replacement-sources.json) records repository metadata, pinned commits, fetched file URLs, and SHA-256 fingerprints. It includes supporting files fetched during investigation; fetching a file does not imply exhaustive review of every line.

| Project | Observed stars | Reviewed commit | Role in this review |
|---|---:|---|---|
| [Apache Superset](https://github.com/apache/superset) | 75,030 | `68f19947a8012ac587bb407c469abacbebae61ca` | SQLAlchemy backend; reports, dependencies, cancellation |
| [Paperless-ngx](https://github.com/paperless-ngx/paperless-ngx) | 46,264 | `8adbff1423af58575bc5a08eee7a6d833fd95651` | Document ingestion, indexing, tracked tasks |
| [PostHog](https://github.com/PostHog/posthog) | 40,128 | `526d64dd82340b1bf4293d6d9baea7e965997048` | Redis-backed Celery, exports, repeated cohort calculations |
| [Saleor](https://github.com/saleor/saleor) | 23,406 | `8385ca60ecefa3a068aed3d1a28188e096cdc543` | Webhooks, exports, email, scheduled maintenance |
| [Sentry](https://github.com/getsentry/sentry) | 45,254 | `94bf1b8aad4c21840829be1f362b096d12f782d0` (24.1.0) | Historical Celery comparison: outbox batching and digests |

Sentry's current snapshot, `a86c9bd36cf09c0c4ee890aeb5b0b5b5c96e367b`, uses `taskbroker_client` in its [task decorator](https://github.com/getsentry/sentry/blob/a86c9bd36cf09c0c4ee890aeb5b0b5b5c96e367b/src/sentry/tasks/base.py). It is not counted as a current Celery implementation. PostHog also has migration boundaries: its reviewed export task explicitly labels the Celery path a legacy fallback while Temporal handles activity retries. This review does not treat all PostHog background work as Celery work.

Broker choice is configurable in several projects. These examples establish Celery usage; they do not establish that every deployment uses Redis. PostHog's reviewed configuration explicitly assigns Redis as both broker and result backend. Our benchmark baseline remains Celery + Redis regardless of other projects' broker choices.

## What the projects actually use

### Apache Superset

The report scheduler evaluates cron windows and publishes individual executions with ETA and task timeout options. The generic task executor loads persistent task records, defers unmet DAG prerequisites with Celery retry, propagates prerequisite failure, records a Celery task ID, and maintains a heartbeat. These are concrete needs beyond running a function in another process. [Scheduler source](https://github.com/apache/superset/blob/68f19947a8012ac587bb407c469abacbebae61ca/superset/tasks/scheduler.py).

Superset also implements submission deduplication using application locks and database constraints, and has explicit cancellation and orphan recovery commands. Celery does not remove the need for the surrounding application state machine. [Submission](https://github.com/apache/superset/blob/68f19947a8012ac587bb407c469abacbebae61ca/superset/commands/tasks/submit.py), [cancellation](https://github.com/apache/superset/blob/68f19947a8012ac587bb407c469abacbebae61ca/superset/commands/tasks/cancel.py), [orphan recovery](https://github.com/apache/superset/blob/68f19947a8012ac587bb407c469abacbebae61ca/superset/commands/tasks/reap.py).

Replacement inference: persistent query/export request rows and SQL dependency gates are promising fits. Scheduled reports need distinct occurrence identity or a deliberately recurring state machine. Query cancellation, completion notifications, timeouts, and worker health still need implementations. The existing SQLAlchemy architecture makes adaptation more direct than in Django projects, but it does not make existing handlers transaction-compatible automatically.

### Saleor

Webhook delivery already has a durable delivery identity. Tasks generate deferred payloads, route deliveries to selected queues, create attempt records, perform external requests, and invoke retry handling. Observability work uses a Celery group. [Webhook transport](https://github.com/saleor/saleor/blob/8385ca60ecefa3a068aed3d1a28188e096cdc543/saleor/webhook/transport/asynchronous/transport.py).

CSV exports load `ExportFile` records and use task success/failure hooks to update job state and emit events. Email tasks instead accept recipient/payload/configuration arguments, illustrating a call-oriented interface. Periodic tasks and named queues appear in settings and product tasks. [Exports](https://github.com/saleor/saleor/blob/8385ca60ecefa3a068aed3d1a28188e096cdc543/saleor/csv/tasks.py), [email](https://github.com/saleor/saleor/blob/8385ca60ecefa3a068aed3d1a28188e096cdc543/saleor/plugins/user_email/tasks.py), [settings](https://github.com/saleor/saleor/blob/8385ca60ecefa3a068aed3d1a28188e096cdc543/saleor/settings.py), [product tasks](https://github.com/saleor/saleor/blob/8385ca60ecefa3a068aed3d1a28188e096cdc543/saleor/product/tasks.py).

Replacement inference: export requests and delivery rows fit the DBWorker identity well. Webhook retries need persisted attempt counts, deadlines, and jitter; a lost HTTP response creates an ambiguous external effect in either framework. A user/order row alone cannot represent multiple independent emails. Notification or delivery rows are required, and adding them must be counted as migration work. Saleor is Django-based: DBWorker currently requires SQLAlchemy models and sessions, so this is a workload comparison rather than a decorator swap.

### Paperless-ngx

`consume_file` runs an ingestion plugin sequence using a Celery request ID, progress management, temporary files, and cleanup. Index operations and AI suggestions configure retries for selected failures. Task lifecycle signals create and update application-owned `PaperlessTask` records; retries reuse the existing task ID. [Tasks](https://github.com/paperless-ngx/paperless-ngx/blob/8adbff1423af58575bc5a08eee7a6d833fd95651/src/documents/tasks.py), [lifecycle handlers](https://github.com/paperless-ngx/paperless-ngx/blob/8adbff1423af58575bc5a08eee7a6d833fd95651/src/documents/signals/handlers.py).

Replacement inference: ingestion request rows are a natural source because the final document may not exist when ingestion starts. OCR and artifact generation resemble our image workload, but filesystem writes, subprocesses, intermediate progress, and failure cleanup are additional semantics. Reindexing the same document after edits needs revision/request identity or a continuing dirty-state worker; marking the first indexing execution finished permanently would miss later updates. Django integration is again a real adaptation cost.

### PostHog

The reviewed configuration uses Redis for the broker and results. Worker setup publishes task lifecycle counters and duration metrics. [Celery settings](https://github.com/PostHog/posthog/blob/526d64dd82340b1bf4293d6d9baea7e965997048/posthog/settings/celery.py), [worker setup](https://github.com/PostHog/posthog/blob/526d64dd82340b1bf4293d6d9baea7e965997048/posthog/celery.py).

The export task consumes an `ExportedAsset` ID, uses a dedicated queue, late acknowledgment, soft/hard time limits, and retained Celery results for groups/chords. Its business operation records export errors itself; the wrapper catches exceptions. The same file documents its ongoing Temporal migration. [Export task](https://github.com/PostHog/posthog/blob/526d64dd82340b1bf4293d6d9baea7e965997048/posthog/tasks/exporter.py).

Cohort calculation accepts a pending version, rejects obsolete work, handles transient failures, and maintains calculation history and recovery bookkeeping. Long-running queues isolate population work. [Cohort tasks](https://github.com/PostHog/posthog/blob/526d64dd82340b1bf4293d6d9baea7e965997048/posthog/tasks/calculate_cohort.py).

Replacement inference: export request processing is a good fit, subject to explicit fan-in and timeout support. Repeated cohort calculation is a valuable counterexample to a simplistic row queue: a stable cohort ID does not identify a calculation version. ClickHouse or remote RPC writes also sit outside the SQL coordination transaction. Broker-level success and business success must be checked separately in both implementations.

### Sentry 24.1.0: historical comparison

The Celery task decorator adds duration/memory instrumentation and silo restrictions; its implementation deliberately avoids result-backend usage. Digest scheduling and delivery have different queues. Outbox scheduling partitions durable work into batches and dispatches drain tasks with ID ranges. [Task decorator](https://github.com/getsentry/sentry/blob/94bf1b8aad4c21840829be1f362b096d12f782d0/src/sentry/tasks/base.py), [digests](https://github.com/getsentry/sentry/blob/94bf1b8aad4c21840829be1f362b096d12f782d0/src/sentry/tasks/digests.py), [outbox draining](https://github.com/getsentry/sentry/blob/94bf1b8aad4c21840829be1f362b096d12f782d0/src/sentry/tasks/deliver_from_outbox.py).

Replacement inference: durable outbox rows can supply work directly, making enqueue indirection potentially removable. This does not remove cross-region delivery, per-shard ordering, batching, or isolation requirements. One-source-at-a-time claiming may be inferior to batch scheduling for tiny work items; that is a benchmark question, not a proven limitation.

## Capability comparison

The assessments below concern the checked-in `src/dbworker.py`, not an imagined future version. Celery capabilities depend on its broker, backend, pool, platform, and configuration.

| Requirement | DBWorker today | Replacement assessment |
|---|---|---|
| Run durable background work after API commit | Discovers committed eligible rows | Direct fit when a source row represents the request |
| CPU parallelism | Spawned process pool per registered worker | Direct fit; startup and memory costs need measurement |
| Save SQL results and completion atomically | One supplied session transaction | Strong fit within that transaction boundary |
| Track status and terminal failure | Four statuses, error string, lease | Basic fit; no traceback/attempt history or cancellation states |
| Resume paged work | `Unfinished()` plus application progress | Direct fit; restarts the function, not its instruction pointer |
| SQL-defined prerequisites | Eligibility predicates and status `EXISTS` | Good fit; failure propagation belongs to application logic |
| Multiple coordinators/hosts | Database claims and tokens coordinate ownership | Architecturally supported; distributed fault tolerance and scaling remain unproven |
| Recover after coordinator death | Unrenewed claims become reclaimable | Partial fit; distinguish this from child-only failure |
| Retry selected transient exceptions | Exceptions become `FAILED`; manual reset | Missing policy; app can catch, persist retry state, return `Unfinished()` |
| Retry delay, backoff, jitter, attempt limits | No per-item policy | Persist attempts and `next_attempt_at`, filter eligibility |
| Delayed work and expiration | No dedicated API | Source timestamps can gate claims; expiration state/cleanup needs app logic |
| Cron and recurring schedules | No scheduler | Possible with occurrence rows or a continuing schedule row; requires clock/cron/misfire semantics |
| Recompute after a finished source changes | Finished state excludes it | Requires a new request/revision row, or a worker kept unfinished by design |
| Independent repeated invocations and arguments | One state per source/worker | Must materialize invocations; cannot preserve them using the unchanged identity alone |
| `chain`, `group`, `chord`, callbacks | No Canvas API or result propagation | Persist dependencies/results and implement join/error semantics |
| Arbitrary return values / `AsyncResult` | Only `Finished` or `Unfinished` | Results must be application data; no compatible Celery result API |
| Per-task-type isolation | Separate fixed process pools | Basic isolation exists; no queue subscription/routing administration |
| Per-invocation routing, CPU/GPU pools | SQL eligibility can partition sources | Needs stable partition ownership/configuration; same worker name preserves shared state |
| Priority | Eligibility can order candidates | Basic next-selection policy; no preemption or strict completion ordering |
| Rate limiting | Concurrency only | Requires an actual rate policy; concurrency is not requests per second |
| Soft/hard time limits and process recycling | No runtime support | Requires process supervision/runtime changes |
| Cancel pending or running work | No public cancel/revoke API | Eligibility can skip pending work; active cancellation needs cooperation or process control |
| Health, events, inspect, Flower, autoscale | Logging and per-source state | Missing operations layer; external supervision alone is incomplete |
| Django task integration | SQLAlchemy model/session contract | Significant adaptation; ORM writes do not automatically share the managed transaction |
| Replace Redis cache/locks/pub-sub/streams | No Redis service interface | Outside DBWorker's role; removing Celery does not establish that Redis can be removed |

Celery references: [task execution, retry, rate limits and states](https://docs.celeryq.dev/en/stable/userguide/tasks.html), [calling, ETA, expiration and results](https://docs.celeryq.dev/en/stable/userguide/calling.html), [Canvas](https://docs.celeryq.dev/en/stable/userguide/canvas.html), [periodic tasks](https://docs.celeryq.dev/en/stable/userguide/periodic-tasks.html), [routing](https://docs.celeryq.dev/en/stable/userguide/routing.html), [worker control](https://docs.celeryq.dev/en/stable/userguide/workers.html), [monitoring](https://docs.celeryq.dev/en/stable/userguide/monitoring.html), [pool differences](https://docs.celeryq.dev/en/stable/userguide/concurrency/index.html).

Celery's task rate limit is per worker instance; a cluster-wide vendor quota requires additional coordination or restricted routing. ETA is an earliest start time, not an exact execution guarantee. Redis transport priority is approximate, not a universal strict ordering guarantee. Chords require supported result storage and participating tasks cannot simply ignore results. Compare the configured baseline rather than an idealized superset of every Celery feature.

## Boundaries and reliability details

### Constraints of the existing model

1. **No SQL representation, no work.** Producers can be other services or languages, but they must commit compatible durable input to the shared database, directly or through an API. Adding a generic job table enables call-like tasks at the cost of introducing the job model that the current design avoids.
2. **No coordination database, no new claims or durable completion.** Already-running computation may continue during an outage. It cannot complete the framework's SQL state transition until persistence works. Broker-based Celery can buffer and dispatch independently of the application database, although its task bodies may also need that database.
3. **One identity cannot represent many independent completed calls.** Changing a finished source's fields does not make it available again. `reset_failed()` resets failed work only. New occurrence/revision/request rows preserve independent history; a perpetual `Unfinished()` state machine can implement recurring processing but has different completion/history semantics.
4. **A SQL rollback cannot undo a remote effect.** If an HTTP server accepts a webhook and the process dies before SQL commit, redelivery may duplicate it. Use receiver idempotency keys, durable delivery state, and reconciliation. Celery has the same ambiguity; its own documentation asks for idempotent tasks.
5. **Neither system provides general hard real-time deadlines.** Polling, contention, network delays, and worker saturation all affect scheduling latency. Reducing polling intervals can improve DBWorker latency but cannot establish an unconditional bound.

The third constraint is about preserving semantics, not a claim that repeated processing is impossible in Python. Similarly, database-backed workflows are implementable; their absence today does not make Canvas-like outcomes fundamentally impossible.

### Concrete risks in the current runtime

These findings follow from code paths and require targeted fault tests before claiming production equivalence:

- `_run()` handles a handler future exception by calling `_fail()`. Abrupt death of a child can surface as `BrokenProcessPool`, leading to failed work and a broken executor. There is no executor rebuild path. Coordinator death, where renewal stops, has different recovery behavior. Lease reclamation is not automatic recovery from every kind of process failure.
- Exceptions from `claim()`, `renew()`, `_fail()`, and submission are not enclosed by a scheduler recovery loop. An SQL operational error can terminate a scheduling thread while the service process stays alive. Benchmark `check_alive()` only checks process exits, so it cannot detect this by itself.
- A stuck handler can keep receiving lease renewals. `stop()` drains active futures and can wait indefinitely without a task limit. A lease is an ownership timeout, not a computation timeout.
- Final writes check claim ownership by token/status, not simply whether the lease deadline passed. An expired lease does not kill the old process; another coordinator may claim the item while the original computation continues. The token fences stale supplied-session commits, not external requests or writes made with other sessions.
- Worker/source claims do not lock all related business resources. Two different source IDs can still modify the same account, file, or external resource. Such constraints need domain-level uniqueness, locks, ordering, or idempotency.
- Empty polls back off to 10 seconds by default, with no database notification waking the scheduler on a new business row. Idle-to-burst latency is therefore a first-class metric. Successful claims refill immediately, so busy throughput does not expose the same latency behavior.
- Concurrency is per coordinator, per registered worker. Adding coordinators increases the aggregate cap; it does not enforce a global concurrency or rate budget.
- Claims use application clocks. Multi-host clock skew and SQL contention need explicit testing. The compiled PostgreSQL/MySQL path is not evidence of exercised production behavior on those databases.

The current [Celery example](../../examples/imagededup_system_redis_celery/src/imagededup_system_redis_celery/celery_app.py) already configures late acknowledgment, worker-loss redelivery, prefetch 1, routing, soft/hard limits, and broker retry. Its [task class](../../examples/imagededup_system_redis_celery/src/imagededup_system_redis_celery/tasks.py) retries SQL operational errors. DBWorker lacks matching runtime policies. Happy-path image timings must not be presented as reliability equivalence.

Redis Celery also has constraints: its visibility timeout affects redelivery and delayed tasks, and persistence configuration determines loss windows. Extending visibility timeouts can defer recovery. [Redis transport documentation](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/redis.html).

## Benchmark and automation proposal

### Workloads grounded in the review

| Priority | Workload | Project pattern | What it establishes |
|---|---|---|---|
| P0 | Existing image hashing and paged comparison | Paperless ingestion/artifacts | CPU work, incremental progress, dependency readiness |
| P0 | Small SQL-only transformation | Saleor maintenance, durable outbox work | Coordination overhead without image-library dominance |
| P0 | Idle-to-burst and steady arrivals | Interactive requests across projects | Queue wait distribution and polling tradeoff |
| P0 | Two or more coordinators sharing PostgreSQL | Distributed workers | Claim contention, stale-write fencing, scaling |
| P0 | Fault injection | All production patterns | Recovery/liveness before throughput claims |
| P1 | Webhook delivery to an isolated local receiver | Saleor | I/O concurrency, retry policy, ambiguous effects |
| P1 | Versioned recomputation under repeated edits | PostHog cohorts, Paperless indexing | No lost revision; stale computation handling |
| P1 | Fan-out and persisted fan-in | Superset dependencies, PostHog export groups | Join overhead, failed prerequisite propagation |
| P2 | Periodic/delayed occurrences | Superset reports, Saleor maintenance | Due-time latency, misfires, overlap and restart behavior |

Unsupported semantics must be recorded explicitly. Do not silently add retries, a scheduler, or workflow infrastructure only to DBWorker and then describe the result as the current framework. Run current-runtime and extended-application variants separately.

### Baseline fairness

- Use Celery + Redis, prefork, explicit concurrency and queues, prefetch 1, late acknowledgment, and deliberate worker-loss behavior. Retain the existing SQL outbox baseline for a durable business-request comparison.
- A simpler direct-publication Celery variant can measure common enqueue overhead, but label its SQL-commit/publication crash window. Do not compare it as equally durable unless recovery closes that window.
- Use the same application database, inputs, calculation, batch sizes, indexes, and result validation. Include PostgreSQL, which exercises DBWorker's row-lock path; retain SQLite as a local deployment profile.
- Report defaults and separately tuned configurations. Sweep DBWorker polling and Celery dependency countdowns rather than choosing a fast setting for only one backend.
- Measure total-stack memory/CPU and the effect on foreground CRUD latency, SQL connections, locks, query counts, and database CPU/I/O. A smaller worker RSS does not establish a smaller overall cost if work moved into PostgreSQL.
- Separate warm/cold startup, steady throughput, backlog draining, idle latency, and large completed-history behavior. Run several repetitions and report distributions and variability, not a universal crossover threshold inferred from one image size.
- Record Redis AOF/fsync, SQL durability, result storage, visibility timeout, task limits, retry policies, platform, package versions, git SHA, random seed, and task granularity. Different durability boundaries must remain explicit.

### Required fault scenarios and invariants

Run failures at named barriers: before source commit; after source commit but before publication; after claim but before handler start; during computation; after SQL flush but before final commit; after remote acceptance but before delivery commit. Also kill a handler child separately from its coordinator, interrupt Redis, interrupt the database during claim and renewal, and test a hung handler.

Assert that each submitted request ends in a valid terminal state or an explicitly observable recoverable state; that unrelated work continues; that stale owners cannot commit supplied-session results; and that committed progress survives restart. For webhooks, record remote duplicates and verify idempotency behavior instead of pretending a task status proves single delivery. For workflows, distinguish all-success joins from all-terminal joins. For versions, distinguish processing every event from converging on the newest state.

The existing test suite covers important status, polling, lease, rollback, and stale-token cases. It does not establish the broader outage and multi-host claims above. Fault-test failures should be correctness findings, not discarded slow runs.

For this review, the 17 root unit tests and 7 example process tests passed against local source, using explicit `PYTHONPATH` because the existing environments did not expose the project packages by default. The process tests exercised Python 3.12; the root tests used Python 3.13. No new performance run or outage experiment was performed.

### Automation shape

Keep stack startup, workload generation, measurement, and correctness checking separate. Extend the existing sequential runner with workload IDs, backend adapters, readiness probes, deterministic barrier-based fault hooks, and deadline-based progress watchdogs. The harness should control both roots and descendants and record the fault timing in JSON.

Process existence is insufficient readiness or liveness. Probe worker progress/heartbeat and report a stalled scheduler even when the API remains healthy. A successful artifact build is also insufficient for end-to-end business success: validate durable result contents, revision/ledger counts, and receiver records outside timed measurement.

Every run should produce a versioned machine-readable report, process logs, configuration, source fingerprints, correctness verdicts, and explicit skipped/unsupported capabilities. CI should execute tiny deterministic correctness/fault cases. Performance regressions belong on controlled runners with retained historical distributions; shared CI timing alone is too noisy to establish small differences.

## Suggested product scope

Position DBWorker around durable SQL-owned requests and resumable procedures. Prioritize scheduler resilience to SQL failures, child-pool recovery, observable health, and a clear timeout/shutdown policy before expanding toward a full task platform. Versioned work and retry deadlines can remain explicit application models initially, with documented patterns.

The first benchmark claim should be narrow and testable: for specified durable row-processing workloads, DBWorker can replace the Celery enqueue/dispatch layer, with measured runtime cost, latency, database impact, and recovery behavior. Evidence from these projects does not justify a claim that it replaces Celery or Redis in their entirety.
