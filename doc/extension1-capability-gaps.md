# Extension 1: Implementable Capability Gaps

Date: 2026-10-04
Branch: `extension1`
Baseline: DBWorker 0.0.6, commit `9e3e63f58b589b0502e5dd6f914a80f036de44ec`

## Purpose and status

This document records capabilities that the current implementation lacks, or only partially provides, but that can be implemented while retaining database-owned work. It is a design inventory, not a declaration that the features exist or a commitment to a final API.

The implementation evidence comes from [the core runtime](../src/dbworker.py), [the root README](../README.md), and [the Celery comparison application](../examples/imagededup_system_redis_celery/src/imagededup_system_redis_celery/celery_app.py). Motivation and pinned project sources are recorded in [the replacement research](../docs/research/celery-replacement.md).

Existing capabilities must not be confused with gaps. DBWorker already provides process concurrency, claims, automatic lease renewal, token-guarded final commits, basic execution status, manual failed-work reset, SQL eligibility, and resumable application procedures using `Unfinished()`. The extensions below build on those mechanisms.

## Design boundaries

Preserve these properties unless a later design explicitly changes them:

- Durable SQL data identifies work. Handler definitions and models remain importable for spawned processes.
- Business writes through the supplied session and the execution outcome commit together. Handlers do not commit or close that session.
- Ownership-sensitive transitions use conditional updates with the current claim token or an equivalent execution generation.
- Coordination waits do not consume handler processes. Delays and dependency waits belong in scheduling state and eligibility.
- SQLite remains a local deployment profile; PostgreSQL and MySQL need independently exercised concurrency behavior.
- External effects require idempotency or reconciliation. A SQL transaction cannot guarantee exactly-once email, HTTP delivery, filesystem changes, or writes through another database connection.

Features requiring additional domain tables should be described as application patterns or optional extensions. Their implementation cost must remain visible in comparisons with Celery.

## Inventory and priorities

P0 means prerequisite for credible recovery and automated fault testing. P1 means common application behavior that substantially broadens replacement scope. P2 means operational or integration expansion after core behavior is stable. Priorities are proposed, not an implementation schedule.

| ID | Capability | Current gap | Proposed location | Priority |
|---|---|---|---|---|
| E01 | Scheduler recovery from database errors | Coordination exceptions can end a scheduling thread | Core runtime | P0 |
| E02 | Recovery from child death and broken process pools | No executor rebuild policy | Core process supervision | P0 |
| E03 | Readiness, liveness, and progress health | No worker health contract | Core runtime and service adapter | P0 |
| E04 | Execution deadlines and bounded shutdown | No execution limit or drain deadline | Core process supervision | P0 |
| E05 | Automatic retry policies | Handler exceptions become terminal failures | Core outcome/policy support; optional application pattern | P1 |
| E06 | Per-item delays and expiration | No first-class due time or expiry lifecycle | Source fields and optional scheduling extension | P1 |
| E07 | Periodic schedules and occurrence generation | No cron, misfire, or recurring-run semantics | Optional scheduler and application tables | P1 |
| E08 | Versioned work and repeatable requests | Finished worker/source identity cannot identify a later run | Application models and optional public APIs | P1 |
| E09 | Workflow composition and failure propagation | No general graph, join, or callback abstraction | Optional workflow extension | P1 |
| E10 | Cancellation | No public pending/active cancellation lifecycle | Core runtime and cooperative handler context | P1 |
| E11 | Results, progress, and attempt history | Only basic status and latest error are recorded | Application storage and optional runtime metadata | P1 |
| E12 | Shared concurrency, rate limits, and resource ordering | Caps are local; no shared admission policy | Optional SQL admission control | P1 |
| E13 | Routing, partitions, priority, and capacity changes | Fixed per-worker pools and eligibility only | Registration/runtime configuration | P2 |
| E14 | Metrics, events, and management tools | Logs and state queries only | Runtime hooks and service integrations | P2 |
| E15 | Lower-latency wakeups and coordination efficiency | Polling can dominate idle latency or tiny-task cost | Core scheduler with backend adapters | P2 |
| E16 | ORM and execution-environment integrations | SQLAlchemy and synchronous process handlers only | Optional adapters | P2 |

## E01: Scheduler recovery from database errors

**Current evidence.** `_run()` handles exceptions obtained from handler futures, but calls to `claim()`, `renew()`, `_fail()`, and executor submission are outside a scheduler recovery boundary. A transient database failure can end the thread while the parent process remains alive. A failed final commit can also have an ambiguous outcome if the server committed before the connection was lost.

**Implementation direction.** Separate errors by phase. Retry safe coordination operations with bounded backoff and jitter, use fresh sessions after failed transactions, and reconcile persisted ownership/status before deciding whether work needs another attempt. Do not blindly rerun a handler because a commit acknowledgment was lost. Report degraded coordination, and define whether claiming pauses when renewal is unreliable. Retain ownership guards during recovery.

**Acceptance criteria.** Interrupt the database during claim, renewal, and failure recording. Restore it and verify that scheduling resumes or reports a deliberate terminal service failure. A committed success must not be overwritten as failed. Old owners must not persist final results after ownership replacement. The API staying alive must not make a dead scheduler appear healthy.

## E02: Recovery from child death and broken process pools

**Current evidence.** `ProcessPoolExecutor` is created once per worker. Abrupt child exit can break the pool and surface exceptions for submitted futures. The runtime does not rebuild the executor. Ordinary handler exceptions, child death, and coordinator death therefore have different behavior.

**Implementation direction.** Introduce an explicit pool lifecycle and classify infrastructure failures separately from application errors. Stop submissions into a broken pool, reconcile affected claims, create replacement capacity, and apply a bounded recovery policy. A lost process may have committed before disappearing, so query durable state before replay. Decide whether affected claims expire naturally or are explicitly released using ownership guards. Never release unrelated claims.

**Acceptance criteria.** Kill a child during computation and after a controlled commit barrier. Subsequent unrelated work must run. No completed execution may be replayed as though uncommitted. Repeated deterministic child crashes must stop at a policy limit or become visibly quarantined rather than causing an endless rebuild loop.

## E03: Readiness, liveness, and progress health

**Current evidence.** `_running` stores threads and executors, but there is no public health snapshot or durable coordinator heartbeat. Process existence does not prove that scheduling, renewal, or execution is functioning.

**Implementation direction.** Track scheduler-thread state, last successful coordination, pool state, active claims, and degraded reasons. Define readiness separately from liveness: a service can be alive but unable to claim work. Use monotonic time for local elapsed intervals. An optional SQL coordinator registry can expose multi-host ownership and heartbeats without turning it into another source of task truth. Keep health reads bounded in cost.

**Acceptance criteria.** A stopped scheduler thread and a broken pool must become observable promptly. Idle workers must remain healthy without requiring task completions. Database outages must report a coordination problem without generating false task-failure counts. The benchmark runner must detect lack of progress even when all process roots remain alive.

## E04: Execution deadlines and bounded shutdown

**Current evidence.** Active claims are renewed while futures remain active. A stuck handler can therefore retain a lease indefinitely. `stop()` waits for active work with no drain deadline. Lease duration is not execution duration.

**Implementation direction.** Define distinct execution, I/O, lease, and shutdown deadlines. Start execution timing at an explicit event, not an accidental enqueue timestamp. Cooperative cancellation can provide a soft deadline; hard termination needs a supervisor that knows which process owns an invocation and can replace it safely. Cancelling a running `Future` is insufficient. A stock shared executor does not provide a complete per-task termination protocol.

Define the terminal or recoverable status after timeout, when ownership is invalidated, and how termination is confirmed. If the database is unavailable, report an indeterminate result instead of claiming that fencing or failure recording succeeded. A bounded stop policy must specify whether unfinished claims remain recoverable.

**Acceptance criteria.** Hang a handler and verify a bounded return from shutdown under the configured policy. Test soft-deadline cleanup, hard termination, replacement capacity, and loss of database access during termination. Timeout recovery must not admit stale final writes or silently declare an unknown result successful.

## E05: Automatic retry policies

**Current evidence.** `_fail()` records `FAILED`, and `reset_failed()` requires an explicit external action. The polling backoff governs empty candidate searches; it does not retry failed handlers. `Unfinished()` represents continuation, not an exception-retry policy.

**Implementation direction.** Define retryable exception classes, attempt counting, maximum attempts, delay calculation, backoff cap, jitter, and exhaustion behavior. Persist `attempt_count`, `next_attempt_at`, and relevant error information in optional runtime metadata or application records. Distinguish a logical execution from attempts and normal continuation pages.

An application pattern can catch a known transient failure, roll back the failed transaction, persist retry state in a valid transaction, and return `Unfinished()`. It must not commit incidental partial business writes accidentally. A core retry outcome could standardize this, but its API remains undecided. Delays must release process capacity.

**Acceptance criteria.** Known transient failures recover after the expected attempt count; permanent failures do not retry. Restart preserves budgets and due times. Retry-state writes and execution transitions remain consistent. Lost commit acknowledgments are reconciled. Tests must verify that continuations do not consume retry budgets and that jitter prevents synchronized retry bursts.

## E06: Per-item delays and expiration

**Current evidence.** Eligibility can already filter application timestamps, but no dedicated delayed-work API, indexed due-time contract, expiry status, or expired-work cleanup exists. An item excluded by eligibility can remain unclaimed indefinitely.

**Implementation direction.** Support a `not_before` or `next_attempt_at` field and an optional `expires_at`, with explicit UTC semantics. Decide whether timestamps belong to application models or optional worker metadata. Separate execution lease expiry from task expiry. A lifecycle path must record expired work even when the ordinary handler is never eligible. Near-term timer wakeups can improve latency while periodic polling remains the recovery mechanism.

**Acceptance criteria.** Work never starts before its due time under the specified clock policy. Due times survive restart. Expired items become observable and do not execute. Test future backlogs, clock skew, overdue bursts, and items expiring while awaiting capacity. Record lateness rather than promising exact scheduling.

## E07: Periodic schedules and occurrence generation

**Current evidence.** There is no interval/cron scheduler. A worker completed against a schedule row is excluded from future claims. A permanently unfinished schedule row is possible, but it does not automatically provide independent occurrence history.

**Implementation direction.** Prefer a schedule definition plus durable occurrence rows when each run needs identity, retry, and audit history. Enforce a unique schedule/occurrence key so competing schedulers cannot create duplicate occurrences. Define timezone handling, daylight-saving transitions, missed-run policy, overlap policy, disabled schedules, and retention. An interval-only extension is smaller than a full cron implementation.

A continuing schedule-row state machine is an alternative for deliberately coalesced recurring maintenance; document its different run-history semantics. Occurrence generation and advancement of the schedule cursor should commit together.

**Acceptance criteria.** Two schedulers generate one occurrence per scheduled slot. Restart applies the documented skip, catch-up, or coalescing policy. Test overlapping runs, disabled schedules, failure/retry, and DST boundaries. The implementation must not hold handler slots while waiting for the next occurrence.

## E08: Versioned work and repeatable requests

**Current evidence.** Worker state is keyed by `source_id`. A `FINISHED` item is unavailable even if its business fields change. Ownership tokens prevent stale ownership commits, but do not independently detect input-version changes under the same ownership.

**Implementation direction.** Use request or revision rows with a single primary key and a domain uniqueness constraint such as `(entity_id, revision)`. Keep execution identity distinct from entity identity. Store immutable input or a version reference; check that version when publishing results. Define whether every event must run or only the newest version needs processing. A continuous dirty-state worker is possible, but needs careful coordination when edits arrive during processing.

**Acceptance criteria.** Submit revisions while an earlier revision is running. No required revision is lost, and an old calculation cannot overwrite a newer published result. Test duplicate submissions, latest-only coalescing, and every-event processing separately. Do not rely on manually deleting internal work rows to simulate a new execution.

## E09: Workflow composition and failure propagation

**Current evidence.** SQL eligibility and `has_execution_status()` support application-defined dependency gates. There is no reusable workflow graph, group membership, result propagation, join policy, or completion callback lifecycle.

**Implementation direction.** Add optional workflow, node, and dependency models with stable execution identities. Define sequence, fan-out, and fan-in behavior; distinguish all-success joins from all-terminal joins. Persist upstream results or references and make downstream creation idempotent. Reject cycles or explicitly support them as state machines. Define semantics for dynamically added children and closed groups.

A dependency filtered only on success can wait forever after upstream failure. Provide a separate failure/cancellation propagation path instead of leaving such nodes permanently ineligible. Durable callbacks should be ordinary identifiable work items.

**Acceptance criteria.** A join fires once only after the configured dependency condition. Failed/cancelled prerequisites produce the documented outcome. Test duplicate completion, concurrent final children, restart, empty groups, graph rejection, and dynamic membership. Waiting consumes coordination work rather than handler slots.

## E10: Cancellation

**Current evidence.** Eligibility can skip a pending source, but there is no public cancel transition, active cancellation signal, or cancelled status. Removing eligibility does not interrupt a claimed handler.

**Implementation direction.** Define cancellation requests and state transitions separately from failure. Pending cancellation must race safely with claims. Active cancellation can use a cooperative handler context; uncooperative termination depends on E04. Guard completion against cancellation/generation changes so success cannot overwrite an accepted cancellation. Define whether cancellation means request accepted, computation stopped, or final commit prevented.

**Acceptance criteria.** Test cancellation before claim, during claim, during computation, and during final commit. Expose the winner of each race. A cancelled task must not regain eligibility unintentionally. Remote side effects already accepted remain possible and must not be described as undone.

## E11: Results, progress, and attempt history

**Current evidence.** Handlers return only `Finished()` or `Unfinished()`. The state table holds status, token, lease, and a latest error string. There is no generic result handle, traceback record, attempt history, or progress event channel. Application-owned progress already works between committed continuation steps.

**Implementation direction.** Keep business results in domain tables and optionally expose a result-reference/query interface. Add bounded attempt records with start/end timestamps, failure classification, and sanitized traceback information. Define progress visibility explicitly: writes in the managed transaction are not visible until commit. Progress within a long invocation needs continuation boundaries or a separate telemetry channel that is not authoritative business success.

Preserve failures raised before rollback by recording their metadata through the runtime's separate coordination path. Define retention and cleanup for status/history tables.

**Acceptance criteria.** Results agree with committed business state. Rolled-back work is never published as complete. History survives restart without duplicate logical attempts. Progress reporting cannot corrupt claim ownership or introduce unwanted business commits. Cleanup preserves unfinished-work and dependency correctness.

## E12: Shared concurrency, rate limits, and resource ordering

**Current evidence.** `concurrency` limits one coordinator's worker pool. Two coordinators increase the aggregate capacity. There is no shared requests-per-second budget or serialization across different source rows referencing one resource.

**Implementation direction.** Separate admission policies: global concurrency slots, token-bucket rate budgets, tenant quotas, and resource serialization have different semantics. Use SQL conditional operations and expiring ownership records where needed. Acquire shared permits before dispatch without occupying a child while waiting. Define permit recovery and whether a failed attempt consumes rate budget. Never confuse concurrency with rate.

For strict per-resource ordering, define a resource key and sequence policy. A priority sort alone cannot prevent parallel completion or races on a shared account/file.

**Acceptance criteria.** Multiple coordinators respect the same configured budget. Permit leaks recover after crashes without uncontrolled over-admission. One tenant cannot indefinitely starve another under the chosen policy. Test clock behavior, long tasks, and same-resource updates. Measure SQL contention introduced by admission control.

## E13: Routing, partitions, priority, and capacity changes

**Current evidence.** Separate worker names get separate pools, and eligibility can partition or order candidates. There is no worker subscription API, process capability registry, dynamic pool resize, or fairness policy. A new worker name also creates a separate execution identity, which can accidentally process the same source twice.

**Implementation direction.** Allow multiple deployments to run the same logical worker with disjoint or overlapping capability predicates, sharing the same work table. Make runtime selection distinct from worker registration so API processes can inspect all definitions while services execute only selected ones. Define static CPU/GPU or tenant partitions before attempting dynamic routing. Preserve stable identity during capacity changes.

For priority, document selection order and add an aging/fairness policy if required. Dynamic resizing must drain or replace capacity without dropping ownership.

**Acceptance criteria.** Only capable processes execute each routed item. Partitions share completion identity correctly. Test overlapping deployments, configuration changes, drain behavior, and low-priority starvation. No claim of execution preemption or strict completion order follows from candidate ordering alone.

## E14: Metrics, events, and management tools

**Current evidence.** Runtime logging and individual state queries exist; there are no stable lifecycle hooks, metrics interface, cluster inspection API, or operational dashboard.

**Implementation direction.** Expose structured events for claim, attempt start, renewal trouble, continuation, retry, failure, lost ownership, pool recovery, and shutdown. Provide duration, queue wait, active work, backlog age, coordination errors, and recovery counters. Avoid source IDs as metric labels. Optional integrations can provide Prometheus, tracing, CLI inspection, and dashboards.

Business-critical callbacks need durable rows or an outbox; an in-memory event hook is best-effort telemetry. A hook failure must not change an already committed business outcome.

**Acceptance criteria.** Operators can distinguish an idle worker, an overloaded worker, a failed task, and a stalled scheduler. Event delivery failures do not break dispatch. Metrics remain bounded in cardinality. Management mutations use public ownership-aware APIs rather than unchecked internal-table edits.

## E15: Lower-latency wakeups and coordination efficiency

**Current evidence.** Empty polling backs off to `max_poll_seconds`, defaulting to 10 seconds. Application row commits do not send scheduler wakeups. Candidates are claimed individually, which can make coordination expensive for tiny tasks. Existing separate availability selection already addresses one query-planning concern.

**Implementation direction.** Retain portable polling and optionally add explicit application wakeups or backend notification adapters. Treat notifications as hints: periodic polling must recover missed signals. Tune polling by workload and add jitter across coordinators. Consider batch claims only after profiling, with clear limits on reserved work, fairness, and crash recovery. Keep query/index guidance backend-specific and measure completed-history growth.

**Acceptance criteria.** A missed notification cannot strand committed work. Measure idle-to-burst latency, empty-query cost, tiny-task throughput, contention, and foreground request latency. Batch reservation must not silently starve other coordinators or inflate recovery time. Publish defaults and tuned variants separately.

## E16: ORM and execution-environment integrations

**Current evidence.** Sources must be SQLAlchemy models with one primary-key column. Handlers receive a SQLAlchemy session, run synchronously in spawned Python processes, and must be importable. There is no Django transactional adapter or alternative handler pool contract.

**Implementation direction.** Start with documented interoperability patterns using explicit SQLAlchemy request models. A genuine Django adapter needs a model/transaction abstraction; Django writes do not automatically join the supplied SQLAlchemy transaction, even if both use the same database. Decide whether to preserve atomicity, introduce reconciliation, or support a different transaction owner explicitly.

Alternative thread or async execution can be optional for I/O-heavy work, with clearly different cancellation, CPU, isolation, and connection-lifecycle guarantees. Subprocess/container execution needs resource identity and supervision rather than merely serializing a function name.

**Acceptance criteria.** Adapters demonstrate the advertised commit/rollback boundary under failures. Imports do not start workers or connect unexpectedly. Pool-specific guarantees are documented and exercised. Do not describe an independently committed Django update as atomic with DBWorker completion.

## Cross-cutting state and compatibility decisions

The current public status enum has four values. Adding retry, cancelled, expired, or timed-out states affects status queries, schema representation, eligibility, and applications that treat status as terminal or nonterminal. A design may instead retain a small execution-state enum and add an outcome reason, but that choice must be made explicitly.

Before implementation, settle these questions:

1. Which fields belong to domain models, generated worker tables, or optional extension tables?
2. What identifies a logical execution, an attempt, a continuation, and an input revision?
3. Which operations can safely retry, and which need reconciliation after an ambiguous commit?
4. Which clock controls persisted due times and leases, and what skew assumptions apply?
5. How are generated schemas migrated without silently changing existing work identity?
6. Which APIs are synchronous queries, durable commands, or best-effort telemetry?

An optional extension must not require a generic job table for applications already well represented by domain rows. Additional request tables are appropriate when they express a genuine business occurrence or revision.

## Suggested implementation sequence

1. Implement E01-E03 and add deterministic database-outage, child-loss, and stalled-thread tests.
2. Establish E04 deadline/shutdown semantics and the process supervision model; reuse it for active cancellation in E10.
3. Design attempt metadata and E05-E06 together so retries survive restart and delayed work has a coherent lifecycle.
4. Document and validate E08 request/version patterns, followed by E07 occurrence scheduling and E09 workflows.
5. Add E11 history/progress and E12 admission policies according to the selected workloads.
6. Expand routing, operations, performance adapters, and integration scope only after the preceding contracts are stable.

Each increment should declare its semantics, schema/API changes, compatibility impact, correctness invariants, and benchmark variant. Feature presence and measured behavior must be reported separately.

## Validation and benchmark requirements

Use named barriers and deterministic faults instead of relying on arbitrary sleep durations. Exercise failures after claim, before dispatch, during computation, before final commit, and after remote acceptance. Kill a child independently from its coordinator. Interrupt the database during renewal as well as submission. Run at least two coordinators against PostgreSQL; retain SQLite checks for the local profile and add real MySQL coverage before making equivalent backend claims.

Measure completed business outcomes, stale-write rejection, retry/continuation counts, lost or duplicated occurrences, progress health, and recovery time. For external effects, validate receiver records and idempotency behavior. Unsupported outcomes must be reported as unsupported rather than silently simulated.

Performance measurements should include total-stack CPU and memory, database load, foreground API latency, idle wakeup latency, throughput, and tail queue wait. Keep the Celery + Redis baseline's durability, retry, limits, and routing configuration explicit. Compare the current runtime separately from any application or runtime extension.

This document adds no implementation and claims no new test coverage. It records the gaps and acceptance criteria for work on `extension1`.

## Reference material

- [Local replacement research and project evidence](../docs/research/celery-replacement.md)
- [Celery task policies](https://docs.celeryq.dev/en/stable/userguide/tasks.html)
- [Celery calling, delayed tasks, expiration, and results](https://docs.celeryq.dev/en/stable/userguide/calling.html)
- [Celery workflow composition](https://docs.celeryq.dev/en/stable/userguide/canvas.html)
- [Celery periodic scheduling](https://docs.celeryq.dev/en/stable/userguide/periodic-tasks.html)
- [Celery worker lifecycle and control](https://docs.celeryq.dev/en/stable/userguide/workers.html)
- [Celery monitoring](https://docs.celeryq.dev/en/stable/userguide/monitoring.html)
