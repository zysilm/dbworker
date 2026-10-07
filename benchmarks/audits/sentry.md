# Historical Sentry native-task benchmark audit

## Finding

The current suite is a real SMTP utility experiment, but it is **not a native
Sentry Celery baseline**. It cannot satisfy the requirement to compare pinned
upstream Celery tasks against DBWorker with the same business jobs, granularity,
stages, timing boundaries and resources. Existing successful JSON artifacts show
that the custom bridge delivered the fixture; they do not establish native-task
replacement equivalence. This audit is static inspection only. No experiment,
implementation change or Git mutation was performed for this audit.

The pinned source is historical Sentry 24.1.0, commit
`94bf1b8aad4c21840829be1f362b096d12f782d0`. It must remain labeled historical;
current Sentry's taskbroker is outside this source and this experiment.

## Native entry points and business work

The actual Celery application is `sentry.celery.app`, an instance of
`SentryCelery` configured from Django settings and using `SentryTask`
([sentry/celery.py:86](../../examples/sentry/src/sentry/celery.py#L86),
[sentry/celery.py:112](../../examples/sentry/src/sentry/celery.py#L112),
[sentry/celery.py:121](../../examples/sentry/src/sentry/celery.py#L121)).

The terminal delivery tasks are:

- `sentry.tasks.email.send_email`, queue `email`, REGION silo, one message per
  task ([tasks/email.py:47](../../examples/sentry/src/sentry/tasks/email.py#L47)).
- `sentry.tasks.email.send_email_control`, queue `email.control`, CONTROL silo,
  also one message per task
  ([tasks/email.py:58](../../examples/sentry/src/sentry/tasks/email.py#L58)).

Both invoke `send_messages([message])`; neither calls a subprocess. The
`instrumented_task` decorator registers on the real app, wraps execution with
Sentry SDK tags, duration and memory metrics, disables task trails, and applies
silo restrictions
([tasks/base.py:76](../../examples/sentry/src/sentry/tasks/base.py#L76),
[tasks/base.py:110](../../examples/sentry/src/sentry/tasks/base.py#L110),
[tasks/base.py:121](../../examples/sentry/src/sentry/tasks/base.py#L121)).
`SentryTask.delay` adds publication timing context, and `apply_async` performs
pickle validation when enabled and measures publication
([sentry/celery.py:89](../../examples/sentry/src/sentry/celery.py#L89)).

The ordinary producer is `MessageBuilder.send_async`. It builds one message for
each distinct nonempty destination, chooses the REGION or CONTROL task, and
publishes one native task per built message. It also records queued-email logs
and metrics
([message_builder.py:198](../../examples/sentry/src/sentry/utils/email/message_builder.py#L198),
[message_builder.py:231](../../examples/sentry/src/sentry/utils/email/message_builder.py#L231)).
`build` applies reply headers, subject normalization, generated Message-Id,
optional group-thread database state, text/template rendering, HTML rendering
and CSS inlining
([message_builder.py:122](../../examples/sentry/src/sentry/utils/email/message_builder.py#L122),
[message_builder.py:142](../../examples/sentry/src/sentry/utils/email/message_builder.py#L142)).

A larger notification workflow may have preceding business tasks. For example,
`fetch_commits` is itself a native task and can produce error-email fan-out
([tasks/commits.py:68](../../examples/sentry/src/sentry/tasks/commits.py#L68));
invalid-identity handling builds a real template email, publishes delivery and
then deletes the identity
([tasks/commits.py:30](../../examples/sentry/src/sentry/tasks/commits.py#L30),
[tasks/commits.py:59](../../examples/sentry/src/sentry/tasks/commits.py#L59)).
Two-factor compliance also constructs real templates and calls `send_async`
([tasks/auth.py:165](../../examples/sentry/src/sentry/tasks/auth.py#L165)).
These examples demonstrate omitted business stages; the current synthetic SMTP
fixture cannot be described as executing either complete workflow.

## Current execution graph and counts

The harness launches `benchmarks.upstream.celery_app:app`, not
`sentry.celery.app`
([sentry_backend.py:111](../upstream/sentry_backend.py#L111)). The custom app
publishes `benchmark.execute_request` with an integer request ID; each task reads
a benchmark SQLite row, calls the adapter, then writes a benchmark result
([celery_app.py:19](../upstream/celery_app.py#L19),
[celery_app.py:24](../upstream/celery_app.py#L24)). DBWorker processes the same
request table through its shared integration handler
([runtime.py:25](../../examples/dbworker_integration/runtime.py#L25),
[runtime.py:87](../../examples/dbworker_integration/runtime.py#L87)).

Each adapter execution starts a fresh historical interpreter synchronously
([adapter.py:15](../../examples/sentry_dbworker/adapter.py#L15)). That child fully
initializes Sentry, blocks Celery publication/eager execution, manually constructs
Django messages and directly calls `send_messages`
([legacy_send.py:7](../../examples/sentry_dbworker/legacy_send.py#L7),
[legacy_send.py:18](../../examples/sentry_dbworker/legacy_send.py#L18),
[legacy_send.py:22](../../examples/sentry_dbworker/legacy_send.py#L22)).
The shared SMTP utility still performs real delivery, sent-email metrics and
logs ([email/send.py:15](../../examples/sentry/src/sentry/utils/email/send.py#L15)).

For `N` measured fixture requests, each with one recipient:

| Stage | Current Celery | Current DBWorker | Required native delivery-only comparison |
|---|---:|---:|---:|
| Benchmark request records | N | N | DBWorker may need N durable records; native baseline does not need artificial request/result records |
| Custom queue business tasks | N | N DBWorker claims | 0 custom Celery tasks |
| Native `send_email` / `send_email_control` tasks | 0 | 0 | N native tasks versus N DBWorker delivery jobs |
| Per-request historical app subprocesses | N | N | 0; initialize app once per persistent worker |
| SMTP messages | N | N | N with matching contents |
| Additional warm-up deliveries | 2 | 2 | Same declared warm-up counts, excluded from measured counts |

Current numerical equality of request and recipient counts is accidental to the
one-recipient fixture
([sentry_backend.py:80](../upstream/sentry_backend.py#L80),
[sentry_backend.py:119](../upstream/sentry_backend.py#L119),
[sentry_backend.py:140](../upstream/sentry_backend.py#L140)). With recipient sets
`R_i`, native `get_built_messages` emits `sum(|distinct nonempty R_i|)` delivery
tasks, while the bridge emits `N` custom queue tasks and batches each request's
messages into a single synchronous `send_messages` call. The current fixtures
do not exercise this mismatch. If selecting a larger notification scenario,
its preceding queued business tasks must also be counted and reproduced rather
than folded into a delivery job.

## Configuration, omissions and measurement bias

Native Sentry uses protocol 1, pickle payloads, no result backend, and the native
queue declarations; the queue flags are set nondurable
([conf/server.py:696](../../examples/sentry/src/sentry/conf/server.py#L696),
[conf/server.py:712](../../examples/sentry/src/sentry/conf/server.py#L712),
[conf/server.py:726](../../examples/sentry/src/sentry/conf/server.py#L726),
[conf/server.py:816](../../examples/sentry/src/sentry/conf/server.py#L816),
[conf/server.py:848](../../examples/sentry/src/sentry/conf/server.py#L848),
[conf/server.py:1232](../../examples/sentry/src/sentry/conf/server.py#L1232)).
The custom app instead sets late acknowledgements, reject-on-worker-loss and
prefetch 1, and publishes an integer rather than the upstream Django message
object ([celery_app.py:19](../upstream/celery_app.py#L19)). Native effective
acknowledgement and prefetch settings must be captured from the actual app;
the custom settings must not be attributed to upstream.

The email tasks declare a five-minute default retry delay and unlimited retry
metadata, but their body contains no `retry()` and no automatic-retry decorator
([tasks/email.py:47](../../examples/sentry/src/sentry/tasks/email.py#L47)). Do not
claim SMTP exceptions are automatically retried merely from `max_retries=None`.
A selected preceding task may have explicit retry behavior, as `fetch_commits`
does. Such behavior must be audited separately.

The timed batch includes durable request writes, custom publication, complete
per-message Sentry initialization, SMTP work and saved benchmark results. Native
persistent workers normally pay application initialization at startup. Adding
the same artificial bridge to both backends does not make its overhead a valid
native workload; it can dominate latency and conceal queue differences
([sentry_backend.py:119](../upstream/sentry_backend.py#L119),
[legacy_send.py:8](../../examples/sentry_dbworker/legacy_send.py#L8)).

The MIME/envelope oracle is valuable and should be retained, but it currently
checks handcrafted bodies instead of `MessageBuilder` output. Result waiting
uses artificial benchmark result rows, not native task terminal events. Native
Sentry intentionally does not use a result backend. CPU and peak RSS are null;
there is no matched resource measurement across queue processes, Redis,
application workers, SMTP receiver and subprocesses
([sentry_backend.py:144](../upstream/sentry_backend.py#L144),
[sentry_backend.py:173](../upstream/sentry_backend.py#L173)).

## Control no-ops versus business jobs

Worker startup checks, control `ping`, optional gossip/mingle and host readiness
checks are harness/control operations. Excluding them equally from a measured
batch is legitimate. Beat may be omitted for a declared on-demand email-only
scenario because unrelated periodic jobs are not triggered by that workload.

`send_email_control` is **not** a harmless control no-op: CONTROL denotes the
hybrid-cloud silo. It sends real customer email. Rendering/CSS, recipient fan-out,
notification producers, group-thread records, identity deletion, queue routing,
and failed-delivery handling are business behavior. They cannot be suppressed
and still counted as completed native notification work. Sentry's `safe_execute`
can swallow publication errors, so producer return alone is not a success oracle
([utils/safe.py:17](../../examples/sentry/src/sentry/utils/safe.py#L17)).

## Concrete migration and blockers

1. Retire the existing bridge comparison from native performance admission while
   preserving its historical artifacts and its accurate utility-only label.
2. Establish an in-process environment that can run both pinned Sentry business
   code and DBWorker. The present historical runtime is Python 3.10.20 and
   DBWorker packaging requires >=3.12
   ([sentry_provision.py:18](../upstream/sentry_provision.py#L18),
   [pyproject.toml:17](../../pyproject.toml#L17)). Either prove the pinned Sentry
   code and a transparently recorded compatible dependency lock on >=3.12 for
   both backends, or implement and separately review supported DBWorker runtime
   compatibility. A synchronous per-job bridge is not an acceptable resolution
   under the native-baseline requirement. Neither route is proven by this audit.
3. Start a persistent native worker on `sentry.celery.app`, configured from the
   isolated Sentry settings, consuming the genuine `email`/`email.control`
   queues as required by the fixture's silo. Use pinned native task objects and
   effective app settings; never register a replacement wrapper task as baseline.
4. Define the first admitted scope explicitly: terminal native delivery tasks
   versus full `MessageBuilder.send_async` production. Prefer the latter with
   deterministic rendered fixture templates and multiple distinct recipients.
   Preserve real message building, CSS, headers, deduplication and fan-out. If
   notification-model fixtures are outside scope, do not claim full notifications.
5. For DBWorker, provide one persistent delivery record and one handler execution
   per native built message, retaining the corresponding silo/queue category.
   Execute the same delivery business body in initialized worker processes;
   route producer publication to DBWorker records without eager execution or
   hidden Celery delivery. For any expanded producer-task scenario, persist and
   run each preceding stage separately and reproduce its output/state changes.
6. Record publication/execution counts by native task name and stage, with exactly
   the matching DBWorker jobs. Verify all expected SMTP messages, terminal task
   success and any business database state. Use observer signals/instrumentation
   without modifying the native task body; task return is `None`, not the
   bridge's invented `{accepted: 1}` protocol.
7. Begin timing at the same public producer/task-submission boundary and end at
   the same delivery-plus-terminal-success boundary. Exclude environment setup,
   persistent worker startup and warm-up equally. Capture queue latency and
   total-stack CPU/RSS with the same resource limits and two execution workers.
   Report storage/publication overhead differences instead of forcing an extra
   benchmark SQLite result transaction into the native baseline.
8. Only admit the replacement after native app/task identity, stage counts,
   multi-recipient fan-out, output oracle, resource metadata and failure behavior
   are verified. Then run the unified sequential runner and generate new results;
   no previous bridge result should be relabeled as native.
