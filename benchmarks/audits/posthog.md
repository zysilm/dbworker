# PostHog native baseline audit

> Historical pre-migration audit. References to the current implementation below
> describe the implementation inspected at audit time, not the repaired native
> benchmark. See [the audit scope](README.md) and
> [the current benchmark contract](../README.md) for status and validation limits.

Date: 2026-10-05. Source pin: `526d64dd82340b1bf4293d6d9baea7e965997048`.
This is a read-only source and retained-result audit. No experiments were rerun,
no implementation was changed, and no Git metadata was modified.

## Verdict

The existing suite executes real SMTP sending and real MessagingRecord writes,
but it does **not** measure the native PostHog Celery application, native task
configuration, or complete 2FA notification workflow. Its successful paired
results establish a custom durable-request send-boundary experiment only.
Under the new requirement for the original application/tasks/configuration and
matching all business stages, PostHog must be migrated before its timings qualify.

## Proven native business path

Exact source references:

1. `examples/posthog/posthog/api/user.py:1585` begins the actual 2FA validation
   endpoint. At `:1595-1604` it validates/saves the device and changes the session;
   at `:1606` it publishes `send_two_factor_auth_enabled_email.delay(user.id)`.
   The notification benchmark must explicitly state whether it starts at this
   API operation or at the notification task submission. They are different scopes.
2. `examples/posthog/posthog/tasks/email.py:1660-1675` defines the real outer
   `send_two_factor_auth_enabled_email` task with `EMAIL_TASK_KWARGS` and the
   team-scope-audit decorator. It loads a real User, constructs a time/user-based
   campaign key, uses the fixed native subject, renders the 2FA template, adds the
   user recipient, and calls `message.send()`.
3. `examples/posthog/posthog/email.py:544-575` implements EmailMessage construction:
   availability checks, UTM properties, sanitization, template rendering and CSS
   inlining. `:587-593` supplies the user display name and distinct identity.
4. `examples/posthog/posthog/email.py:595-614` implements the default asynchronous
   send and publishes the separate `_send_email` task. At `:541` that task is named
   `posthog.email._send_email`. Its SMTP/HTTP selection occurs at `:508-538`.
5. `examples/posthog/posthog/email.py:288-388` performs real SMTP delivery. It opens
   a connection per task, loops over recipients, locks/creates delivery records,
   skips already-sent campaigns, creates MIME alternatives, sends, records sent_at
   and updates counters. Per-recipient ORM transactions occur at `:319-354`.

Consequently, for N successful outer 2FA notification submissions, the visible
source path has N outer notification tasks plus N inner send tasks, with one
recipient per notification: **2N business task publications on the nominal path**.
Rendering and User lookup are business work in the outer stage, not control no-ops.
This is a source-derived nominal count, not a measured native execution count.
Retries, redeliveries, signals, startup work and broader API follow-ups have not
been traced under a native worker. Other notification helpers may have other
recipient counts and cannot be assigned this 2N rule without separate inspection.

## Native application and task configuration

The real application is `examples/posthog/posthog/celery.py:43`,
`Celery("posthog")`. It loads Django CELERY settings at `:87`, autodiscovers tasks
at `:90`, sets broker_pool_limit=0 at `:94`, and adds the DjangoStructLog worker
step at `:96-97`. Native child initialization at `:210-245` initializes analytics,
OpenTelemetry, metrics and worker series. Task prerun/postrun instrumentation is
at `:287-313`, with success/failure/retry counters at `:316-330`.

`examples/posthog/posthog/settings/celery.py:9-10` declares the default queue;
`:18-33` declares native task imports; `:34-38` declares Redis broker/result
configuration. `examples/posthog/posthog/email.py:99-105` puts the email tasks on
queue `email`, ignores results, and configures Exception autoretry, max_retries=3
and retry_backoff. `examples/posthog/posthog/celery_queues.py:35` names that queue.
A native fixture worker must consume it, rather than only the default queue.

These settings and lifecycle operations are absent from the current custom app;
merely importing a shared native task object does not prove it was published or
executed through the native application.

## Current executed path and counts

`benchmarks/upstream/posthog_backend.py:123-142` creates all messages and rendered
payloads in the parent before timing. These are synthetic notification submissions,
not calls to the actual outer 2FA task. At `:160-164` it starts
`benchmarks.upstream.celery_app:app`; at `:175-177` it publishes
`benchmark.execute_request` once per payload.

`benchmarks/upstream/celery_app.py:19-25` constructs a new benchmark Celery app and
custom task, with late acknowledgement/prefetch=1/reject-on-worker-lost settings.
At `:26-36` it loads the additional SQLAlchemy Request, invokes the shared adapter,
then writes Request.result. It does not use native PostHog task retry/routing or
native PostHog app hooks. DBWorker executes the same custom Request boundary via
`examples/dbworker_integration/runtime.py:87-105`.

`examples/posthog_dbworker/adapter.py:15-21` calls `_send_email_now` synchronously
and verifies per-campaign recipient delivery rows. It omits the outer task and
all nested native publication. It adds ledger-verification reads before completing
its own Request result.

Retained `benchmarks/results/all-full-validated/posthog.json` and
`benchmarks/results/latest/posthog.json` each report `passed`, profile `full`, ten
samples: five Celery and five DBWorker samples, 100 measured messages per sample.
Thus they record **500 measured accepted messages per backend**, not 500 completed
native 2FA workflows. Code at `posthog_backend.py:189-190` also runs two warm-up
payloads per backend/repetition: ten warm-up sends across five repetitions.

| Quantity | Current evidence | Native workflow evidence |
|---|---|---|
| Timed input payloads per backend/repetition | 100 in retained full reports | No native invocation retained |
| Producer business publications | 100 custom wrapper delay calls in the visible nominal code path | Nominal 100 outer + 100 inner publications for the chosen native outer task |
| SMTP accepted messages | Exactly 100 measured + 2 warm-up checked by current fixture | Unknown until native run |
| Recipient count | One per generated payload | One for the inspected native 2FA helper |
| Request/DBWorker completion rows | 102 checked for DBWorker in current code | Not a native PostHog business result |
| Actual task attempts, retries, redeliveries | Not counted by task identity/stage | Unknown |

The current no-missing/no-duplicate receiver checks do not prove exactly one handler
attempt: native SMTP deduplication can skip a repeated accepted campaign. Readiness
`celery.control.ping` at `posthog_backend.py:164` is a control exchange, not a
business task, and must not inflate business-work counts. Table/model creation,
process launch and warm-up are setup, separately counted from measured work.

## Scope and output omissions

- Current fixture uses `EmailMessage` directly, an artificial subject, stable
  artificial campaign key and bare recipient (`posthog_backend.py:125-138`). The
  native outer task uses the real User, a timestamp/user-UUID campaign key, fixed
  subject, user display name/distinct_id and `use_http=True`
  (`tasks/email.py:1663-1675`). Current code forces `use_http=False` and supplies a
  custom nonempty plain body. The native constructor leaves txt_body empty
  (`email.py:574`), and this native helper does not set it. These are output and
  stage differences, despite reuse of the same HTML template.
- With a real native SMTP fixture, empty CUSTOMER_IO_API_KEY can make the original
  `use_http=True` helper fall back to SMTP without changing its code
  (`email.py:520-528`). This must be configured and reported explicitly.
- The AST bootstrap replaces package/module loading through sys.modules namespaces
  and selected original AST nodes (`examples/posthog_dbworker/bootstrap.py:29-57`,
  `:68-91`). Selected function/class bodies are real, but native package initializers
  and the complete application graph are bypassed.
- Custom NotificationConfig registers only MessagingRecord and InstanceSetting
  (`examples/posthog_dbworker/apps.py:7-17`). Custom settings install only that app
  (`examples/posthog_dbworker/settings.py:7`) and the backend creates two models
  directly with schema_editor (`posthog_backend.py:118-120`), rather than running
  native app configuration and migrations. Actual `PostHogConfig.ready` performs
  additional registration/setup (`examples/posthog/posthog/apps.py:24-43`,
  `:142-158`). The native outer helper needs User and broader imports unavailable
  in this two-model fixture.
- The scoped lock is deliberately labeled a notification slice
  (`benchmarks/locks/posthog.txt:1`), not the full native environment. Native task
  aggregation imports other modules (`examples/posthog/posthog/tasks/__init__.py:3-29`);
  outer email task imports domain models/products (`tasks/email.py:18-65`). Native
  configuration compatibility remains unproven by slice installation.
- Timing starts at custom Request creation and ends at saved Request results
  (`posthog_backend.py:168-187`), excluding all rendering. That suits a pre-rendered
  send diagnostic, but drops native outer-stage work and one queue transition.
  Current post-send adapter validation is included; final receiver oracle is not.
- Current SMTP MIME/ledger validation is substantive (`posthog_backend.py:193-227`),
  but native template/output parity is not established by artificial fixture parity.
  Dynamic campaigns/Message-IDs require correlation-aware normalization, preserving
  uniqueness and correct user/campaign mapping rather than replacing native data.
- Metrics expose wall time/throughput only (`posthog_backend.py:236-246`), not all
  native worker, producer, database, broker, receiver and telemetry resource costs.
  Two workers in the simplified app are not evidence of native resource equivalence.

## Concrete native migration plan

1. Define the benchmark boundary explicitly: notification-only starts before the
   real outer task submission; an API/2FA benchmark starts before the actual API
   operation and includes its business state changes. Do not silently use a direct
   inner send as the outer-workflow baseline.
2. Provision the pinned native Python 3.14.7 application and native dependency
   graph. Initialize the original Django app and real migrations; seed genuine
   users and required state. Start `-A posthog.celery:app` with the real email task
   registration and `email` queue. Use supported settings overrides for isolated
   PostgreSQL/Redis/SMTP and external telemetry policy; record every override.
   No synthetic sys.modules/AST application replacement on this baseline.
3. Submit the original `send_two_factor_auth_enabled_email` task through its real
   producer. Retain the native outer and `_send_email` stages, arguments, retries,
   routing and lifecycle. If a direct native `_send_email` comparison is useful,
   retain it as a separate diagnostic scenario, not the required outer workflow.
4. Add the matching DBWorker variation as two durable business stages: one executes
   original user lookup/rendering/recipient/campaign generation; one sends the exact
   produced payload. Route the native nested submission through a narrow reviewed
   sibling seam into durable DBWorker stage-two work. Do not call the entire native
   flow synchronously inside one handler and label its granularity equivalent.
   Preserve side effects, deduplication and equivalent retry policies explicitly.
5. Record per-input correlation and task/stage identities, publications, attempts,
   retries, results and receiver acceptance. Report nominal/observed fan-out
   separately. Count readiness/control traffic and unrelated startup tasks separately
   from business work. Completion requires all selected stages plus receiver and
   durable MessagingRecord checks, not just outer-task success.
6. Use the same native fixture, actual subjects/bodies/user identities and transport
   settings on both backends. Time from equivalent submission through complete
   delivery/required persistence. Instrument total owned process/service resources,
   application instrumentation and sender/receiver work. Keep setup and output
   validation outside timing consistently, except genuine business persistence.
7. Admit with real paired smoke first, then full sequential repetitions. Preserve
   existing paired artifacts as diagnostic history; do not relabel their old timings
   as native results or overwrite them.

## Concrete blockers and unresolved measurements

The currently installed slice cannot import/initialize the original User-based
outer task, native PostHogConfig, native task aggregator and worker hooks as an
admitted full application. Their locked dependencies and required runtime services
must be resolved, not presumed to require every unrelated platform service.
Native startup imports ClickHouse tagging and telemetry code even for mail;
`celery.py:287-297` shows that this is a real initialization concern. Which services
are actually required and which supported no-op telemetry configurations work is
unknown until native admission.

DBWorker currently has no reviewed two-stage interception of `message.send()` and
no equivalent autoretry/backoff policy for the native outer/inner tasks. The current
guard rejects nested Celery dispatch instead of translating it. Native publication,
retry, hook overhead and task counts have no retained runtime trace. Those are
implementation/admission blockers under the new contract, rather than proof that
the previously retained real SMTP diagnostic was fake or that it lacked useful
correctness evidence.
