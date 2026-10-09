# Native PostHog 2FA validation variation

Both arms dispatch the original authenticated
`posthog.api.user.UserViewSet.two_factor_validate` DRF handler. The request
fixture retains native session authentication, CSRF checks, permissions and
throttles. Genuine TOTP validation, device creation, OTP login, persistent session
verification, setup cache cleanup and revocation of another real login session
run inside the measured interval. The fixture does not run a complete HTTP server
or the entire Django middleware chain.

User creation, two authenticated sessions, private TOTP setup keys and service
startup occur before measurement. Tokens are generated at invocation time. Each
successful response and its original device/session/cache/revocation effects are
validated before completing the interval. Private credentials and session keys
are not published; safe user/recipient hashes bind each operation to its fixture.

The Celery baseline uses the original `posthog.celery:app`, full native Django
settings/import graph and existing email queue. The handler's unchanged `.delay`
submits `send_two_factor_auth_enabled_email`, which performs the original user
lookup, template/CSS work and campaign generation before publishing a separate
original `_send_email` task. That task delivers to real local SMTP and commits the
original MessagingRecord. No benchmark Celery app or task replaces this workflow.

For DBWorker, a narrow synchronous context replaces only the original handler's
root notification publication. It persists one independent notification job for
each API call. Its handler invokes the original notification callable and captures
only the original delivery publication into a separate durable child job. Another
handler invokes the original delivery callable. Both stages share two process
slots, matching the native worker's total concurrency. The full 100-operation
profile therefore requires 100 genuine API successes, 100 notification executions
and 100 separate delivery executions per arm, plus two excluded warmup operations.

Root and child submission intent is observed before jobs become visible to
workers; successful DBWorker execution is observed after the source/ledger commit.
Actual worker processes verify the unchanged original registered callables. Trace
admission checks job identities, counts, edges, chronological phases and explicit
warmup identities. Captured clock boundaries cover publication, all API effects,
SMTP acceptance and both successful business stages. Final output validation
checks native message content and MessagingRecord/user/campaign correspondence.

The current handler-based workload passed all five repetitions per backend and
independent aggregate admission in
[GitHub Actions run 37906191421](https://github.com/zysilm/dbworker/actions/runs/37906191421)
at source revision `18321cb73b7ea6406e29877520cac6cbc97b6403`.
Earlier notification-only measurements remain historical and do not represent
this expanded API workload. Provisioning keeps the complete frozen original
application dependencies and verified native schema; no sliced dependency app or
fallback model graph is used.

Native Celery signals, metrics hooks, acknowledgments and autoretry wrappers are
retained on the baseline. DBWorker invokes the original business callable and owns
its execution lifecycle. Its exception policy persists max-three exponential
backoff/jitter, but successful-work admission rejects observed retries or duplicate
attempts. Fault recovery, ambiguous SMTP acceptance, crash/replay, deduplication,
ClickHouse work and complete middleware/network behavior are not established by
this successful-work benchmark.
