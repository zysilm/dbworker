# Native PostHog two-stage notification variation

The baseline now starts the original `posthog.celery:app`, its original Django
application/settings/import graph and its existing email queue. It publishes the
original `posthog.tasks.email.send_two_factor_auth_enabled_email` task. That task
loads a genuine User, renders/inlines the original notification template and
publishes the original `posthog.email._send_email` task. Both stages are timed.
No benchmark Celery application/task, eager execution, projected AST module,
two-model synthetic application or pre-rendered payload replaces this baseline.

The DBWorker sibling has one durable notification row and a separate durable
delivery row per operation. The original notification callable runs with a narrow
process-local interception of only its delivery publication. The child payload
is persisted separately; another handler invokes the original delivery callable.
Actual user lookup, original subject/campaign generation, user display metadata,
empty native plain body, template/CSS work, SMTP fallback and MessagingRecord
transactions are retained. The two stages share two process slots. Exception
retries persist attempts and next-run times with native max-three exponential
backoff/jitter; successful-work admission rejects observed retry/duplicate traces.
Crash/replay correctness is not claimed merely from that policy implementation.

The business boundary starts before notification publication and ends after both
observed stages succeed, the receiver accepts mail, and DBWorker source completion
is durable. Full-scale 100 operations require 200 observed business nodes per arm,
one notification-to-delivery edge per operation. Control ping is not business work.
The native observer does not replace task bodies; origin checks verify task bodies
and the original exported application. Sample JSON retains the observed graph and
the trace path/hash. Existing paired-send results are historical diagnostics and
must not be relabeled as native measurements.

A pristine full native application dependency installation is now necessary.
The earlier notification-slice lock is insufficient: an actual initialization
attempt fails on missing `django_structlog`, before database/SMTP work. Further
native dependency or service requirements are not presumed resolved. PostgreSQL,
Redis and SMTP listeners are also prohibited in the current restricted execution
environment. No native live experiment or new performance result is claimed.
Admission failures produce `admission.json`, a nonzero exit and no successful
sample; there is no fallback to the old sliced application.

Offline checks verify the original worker command, absence of AST/module
projection and separate durable row/trace graph accounting. They do not claim
that native models, migrations, hooks, transport or retries were executed.
