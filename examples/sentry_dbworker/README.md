# Historical Sentry native email variation

This sibling variation retains the pristine Sentry 24.1.0 source at commit
`94bf1b8aad4c21840829be1f362b096d12f782d0`. It replaces native email transport
publication with an individual durable DBWorker delivery record for each message.
The former per-request subprocess bridge has been removed. Current Sentry's
taskbroker and broader notification workflows are outside this historical suite.

## Native producer and delivery

Both arms call the real `MessageBuilder.send_async`. Native template rendering,
CSS inlining, subject normalization, generated message IDs, reply headers,
recipient deduplication, silo routing, queued-email logging, and metrics inside the original task body remain
enabled. Native Celery worker signal metrics are distinct: DBWorker does not
emit Sentry's separate signal-driven `jobs.started`/`jobs.finished` counters or
create a Celery request context. The fixture supplies two distinct recipients plus a duplicate and an
empty address. Native deduplication therefore publishes two independent delivery
jobs per operation: 100 measured operations require 200 delivered messages.

The Celery arm starts a persistent worker using the exported `sentry.celery:app`
and original `send_email`/`send_email_control` registrations. A startup-only
bootstrap configures Sentry once before entering the original Celery CLI; it
declares no tasks or replacement application. Original protocol 1, pickle serialization,
native task class, instrumentation, queue declarations, and silo guards remain.
Live source admission records the original registered functions and configuration.

The DBWorker arm intercepts only `kombu.Producer.publish` in the producer process.
It persists the complete native protocol-1 envelope, one row per recipient
message. The native producer and `SentryTask` publication path still execute.
Each persistent DBWorker process initializes the original application once and
executes the corresponding registered task's original `run` method, retaining its
instrumentation and silo guard. Completion is observed after DBWorker commits.
No handler merges recipient deliveries or starts another interpreter per job.

Auxiliary Redis remains present in both arms for native application cache/options
configuration. This experiment replaces the job queue; it does not claim to
remove every Redis dependency from Sentry. Pickled messages are trusted local
benchmark data. SMTP receipt is an external side effect, so this successful-work
experiment does not establish exactly-once delivery or crash-recovery parity.

## Runtime admission and provisioning

DBWorker supports Python >=3.12. Both application arms now attempt the same
Python 3.12 dependency graph from `benchmarks/locks/sentry-native.txt`, which
copies the historical Sentry lock plus SMTP receiver and resource helpers.
Only the DBWorker environment additionally installs the framework package.
Historical Python 3.10 was sufficient for the removed bridge; it cannot be used
as an unsupported DBWorker runtime or as a hidden per-job fallback.

The first recorded compatibility change is `hiredis==0.3.1` to `hiredis==2.3.2`.
The original extension's setup imports `imp`, removed in Python 3.12, and failed
during a real dependency installation. Both application arms use the same updated
Redis parser pin; this does not change Sentry task bodies, producer logic, pickle
protocol, queue policies, or Django rendering. Further incompatibilities require
the same evidence and an explicitly shared pin update; compatibility remains
unproven until full application initialization and delivery pass.

`typing-extensions==4.5.0` is also raised to 4.6.0 in both arms. SQLAlchemy's
declared dependency requires >=4.6.0, so preserving 4.5.0 would cause a dependency
conflict or a silent difference between the two application environments. The
shared lock selects that declared minimum; native queue packages remain frozen.

The original `grpcio==1.56.0` C++ build failed with modern Clang. Both arms select
`grpcio==1.59.3`, whose [PyPI release](https://pypi.org/project/grpcio/1.59.3/#files)
provides Python 3.12 macOS and Linux wheels. `grpcio-status==1.56.0` remains pinned:
its declared `grpcio>=1.56.0` constraint accepts this version. RPC is outside the
email fixture, while all original Sentry imports and task instrumentation remain.

Both arms select `xmlsec==1.3.14` instead of upstream 1.3.13. This minimum next
release removes obsolete SOAP constants used by 1.3.13 that are absent from modern
libxmlsec. Its [original build requirements](https://github.com/xmlsec/python-xmlsec/blob/1.3.14/pyproject.toml)
accept the unchanged `lxml==4.9.3` headers and historical setuptools constraints.
The previous isolated lock selected 1.3.17 for packaging, but its
[build requirements](https://github.com/xmlsec/python-xmlsec/blob/1.3.17/pyproject.toml)
force `lxml==6.0.2` and `setuptools==80.9.0`, producing a real unsatisfiable
source-build resolution with the historical application graph. Build isolation
remains enabled, and the declared requirements are honored. XML signing is
outside the selected email workload.

A real Linux native worker startup exposed incompatible bundled libxml2 versions
in the lxml and xmlsec wheels while Django checked the original SAML URLs. The
provisioner uses `lxml==4.9.3` and `xmlsec==1.3.14`, rebuilding both from source
against the same system libxml2 in each arm. It discards cached builds for these
two packages and checks their compiled and runtime library versions before
admission. Each result records this linkage evidence. Native URL checks remain
enabled. Source builds require the system compiler, Python headers, pkg-config,
libxml2, libxslt, and xmlsec development packages, supplied by the benchmark image.

```sh
python benchmarks/upstream/sentry_provision.py
python benchmarks/run_all.py --suite sentry --provision
```

Python 3.12 compatibility is a required admission check, not an assumed feature
of the historical lock. Dependency resolution, native extension builds, and full
application initialization must succeed without deleting instrumentation or
substituting application modules. Resolution/build errors stop provisioning.
Runtime errors produce structured `admission.json` evidence and stop execution;
the suite never substitutes the old SMTP utility bridge or generates a passing
native result from it. Any compatibility changes must be explicitly pinned and
used identically by both arms before new results can be admitted.

## Timing and evidence

Timing begins before real `MessageBuilder` rendering/publication and ends only
after all expected SMTP receipts and terminal job successes. Service setup,
persistent worker initialization, and two warm-up operations are excluded equally.
The native baseline has no artificial request/result database transactions.
DBWorker naturally includes its durable publication and outcome transactions.

Observer signals record actual native task publication/execution. DBWorker records
one corresponding delivery node per native envelope and success after commit.
`workflow.jsonl` and its SHA-256 accompany each result. Graph admission requires
exactly two delivery nodes per operation, with no missing, duplicate, failed,
retried, uncorrelated, or unexpected business nodes.

The SMTP oracle checks exact recipient identities, envelopes, visible address and
reply headers, normalized subject, independently expected text and HTML template
content, inlined CSS, one valid native Message-ID and Date header per receipt.
Original random Message-ID values can collide; collision diagnostics are separate
from the exact unique operation/recipient delivery requirement. Generated Message-Id, Date,
and MIME boundaries are excluded from cross-backend output normalization.
`smtp-evidence.json` retains safe decoded observed headers, their occurrences,
SMTP envelopes and MIME text/HTML parts. Aggregate admission derives the trusted
100-operation fixture independently, repeats the content oracle and recomputes
the business digest. Raw synthetic MIME receipts remain matrix diagnostics.
`measurement_window` binds the reported monotonic duration to Unix trace timestamps;
every measured publication/start/success must fall inside its recorded bounds.
The two warmup operation IDs are explicitly recorded and excluded. CPU and summed process RSS
cover the producer, receiver, coordinator, Redis, persistent workers and children;
shared pages can be counted twice in summed RSS.

This is a successful native email production/delivery workload. Group-thread
models, preceding notification tasks, current taskbroker behavior, broker or
database outages, retries and crash recovery require separate experiments.
