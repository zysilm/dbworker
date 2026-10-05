# Historical Sentry SMTP bridge

This variation calls the unmodified Sentry 24.1.0 `sentry.utils.email.send_messages`
utility after the upstream runner initializes the application. It does not
reimplement the SMTP operation or substitute application modules. Django creates
real multipart MIME messages and sends them to an isolated local SMTP receiver.
The utility uses Sentry's real options manager, logging and metrics code.

Both queue backends store an identical durable request and start the same
synchronous subprocess for each operation. Consequently, both include complete
historical app initialization and bridge overhead in their measurements. Results
are labeled `paired_subprocess_bridge`. These are historical SMTP utility
measurements, not a claim about the current Sentry taskbroker or complete native
Celery notification lifecycle replacement.

## Environments

The queue environments run Python 3.12. The historical application runs Python
3.10.20 in its own environment. Although upstream `setup.py` recommends 3.8,
its frozen requirements explicitly state they were generated on Python 3.10;
on Python 3.8 they fail resolution because `sentry-relay==0.8.41` requires
Python >=3.9 and the botocore/urllib3 constraints conflict. This compatibility
choice is recorded in every sample.

The full public historical dependency graph is pinned in
`benchmarks/locks/sentry-upstream.txt`. `xmlsec==1.3.17` replaces the historical
1.3.13 pin to use an ARM-compatible wheel. XML signing is outside this SMTP
workload. Old PyYAML builds require Cython <3 and setuptools <70; those build
constraints are checked in. The upstream checkout remains clean and pinned.

Run `python benchmarks/upstream/sentry_provision.py` to provision all four isolated
environments. The central provisioner also calls its `provision` function.
Redis must be available on PATH. Services use isolated temporary output
directories and dynamically selected loopback ports.

## Experiment and oracle

Each backend warms up with two messages, then sends the configured number of
measured messages using two queue workers. A request has one recipient, a
stable subject, Unicode plain and HTML bodies, a stable Message-Id and an
X-Benchmark header. The SMTP receiver checks envelope sender/recipient,
visible From/To, subject, both MIME alternatives and custom headers against
independently generated fixtures. It requires exactly one delivered message per
request and verifies upstream accepted counts. Only Date and generated MIME
boundary values are excluded from the normalized output digest. Delivered
normalized messages are retained in `smtp-delivery.json`. DBWorker must also
have a finished ledger entry for every measured and warm-up request.

Timing begins before durable request creation/publication and ends after results
are saved following synchronous SMTP delivery. Environment setup, queue worker
startup, the SMTP receiver and warm-up are excluded. CPU and peak memory are
explicitly unavailable; crash recovery, native task callbacks, templates,
notification models and production delivery guarantees remain untested.
