# Coordinator handoff diagnostics

These local experiments diagnose scheduling behavior. They are separate from the
native application benchmarks and never update the official README result table.

`worker_handoff.py` uses the actual, unmodified `Coordinator`, spawned execution
processes, PostgreSQL or SQLite, and a job schema that matches the PostHog variation exactly.
Each notification creates one separate delivery job. Handlers perform 20 ms of
simulated service and commit through the original runtime. Native 2FA validation,
Django, rendering and SMTP are absent, so these results cannot establish native
PostHog performance or identify another machine's bottleneck by themselves.

## Fixed local reproduction

Both variants use 64 execution slots, 8 producers, 5,000 root operations and 5,000
delivery jobs across a 60-second submission schedule. There is no count, worker
or rate sweep. Processes start normally; startup is included. Child receipts cover
handler entry through the runtime-owned transaction's successful commit.
Mean running concurrency uses the interval from the first handler entry to the
last successful commit, rather than the complete wall-time interval.

| Measurement | Original eligibility | Pending filter and candidate index |
| --- | ---: | ---: |
| Wall time | 80.906 s | 60.129 s |
| Peak published unfinished jobs, sampled every 0.5 s | 936 | 169 |
| Mean simultaneously executing jobs | 3.072 | 4.100 |
| Peak simultaneously executing jobs | 23 | 24 |
| Successful claim time, median | 6.930 ms | 1.736 ms |
| Successful claim time, P95 | 12.713 ms | 3.491 ms |
| Cumulative successful claim time | 70.166 s | 21.346 s |
| Candidate SQL time, median | 5.739 ms | 0.669 ms |
| Claim return to handler entry, median | 1.034 ms | 1.066 ms |
| Handler entry to commit, median | 24.779 ms | 23.792 ms |

All 10,000 jobs completed once in each variant; stage counts and durable completion
were checked. [Report index and checksums](results/index.json) identifies the actual
core source revision. Full JSON and append-only execution receipts are retained.
This is one local paired diagnostic, not a statistically established speedup.

The original coordinator claims one source row at a time on a single scheduling
thread. Its candidate SQL uses an availability subquery over the source and work
tables. The completed-queue PostgreSQL plan performs 10,000 outer source-row checks,
10,000 additional source primary-key probes and 10,000 work-ledger probes. That
empty-queue query took 13.803 ms and touched 60,254 shared buffers. Measured candidate
query time during the actual run accounted for most successful claim duration.

The prototype adds `complete IS FALSE` to application eligibility and creates a
partial source index on `(next_run, id)` for incomplete jobs. It retains the same
core availability, ownership and lease predicates. The final query plan still
chooses a sequential source scan, but excludes completed records before entering
the availability joins; the inner primary-key probes do not execute. That query
took 0.986 ms and touched 228 shared buffers. The two changes were tested together;
these results do not prove a benefit from the partial index alone.

The evidence points to candidate selection and serial claiming, rather than slow
process handoff, in this diagnostic. At approximately 167 offered tasks per second
and 25 ms per handler, roughly four simultaneously busy workers can keep pace.
Low utilization with a growing backlog and low utilization without a growing
backlog therefore require different interpretations. The observed peak exceeded
five in both runs; this reproduces low average utilization, not a strict five-worker
ceiling.

## SQLite check for the reported 64-slot configuration

The same fixed 5,000-operation, 10,000-job workload also ran with unmodified
SQLite defaults: DELETE journaling and a 5,000 ms busy timeout. It finished in
60.125 seconds, with mean running concurrency 4.822, peak running concurrency 23,
and a sampled unfinished-job peak of 125. Successful claims took 1.775 ms at the
median and 8.212 ms at P95; claim-to-handler delay was 0.943 ms at the median.
Handler-entry-to-commit P95 was 45.260 ms, compared with a 20 ms simulated service.

All 10,000 tasks completed once. This SQLite fixture did not reproduce persistent
backlog or a five-worker ceiling. It establishes that low average concurrency can
be healthy at this offered load. Long claim or commit tails alone do not establish
lock-wait time; lock waits were not directly instrumented. Larger retained source
tables, different eligibility predicates, heavier writes and journal settings
require the affected application's own reproduction. These local runs overlap
untimed native application setup and are not controlled database speed comparisons.

An additional fixed read-query diagnostic contains exactly 100,000 completed
source rows and 100,000 FINISHED ledger rows. No workers start and no jobs execute.
After one cache warmup, 100 original candidate queries return no work; median query
time is 10.001 ms and P95 is 13.216 ms. The SQLite plan scans the source table in
the availability subquery and probes the ledger primary key for each source row.
This makes retained history a concrete investigation target. It does not reproduce
a live backlog or prove the affected machine has that history. Read-query time is
only one component of claiming, and these numbers are not maximum worker capacity.
The separate [query receipt](results/sqlite-history-100k.json) retains the original
SQL, plan, fixed fixture size and core hash.

## Reproduce

Use Python 3.12+, SQLAlchemy, psycopg, and local `initdb`/`postgres` binaries. Run
from an ordinary local checkout outside cloud synchronization; output directories
must be new. The script owns and shuts down its PostgreSQL cluster.

```sh
PYTHONPATH=.:src python -m benchmarks.diagnostics.worker_handoff \
  --output /tmp/handoff-baseline --workers 64 --operations 5000 \
  --window-seconds 60 --handler-seconds .02

PYTHONPATH=.:src python -m benchmarks.diagnostics.worker_handoff \
  --output /tmp/handoff-pending --workers 64 --operations 5000 \
  --window-seconds 60 --handler-seconds .02 --eligibility pending-index

PYTHONPATH=.:src python -m benchmarks.diagnostics.worker_handoff \
  --output /tmp/handoff-sqlite --database sqlite --workers 64 --operations 5000 \
  --window-seconds 60 --handler-seconds .02

PYTHONPATH=.:src python -m benchmarks.diagnostics.candidate_history \
  --output /tmp/handoff-history
```

## Next investigation

First instrument the unchanged native 2FA workflow to verify the same claim-time
distribution and plans with its actual database traffic. Review whether the source
completion predicate is valid for retries, failures and reclaimed leases before
changing that variation. A generic core fix must not assume every application has
a `complete` column. PostgreSQL candidate-query changes must also preserve custom
eligibility, ownership arbitration and `SKIP LOCKED`; SQLite needs separate review.
Batch claiming or multiple coordinators require their own ownership and fairness
checks and should follow query diagnosis rather than substitute for it.
