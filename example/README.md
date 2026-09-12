# Durable worker example

This is a deliberately small, standalone FastAPI project. It demonstrates a durable database-backed pipeline without referring to any particular business domain.

## What it models

1. A workspace imports text files as **documents**.
2. Each document gets one **feature artifact**. A coordinator claims pending artifacts and sends CPU-heavy token-frequency extraction to a `ProcessPoolExecutor`.
3. A comparison request scores one ready artifact against every other ready artifact in its workspace and retains the best matches.

The database is the durable source of truth. It holds status, ownership token, and lease expiry; there is no broker queue containing one message per item.

## Run

From the repository root, activate the existing virtual environment so Poetry uses it:

```sh
source .venv/bin/activate
cd example
poetry install
poetry run durable-worker-example-api
```

The service listens on `http://127.0.0.1:8001` by default. Its SQLite database is `example.db` in this directory.

## Core pattern

- A coordinator thread claims only up to its process-pool capacity.
- A claim gives a row a random ownership token and short renewable lease.
- The coordinator renews leases while CPU child processes work.
- Only a matching token may write completion. If the coordinator dies, the lease expires and another coordinator can reclaim the row.
- The process-pool functions receive only serializable data and return normal Python values. Database sessions remain in the coordinator process.

The worker uses separate claim functions: `FOR UPDATE SKIP LOCKED` on supported PostgreSQL/MySQL/MariaDB versions, and conditional updates otherwise (including SQLite). Claims commit before CPU work is submitted. Comparison completion checks ownership and saves the top-K results, scored-candidate records, and count in one transaction.

Run the worker ownership tests from this directory:

```sh
PYTHONPATH=src poetry run python -m unittest discover -s tests -v
```

The tests use file-backed SQLite with separate connections. The locking path still requires integration testing against the corresponding database servers and transactional tables.

## Why comparisons are the awkward case

Building an artifact is independent: claim one row, calculate it, save it.

A comparison request aggregates many scores into one request row: it owns a score count and a retained top-K. A separate `scored_candidate` table records every completed pair, including candidates outside the top-K. Pages select ready candidates with no completion record, so artifacts that become ready out of order are not skipped. The pair of request ID and candidate ID is the primary key, preventing duplicate completion records. The example intentionally scores one bounded page at a time. That is easy to read and makes recovery clear, but it also exposes the design question a larger system must answer: how should it allow bounded parallel pages without several pages racing to update the same count and top-K?

A future framework can share claim/lease/process-pool mechanics, while retaining separate domain logic for independent work and aggregation work.

Progress no longer depends on increasing artifact IDs; IDs only order the currently ready candidates. The example still uses integer keys. Requests include ready candidates as they run and wait for pending/building artifacts; completed requests are not reopened for later imports. Empty-page finalization and blocked-request scheduling remain separate limitations.

Schema change: existing databases need migration before running this version. Remove `candidate_cursor_artifact_id` from `comparison_request` and create `scored_candidate`; old top-K results cannot reconstruct completion history, so existing comparisons need to be reset and rescored (clear their results, counts, and claims). `create_all()` alone does not migrate existing tables.
