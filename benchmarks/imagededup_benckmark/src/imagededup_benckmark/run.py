"""Run both image APIs sequentially and retain structured results."""

import argparse
import hashlib
import json
import os
import platform
import shutil
import sqlite3
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil

from imagededup_benckmark.dataset import DEFAULT_DIRECTORY, download_dataset
from imagededup_benckmark.measurement import Measurement, distribution
from imagededup_benckmark.runtime import WORKERS, Stack

PROJECT = Path(__file__).resolve().parents[2]


def file_bytes(path: Path) -> bytes:
    # Read the observed file length and reject an incomplete fingerprint.
    size = path.stat().st_size
    with path.open("rb") as stream:
        content = stream.read(size)
    if len(content) != size:
        raise OSError(f"Incomplete read: {path}")
    return content


def select_images(directory: Path, count: int) -> list[Path]:
    files = [directory / f"im{index}.jpg" for index in range(1, count + 1)]
    missing = [str(path) for path in files if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Missing MIRFLICKR images, including {missing[0]}; download them first or use --download")
    return files


def prepare_images(files: list[Path], directory: Path) -> list[dict[str, Any]]:
    directory.mkdir(parents=True, exist_ok=False)
    manifest: list[dict[str, Any]] = []
    for index, source in enumerate(files, 1):
        target = directory / f"{index:06d}_{source.name}"
        # Stable API import ordering, with exactly the same bytes for both apps.
        try:
            os.link(source, target)
        except OSError:
            shutil.copyfile(source, target)
        manifest.append({"name": source.name, "bytes": source.stat().st_size,
                         "sha256": hashlib.sha256(file_bytes(source)).hexdigest()})
    return manifest


def validate(stack: Stack, workspace: int, artifact_ids: list[int], request_ids: list[int],
             *, top_k: int, max_distance: int) -> dict[str, Any]:
    # Untimed, complete verification. SQL uses only shared application tables;
    # final statuses are verified through each application's public HTTP API.
    count = len(artifact_ids)
    for offset in range(0, count, 1000):
        after_id = artifact_ids[offset - 1] if offset else 0
        rows = stack.request("GET", f"/workspaces/{workspace}/artifacts?after_id={after_id}&limit=1000")
        if any(row["execution_status"] != "finished" or row["error"] is not None for row in rows):
            raise AssertionError(f"Build failures in workspace {workspace}: {rows}")
    result_digests: list[str] = []
    with sqlite3.connect(stack.database) as connection:
        hashes = dict(connection.execute("SELECT id,hash_value FROM feature_artifact WHERE workspace_id=?", (workspace,)))
        if len(hashes) != count or any(value is None for value in hashes.values()):
            raise AssertionError("Wrong number of completed image hashes")
        ordinal = {key: index for index, key in enumerate(artifact_ids)}
        for query_id, request_id in zip(artifact_ids, request_ids, strict=False):
            status = stack.request("GET", f"/comparisons/{request_id}")
            if status["execution_status"] != "finished" or status["candidates_scored_count"] != count - 1:
                raise AssertionError(f"Incomplete comparison {status}")
            results = stack.request("GET", f"/comparisons/{request_id}/results")
            expected = sorted(((key, (int(hashes[query_id], 16) ^ int(value, 16)).bit_count())
                               for key, value in hashes.items() if key != query_id), key=lambda item: (item[1], item[0]))
            expected = [item for item in expected if item[1] <= max_distance][:top_k]
            actual = [(row["candidate_artifact_id"], row["distance"]) for row in results]
            if actual != expected:
                raise AssertionError(f"Incorrect top-K for request {request_id}")
            ledger_count = connection.execute("SELECT COUNT(*) FROM scored_candidate WHERE request_id=?", (request_id,)).fetchone()[0]
            if ledger_count != count - 1:
                raise AssertionError(f"Wrong completion ledger count: {request_id}")
            normalized = [(ordinal[key], distance) for key, distance in actual]
            result_digests.append(hashlib.sha256(json.dumps(normalized).encode()).hexdigest())
    hash_digest = hashlib.sha256(json.dumps([hashes[key] for key in artifact_ids]).encode()).hexdigest()
    return {"passed": True, "hashes_digest": hash_digest, "top_k_digests": result_digests,
            "artifacts": count, "requests": len(request_ids), "scored_pairs": len(request_ids) * (count - 1)}


def scenario(stack: Stack, kind: str, directory: Path, count: int, *, page_size: int,
             top_k: int, max_distance: int, interval: float, timeout: float,
             existing: tuple[int, list[int]] | None = None) -> tuple[dict[str, Any], tuple[int, list[int]]]:
    if existing is None:
        workspace = int(stack.request("POST", "/workspaces", {"name": kind})["id"])
        artifact_ids: list[int] = []
    else:
        workspace, artifact_ids = existing
    request_ids: list[int] = []
    comparison_count = 0 if kind == "build" else count
    meter = Measurement(stack, workspace, count, comparison_count, interval=interval, timeout=timeout)
    stack.http_samples.clear()
    meter.start()
    import_finished: float | None = None
    first_request_submitted: float | None = None
    try:
        if existing is None:
            imported = stack.request("POST", f"/workspaces/{workspace}/imports", {"directory": str(directory), "limit": count})
            artifact_ids = imported["artifact_ids"]
            if imported["imported_images"] != count or len(artifact_ids) != count:
                raise AssertionError("Image import did not match selected dataset")
            import_finished = time.perf_counter() - meter.started
        if comparison_count:
            for key in artifact_ids:
                created = stack.request("POST", f"/comparisons/{key}", {"retained_max_k": top_k, "max_distance": max_distance})
                request_ids.append(int(created["id"]))
                if first_request_submitted is None:
                    first_request_submitted = time.perf_counter() - meter.started
        submission_finished = time.perf_counter() - meter.started
        metrics = meter.finish()
    finally:
        meter.cancel()
    metrics["submission_seconds"] = submission_finished
    metrics["import_response_seconds"] = import_finished
    metrics["first_comparison_submitted_seconds"] = first_request_submitted
    metrics["post_submission_completion_seconds"] = max(0, metrics["wall_seconds"] - submission_finished)
    metrics["submission_http_latency_ms"] = distribution([duration for _, duration in stack.http_samples], scale=1000)
    metrics["requests_submitted_before_builds_finished"] = (
        first_request_submitted is not None and metrics["builds_finished_seconds"] is not None
        and first_request_submitted < metrics["builds_finished_seconds"]
    )
    metrics["images_per_second"] = count / metrics["wall_seconds"] if existing is None else None
    checked = validate(stack, workspace, artifact_ids, request_ids, top_k=top_k, max_distance=max_distance)
    row = {"backend": stack.backend, "scenario": kind, "status": "passed", "images": count,
           "comparison_requests": comparison_count, "expected_pairs": comparison_count * (count - 1),
           "page_size": page_size, "workers": {"build": WORKERS, "comparison": WORKERS},
           "metrics": metrics, "validation": checked, "sql_database_bytes": stack.database.stat().st_size}
    return row, (workspace, artifact_ids)


def write_results(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.part")
    temporary.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--images", type=int, default=100)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--warmup-images", type=int, default=8)
    parser.add_argument("--page-size", type=int, default=250)
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--max-distance", type=int, default=10)
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DIRECTORY)
    parser.add_argument("--download", action="store_true", help="Download/check selected images before starting any stack")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--work-dir", type=Path, help="New directory for private DBs, logs, Redis data and input links")
    parser.add_argument("--dbwork-python", type=Path)
    parser.add_argument("--celery-python", type=Path)
    parser.add_argument("--redis-server", default="redis-server")
    parser.add_argument("--poll-interval", type=float, default=.1)
    parser.add_argument("--timeout-seconds", type=float, default=300)
    args = parser.parse_args(argv)
    if not 2 <= args.images <= 25000 or args.repetitions < 1:
        parser.error("images must be 2–25000 and repetitions must be positive")
    if not 0 <= args.warmup_images <= args.images or args.warmup_images == 1:
        parser.error("warmup-images must be 0 or between 2 and images")
    if args.page_size < 1 or not 1 <= args.top_k <= 100 or not 0 <= args.max_distance <= 64:
        parser.error("invalid page size, top-K or Hamming threshold")
    if args.poll_interval <= 0 or args.timeout_seconds <= 0:
        parser.error("poll interval and timeout must be positive")
    return args


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    output = (args.output or PROJECT / "results" / f"{stamp}.json").resolve()
    work_dir = args.work_dir.resolve() if args.work_dir else Path(tempfile.mkdtemp(prefix="imagededup-benchmark-"))
    if args.work_dir:
        work_dir.mkdir(parents=True, exist_ok=False)
    dataset_dir = args.dataset_dir.expanduser().resolve()
    if args.download:
        download_dataset(dataset_dir, args.images)
    files = select_images(dataset_dir, args.images)
    inputs = work_dir / "inputs"
    manifest = prepare_images(files, inputs)
    warmup = work_dir / "warmup"
    if args.warmup_images:
        prepare_images(files[:args.warmup_images], warmup)
    report: dict[str, Any] = {
        "schema_version": 1, "created_at": datetime.now(UTC).isoformat(), "status": "running",
        "host": {"platform": platform.platform(), "machine": platform.machine(), "python": platform.python_version(),
                 "logical_cpus": psutil.cpu_count(), "physical_cpus": psutil.cpu_count(logical=False),
                 "memory_bytes": psutil.virtual_memory().total, "sqlite_version": sqlite3.sqlite_version},
        "configuration": {"images": args.images, "repetitions": args.repetitions, "warmup_images": args.warmup_images,
                          "build_workers": WORKERS, "comparison_workers": WORKERS, "page_size": args.page_size,
                          "top_k": args.top_k, "max_distance": args.max_distance, "poll_interval": args.poll_interval,
                          "timeout_seconds": args.timeout_seconds, "sqlite_journal_mode": "delete",
                          "redis_appendonly": True, "redis_appendfsync": "everysec",
                          "hamming_kernel": "integer_xor_bit_count", "scientific_threads_per_process": 1,
                          "sequential": True, "order": "alternating backend order between repetitions"},
        "measurement_notes": [
            "Each repetition starts fresh stacks and SQL databases; scenarios use isolated workspaces.",
            "Warm-up, service startup, dataset preparation, final correctness checks and shutdown are untimed.",
            "Timed work includes API submissions, SQL and broker overhead, and completion observation latency.",
            "Progress uses read-only aggregate queries against common SQL data tables; API probes request one artifact.",
            "RSS is summed across all processes and can double-count shared pages; CPU is summed across process trees.",
            "Redis/Celery includes Redis, worker parents, children, API and Beat; no existing Redis instance is touched.",
            "SQLite durability is the same for both apps; Redis AOF everysec is an additional, different durability boundary.",
            "Mixed overlap is observed, not forced: small workloads may finish builds before scoring starts.",
        ],
        "dataset": {"directory": str(dataset_dir), "name": "MIRFLICKR-25K", "selection": manifest},
        "work_directory": str(work_dir), "stacks": [], "runs": [],
    }
    write_results(output, report)
    try:
        for repetition in range(args.repetitions):
            order = ("dbwork", "redis_celery") if repetition % 2 == 0 else ("redis_celery", "dbwork")
            for backend in order:
                print(f"[{repetition + 1}/{args.repetitions}] {backend}: starting isolated stack", flush=True)
                python = args.dbwork_python if backend == "dbwork" else args.celery_python
                with Stack(backend, work_dir / f"{repetition + 1}-{backend}", page_size=args.page_size,
                           python=python, redis_server=args.redis_server) as stack:
                    report["stacks"].append({"repetition": repetition + 1, "backend": backend,
                                             "startup_seconds": stack.startup_seconds, "logs": str(stack.directory), "versions": stack.versions})
                    common = dict(page_size=args.page_size, top_k=args.top_k, max_distance=args.max_distance,
                                  interval=args.poll_interval, timeout=args.timeout_seconds)
                    if args.warmup_images:
                        scenario(stack, "warmup", warmup, args.warmup_images, **common)
                    built: tuple[int, list[int]] | None = None
                    for kind in ("build", "comparison", "mixed"):
                        print(f"  {kind}: {args.images} images", flush=True)
                        row, workspace = scenario(stack, kind, inputs, args.images,
                                                  existing=built if kind == "comparison" else None, **common)
                        row["repetition"] = repetition + 1
                        report["runs"].append(row)
                        if kind == "build":
                            built = workspace
                        write_results(output, report)
                        print(f"  {kind}: {row['metrics']['wall_seconds']:.3f}s, validation passed", flush=True)
        # Exact dataset order makes these normalized results comparable across
        # independent database IDs and all repetitions.
        for kind in ("build", "comparison", "mixed"):
            results = [row["validation"] for row in report["runs"] if row["scenario"] == kind]
            if any(result != results[0] for result in results[1:]):
                raise AssertionError(f"Backends/repetitions produced different {kind} results")
        report["status"] = "passed"
        report["summary"] = {
            kind: {backend: distribution([row["metrics"]["wall_seconds"] for row in report["runs"]
                                         if row["backend"] == backend and row["scenario"] == kind])
                   for backend in ("dbwork", "redis_celery")}
            for kind in ("build", "comparison", "mixed")
        }
    except BaseException as exc:
        report["status"] = "failed"
        report["error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        report["finished_at"] = datetime.now(UTC).isoformat()
        write_results(output, report)
    print(f"Results: {output}", flush=True)


if __name__ == "__main__":
    main()
