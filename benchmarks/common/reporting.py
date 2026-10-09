"""Versioned, atomic suite reports and comparable sample summaries."""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

STATUSES = {"running", "passed", "failed", "blocked", "skipped"}
BACKENDS = {"celery", "dbworker"}


def timestamp() -> str:
    return datetime.now(UTC).isoformat()


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    compact = json.dumps(value, sort_keys=True, allow_nan=False, separators=(",", ":"))
    # Keep small reports readable; avoid multiplying large replay receipts with
    # indentation. Every field remains present for independent admission.
    payload = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False)
               if len(compact) < 1_000_000 else compact) + "\n"
    temporary = path.with_name(path.name + ".part")
    temporary.write_text(payload, encoding="utf-8")
    temporary.replace(path)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def new_report(suite: str, run_id: str, profile: str) -> dict[str, Any]:
    return {
        "schema_version": 1, "suite_id": suite, "run_id": run_id,
        "profile": profile, "status": "running", "created_at": timestamp(),
        "source": {}, "environment": {}, "configuration": {}, "dataset": {},
        "runs": [], "summary": {}, "capabilities": {}, "errors": [], "artifacts": {},
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    groups: dict[tuple[str, str], dict[str, list[float]]] = {}
    for row in rows:
        if row["status"] != "passed" or not row["validation"].get("passed"):
            continue
        key = (row["scenario"], row["comparison_mode"])
        groups.setdefault(key, {}).setdefault(row["backend"], []).append(row["metrics"]["wall_seconds"])
    result: dict[str, Any] = {}
    for (scenario, mode), backends in sorted(groups.items()):
        stats = {
            backend: {"count": len(values), "median_wall_seconds": statistics.median(values),
                      "min_wall_seconds": min(values), "max_wall_seconds": max(values)}
            for backend, values in sorted(backends.items())
        }
        ratio = None
        if BACKENDS.issubset(stats) and stats["dbworker"]["median_wall_seconds"] > 0:
            ratio = stats["celery"]["median_wall_seconds"] / stats["dbworker"]["median_wall_seconds"]
        result[f"{scenario}:{mode}"] = {
            "scenario": scenario, "comparison_mode": mode, "backends": stats,
            "celery_over_dbworker_wall_ratio": ratio,
        }
    return result


def validate_report(report: dict[str, Any], *, terminal: bool = True) -> None:
    required = ("source", "environment", "configuration", "dataset", "summary", "capabilities", "artifacts")
    if report.get("schema_version") != 1 or not isinstance(report.get("suite_id"), str):
        raise ValueError("Invalid suite schema or identity")
    if not isinstance(report.get("run_id"), str) or report.get("profile") != "full":
        raise ValueError("Invalid run identity or profile")
    if report.get("status") not in STATUSES or terminal and report["status"] == "running":
        raise ValueError("Invalid or unfinished suite status")
    for key in required:
        if not isinstance(report.get(key), dict):
            raise ValueError(f"{key} must be an object")
    if not isinstance(report.get("runs"), list) or not isinstance(report.get("errors"), list):
        raise ValueError("runs and errors must be arrays")
    seen = set()
    for row in report["runs"]:
        if not isinstance(row, dict):
            raise ValueError("Samples must be objects")
        if not isinstance(row.get("scenario"), str) or not isinstance(row.get("comparison_mode"), str):
            raise ValueError("Samples require a scenario and comparison mode")
        if not isinstance(row.get("repetition"), int) or isinstance(row["repetition"], bool) or row["repetition"] < 1:
            raise ValueError("Samples require a positive integer repetition")
        if not isinstance(row.get("metrics"), dict) or not isinstance(row.get("validation"), dict):
            raise ValueError("Sample metrics and validation must be objects")
        identity = (row["scenario"], row["comparison_mode"], row["backend"], row["repetition"])
        if identity in seen:
            raise ValueError(f"Duplicate sample: {identity}")
        seen.add(identity)
        if row["backend"] not in BACKENDS or row["status"] not in STATUSES:
            raise ValueError("Invalid sample backend or status")
        seconds = row["metrics"].get("wall_seconds")
        if row["status"] == "passed":
            if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) or not math.isfinite(seconds) or seconds <= 0:
                raise ValueError("Passed samples require a positive finite wall time")
            if row["validation"].get("passed") is not True:
                raise ValueError("Passed samples require successful validation")
    if report["status"] == "passed":
        if report["errors"] or not report["runs"] or any(row["status"] != "passed" for row in report["runs"]):
            raise ValueError("A passed suite must contain validated samples and no errors")
        scenarios = {(r["scenario"], r["comparison_mode"]) for r in report["runs"]}
        for scenario, mode in scenarios:
            records = [r for r in report["runs"] if (r["scenario"], r["comparison_mode"]) == (scenario, mode)]
            counts = {b: {r["repetition"] for r in records if r["backend"] == b} for b in BACKENDS}
            if not counts["celery"] or counts["celery"] != counts["dbworker"]:
                raise ValueError("Comparable scenarios require both backends and matching repetitions")
            expected = report["configuration"].get("profile", {}).get("repetitions") if isinstance(report["configuration"].get("profile"), dict) else report["configuration"].get("repetitions")
            if expected is not None and (not isinstance(expected, int) or isinstance(expected, bool) or expected < 1 or counts["celery"] != set(range(1, expected + 1))):
                raise ValueError("Passed scenarios must contain every configured repetition")
            fingerprints = {r["validation"].get("output_digest") for r in records}
            if None in fingerprints or len(fingerprints) != 1:
                raise ValueError("Comparable scenario output digests must agree")
    json.dumps(report, allow_nan=False)
