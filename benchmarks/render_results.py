"""Deterministically render validated suite JSON into Markdown."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from benchmarks.common.reporting import digest, summarize, validate_report


def escape(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def render(run_dir: Path, output: Path, *, allow_partial: bool = False) -> str:
    index = json.loads((run_dir / "index.json").read_text())
    if index.get("schema_version") != 1 or index.get("status") != "passed":
        raise ValueError("Only successful validated runs can be rendered")
    if not allow_partial and (index.get("profile") != "full" or not index.get("complete_selection")):
        raise ValueError("Official performance reports require a complete full run")
    entries = index["reports"]
    names = [e["suite_id"] for e in entries]
    if len(names) != len(set(names)) or not names:
        raise ValueError("Invalid suite index")
    if index.get("complete_selection") and set(names) != set(index["required_suites"]):
        raise ValueError("Complete index is missing required suites")
    lines = ["# Application Benchmark Results", "", f"Run: `{escape(index['run_id'])}`", "",
             f"Profile: `{escape(index['profile'])}`", "",
             f"Coverage: {'complete' if index['complete_selection'] else 'partial'}", "",
             "Wall-time ratio is Celery / DBWorker; values above 1 mean DBWorker completed faster.",
             "Workload units differ across projects; no overall throughput average is calculated.", ""]
    if index["profile"] == "smoke":
        lines += ["Smoke runs validate correctness. These timings are not fixed-runner performance baselines.", ""]
    for entry in entries:
        path = (run_dir / entry["path"]).resolve()
        if path.parent != run_dir.resolve() or digest(path) != entry["sha256"]:
            raise ValueError("Suite path or checksum does not match the index")
        report = json.loads(path.read_text())
        validate_report(report)
        if report["status"] != "passed" or (report["suite_id"], report["run_id"], report["profile"]) != (entry["suite_id"], index["run_id"], index["profile"]):
            raise ValueError("Suite identity or status does not match the index")
        relative = Path(os.path.relpath(path, output.parent)).as_posix()
        historical = " (historical)" if report["source"].get("historical") else ""
        lines += [f"## {escape(report['suite_id'])}{historical}", "",
                  f"Source: `{escape(report['source'].get('commit', 'unknown'))}`. [Result JSON]({relative}).", "",
                  "| Scenario | Comparison | Celery median (s) | Celery range (s) | DBWorker median (s) | DBWorker range (s) | Ratio | Samples per backend |",
                  "|---|---|---:|---:|---:|---:|---:|---:|"]
        for summary in summarize(report["runs"]).values():
            backends = summary["backends"]
            c, d = backends["celery"], backends["dbworker"]
            ratio = summary["celery_over_dbworker_wall_ratio"]
            lines.append(f"| {escape(summary['scenario'])} | {escape(summary['comparison_mode'])} | {c['median_wall_seconds']:.6f} | {c['min_wall_seconds']:.6f}–{c['max_wall_seconds']:.6f} | {d['median_wall_seconds']:.6f} | {d['min_wall_seconds']:.6f}–{d['max_wall_seconds']:.6f} | {ratio:.3f} | {c['count']} |")
        lines += ["", "Validation: all reported samples passed and comparable output digests agree.", "",
                  f"Environment: `{escape(json.dumps(report['environment'], sort_keys=True))}`", "",
                  f"Scope: `{escape(json.dumps(report['capabilities'], sort_keys=True))}`", "",
                  f"Configuration: `{escape(json.dumps(report['configuration'], sort_keys=True))}`", ""]
    return "\n".join(lines).rstrip() + "\n"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--allow-partial", action="store_true", help="Render an explicitly labeled diagnostic report")
    args = parser.parse_args(argv)
    payload = render(args.run_dir.resolve(), args.output.resolve(), allow_partial=args.allow_partial)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_name(args.output.name + ".part")
    temporary.write_text(payload, encoding="utf-8")
    temporary.replace(args.output)


if __name__ == "__main__":
    main()
