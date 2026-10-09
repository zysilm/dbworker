"""Deterministically render validated suite JSON into Markdown."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from benchmarks.common.reporting import digest, summarize, validate_report


def escape(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def workload_label(report: dict, scenario: str) -> str:
    """Name actual measured business quantities, not broad repository labels."""
    profile = report.get("configuration", {}).get("profile", {})
    rows = [row for row in report["runs"] if row["scenario"] == scenario]
    config = rows[0].get("configuration", {}) if rows else {}
    count = profile.get("requests", config.get("profile", {}).get("requests"))
    if count is None:
        count = rows[0].get("validation", {}).get("workflow", {}).get("operations") if rows else None
        if isinstance(count, list):
            count = len(count)
    suite = report["suite_id"]
    if suite == "imagededup":
        images = report.get("configuration", {}).get("images", rows[0].get("images", 0))
        if scenario == "build":
            return f"Build {images:,} image hashes (one bulk import)"
        pairs = images * (images - 1)
        prefix = "Build + compare" if scenario == "mixed" else "Compare"
        return f"{prefix} {images:,} images / {pairs:,} directed pairs"
    if not isinstance(count, int):
        return scenario
    labels = {
        "superset": f"SQL Lab: {count:,} queries / 10,000 rows",
        "saleor": f"Export: {count:,} x {profile.get('products', 256)} products + {count:,} emails",
        "paperless_ngx": f"OCR: {count:,} scans, archive and index",
        "posthog": f"2FA: {count:,} requests / {count * 2:,} tasks",
        "sentry": f"Email: {count:,} requests / {count * 2:,} deliveries",
    }
    return labels.get(suite, scenario)


def load_cell(report: dict, scenario: str, backend: str) -> str:
    values = []
    for row in report["runs"]:
        if row["scenario"] != scenario or row["backend"] != backend:
            continue
        measured = row.get("metrics", {}).get("task_load", {})
        latency = measured.get("task_end_to_end", {}).get("p95_seconds")
        peak = measured.get("peak_published_outstanding")
        if latency is not None and peak is not None:
            values.append((latency, peak))
    if not values:
        return "n/a"
    return f"{statistics.median(v[0] for v in values):.3f}s / {statistics.median(v[1] for v in values):.0f}"


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
            lines.append(f"| {escape(workload_label(report, summary['scenario']))} | {escape(summary['comparison_mode'])} | {c['median_wall_seconds']:.6f} | {c['min_wall_seconds']:.6f}–{c['max_wall_seconds']:.6f} | {d['median_wall_seconds']:.6f} | {d['min_wall_seconds']:.6f}–{d['max_wall_seconds']:.6f} | {ratio:.3f} | {c['count']} |")
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


def update_readme(run_dir: Path, readme: Path) -> str:
    """Replace the final compact benchmark section using validated full results."""
    render(run_dir, readme)
    index = json.loads((run_dir / "index.json").read_text())
    start, end = "<!-- benchmark-results:start -->", "<!-- benchmark-results:end -->"
    original = readme.read_text()
    if original.count(start) != original.count(end) or original.count(start) > 1:
        raise ValueError("Invalid README benchmark markers")
    lines = [start, "## Benchmark Results", "",
             "Median wall time in seconds; **bold** marks the faster backend. "
             "Each experiment runs in a fresh Docker container on its own GitHub-hosted Ubuntu VM. Fixed profile: 8 producers, a 60-second scheduled submission window and 8 total execution slots; image hash building uses one native bulk import. Three repetitions per backend. Actual schedule delays and backlog are recorded in JSON; this is one load point, not a maximum-capacity search.", "",
             "| Experiment | Workload | Celery wall (s) | DBWorker wall (s) | C / D | Task P95 / peak outstanding (C; D) |",
             "|---|---|---:|---:|---:|---|"]
    for entry in index["reports"]:
        report = json.loads((run_dir / entry["path"]).read_text())
        for summary in summarize(report["runs"]).values():
            celery = summary["backends"]["celery"]["median_wall_seconds"]
            dbworker = summary["backends"]["dbworker"]["median_wall_seconds"]
            c, d = f"{celery:.3f}", f"{dbworker:.3f}"
            if celery < dbworker:
                c = f"**{c}**"
            elif dbworker < celery:
                d = f"**{d}**"
            lines.append(f"| {escape(entry['suite_id'])} | {escape(workload_label(report, summary['scenario']))} | {c} | {d} | {celery / dbworker:.2f} | {load_cell(report, summary['scenario'], 'celery')}; {load_cell(report, summary['scenario'], 'dbworker')} |")
    detail = Path(os.path.relpath(ROOT / "doc/benchmark-results.md", readme.parent)).as_posix()
    raw = Path(os.path.relpath(run_dir / "index.json", readme.parent)).as_posix()
    lines += ["", f"Run: `{escape(index['run_id'])}`. [Details and scope]({detail}) · [JSON results]({raw}).",
              "Scoped application workloads; Sentry uses historical 24.1.0. "
              "Wall time includes the fixed submission window, so a ratio near 1 does not prove equal processing capacity. Task P95 starts at publication; peak outstanding counts already published tasks. Ratios above 1 favor DBWorker; no cross-project average is computed.", end]
    section = "\n".join(lines)
    if start in original:
        before, remainder = original.split(start, 1)
        _, after = remainder.split(end, 1)
        if after.strip():
            raise ValueError("The benchmark section must be the final README chapter")
        return before.rstrip() + "\n\n" + section + "\n"
    return original.rstrip() + "\n\n" + section + "\n"


if __name__ == "__main__":
    main()
