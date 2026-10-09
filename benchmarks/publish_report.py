"""Publish validated full results and the final compact README section to main."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmarks.render_results import render, update_readme
from benchmarks.ci_results import publication_evidence, safe_evidence_path
from benchmarks.common.performance_admission import validate_native_report


def run(*command: str) -> None:
    subprocess.run(list(command), cwd=ROOT, check=True)


def stage_publication_evidence(directory: Path, paths: list[Path], suite_ids: list[str]) -> None:
    """Stage new receipts and obsolete tracked receipts without staging runtime files."""
    if directory != ROOT / "benchmarks/results/latest":
        raise ValueError("Publication requires the fixed latest result directory")
    deleted = subprocess.check_output(
        ["git", "ls-files", "--deleted", "-z", "--", "benchmarks/results/latest"],
        cwd=ROOT,
    ).decode().split("\0")
    top_level = {"index.json", "report.md", "imagededup.raw.json", *(f"{suite}.json" for suite in suite_ids)}
    receipts = {"sample.json", "smtp-evidence.json", "output-evidence.json", "sql-results-evidence.json"}
    obsolete = []
    for name in filter(None, deleted):
        path = safe_evidence_path(directory, ROOT / name)
        relative = path.relative_to(directory)
        if ((len(relative.parts) == 1 and relative.name in top_level)
                or relative.name in receipts or relative.suffix == ".jsonl"):
            obsolete.append(name)
    # Force-add only the explicit new publication allowlist, including ignored receipts.
    run("git", "add", "-f", "--", *(str(path.relative_to(ROOT)) for path in paths))
    if obsolete:
        run("git", "add", "-u", "--", *obsolete)


def main() -> None:
    directory = (ROOT / os.environ["BENCHMARK_RESULT_DIRECTORY"]).resolve()
    if directory != ROOT / "benchmarks/results/latest":
        raise ValueError("Publication requires the fixed latest result directory")
    index = json.loads((directory / "index.json").read_text())
    registry = json.loads((ROOT / "benchmarks/registry.json").read_text())["suites"]
    if {entry["suite_id"] for entry in index["reports"]} != {suite["suite_id"] for suite in registry}:
        raise ValueError("Publication requires every native suite")
    for suite in registry:
        result = json.loads((directory / f"{suite['suite_id']}.json").read_text())
        if result.get("source", {}).get("comparison_contract") != "native-business-workflow-v1":
            raise ValueError("Historical wrapper results cannot be published as native results")
        validate_native_report(result, directory, expected_profile=suite["profiles"]["full"])
    report = ROOT / "doc/benchmark-results.md"
    content = render(directory, report)
    readme = ROOT / "README.md"
    compact = update_readme(directory, readme)
    staged = subprocess.check_output(["git", "diff", "--cached", "--name-only"], cwd=ROOT, text=True)
    if staged.strip():
        raise ValueError("Publication requires an empty staging area")
    run("git", "fetch", "origin", "main")
    current = subprocess.check_output(["git", "rev-parse", "origin/main"], cwd=ROOT, text=True).strip()
    source = os.environ["BENCHMARK_SOURCE_COMMIT"]
    if current != source:
        print("Main advanced during the benchmark; skipping obsolete result publication.")
        return
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if head != source:
        raise ValueError("Publication checkout differs from the measured revision")
    report.write_text(content)
    readme.write_text(compact)
    # Stage only allowlisted replay evidence, including SMTP, image and SQL result receipts.
    # Logs, configs, media and databases stay in artifacts.
    paths = [report, readme, *publication_evidence(directory)]
    run("git", "config", "user.name", "github-actions[bot]")
    run("git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
    stage_publication_evidence(directory, paths, [suite["suite_id"] for suite in registry])
    run("git", "diff", "--cached", "--check")
    run("git", "commit", "-m", "docs: update full application benchmark results [skip ci]")
    # A normal push rejects races and honors main branch protection.
    run("git", "push", "origin", "HEAD:main")


if __name__ == "__main__":
    main()
