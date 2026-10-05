"""Propose a validated complete report from the trusted performance workflow."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from benchmarks.render_results import render


def run(*command: str) -> None:
    subprocess.run(list(command), cwd=ROOT, check=True)


def main() -> None:
    directory = (ROOT / os.environ["BENCHMARK_RESULT_DIRECTORY"]).resolve()
    branch = os.environ["BENCHMARK_RESULTS_BRANCH"]
    if directory.parent != ROOT / "benchmarks/results" or not branch.startswith("benchmark-results/"):
        raise ValueError("Invalid workflow result directory or branch")
    report = ROOT / "doc/benchmark-results.md"
    expected = render(directory, report)
    if report.read_text() != expected:
        raise ValueError("The report differs from its validated JSON inputs")
    staged = subprocess.check_output(["git", "diff", "--cached", "--name-only"], cwd=ROOT, text=True)
    if staged.strip():
        raise ValueError("Report publication requires an empty staging area")
    index = json.loads((directory / "index.json").read_text())
    paths = [report, directory / "index.json"]
    paths += [directory / entry["path"] for entry in index["reports"]]
    # Include the raw image report referenced by the suite JSON, never private
    # databases, environments, arbitrary logs or unrelated working-tree files.
    raw = directory / "imagededup.raw.json"
    if raw.is_file():
        paths.append(raw)
    run("git", "check-ref-format", "--branch", branch)
    run("git", "checkout", "-b", branch)
    run("git", "config", "user.name", "github-actions[bot]")
    run("git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com")
    run("git", "add", "--", *(str(path.relative_to(ROOT)) for path in paths))
    run("git", "commit", "-m", "docs: update validated application benchmark results")
    run("git", "push", "origin", branch)
    with tempfile.TemporaryDirectory() as temporary:
        body = Path(temporary) / "body.md"
        body.write_text("Update the application performance report from a complete fixed-runner experiment.\n\n"
                        f"Run: `{index['run_id']}`. All registered suites passed output and report validation.\n"
                        "The committed JSON files retain source pins, interpreter details and individual samples.\n")
        run("gh", "pr", "create", "--head", branch, "--title", "Update application benchmark results", "--body-file", str(body))


if __name__ == "__main__":
    main()
