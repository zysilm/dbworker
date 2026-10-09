"""Package matrix evidence and combine complete full runs for publication."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from benchmarks.common.reporting import digest, timestamp, validate_report, write_json
from benchmarks.common.performance_admission import validate_native_report
from benchmarks.render_results import render


def safe_evidence_path(root: Path, path: Path) -> Path:
    """Reject evidence that escapes its directory through traversal or symlinks."""
    root = root.absolute()
    if root.is_symlink():
        raise ValueError("Symlink evidence directory is not permitted")
    path = path.absolute()
    try:
        relative = path.relative_to(root)
    except ValueError as error:
        raise ValueError("Evidence path escapes its directory") from error
    if not relative.parts or any(part in (".", "..") for part in relative.parts):
        raise ValueError("Unsafe evidence path")
    current = root
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise ValueError("Symlink evidence is not permitted")
    if path.resolve() != root.resolve() / relative:
        raise ValueError("Evidence path escapes its directory")
    return path


def publication_evidence(directory: Path) -> list[Path]:
    """Select only persisted replay receipts; runtime files are never published."""
    paths = [directory / "index.json", directory / "report.md"]
    index = json.loads((directory / "index.json").read_text())
    for entry in index["reports"]:
        relative = Path(entry["path"])
        if relative.is_absolute() or len(relative.parts) != 1 or relative.suffix != ".json":
            raise ValueError("Published report path must be a JSON basename")
        paths.append(directory / relative)
    for pattern in ("sample.json", "smtp-evidence.json", "*.jsonl"):
        paths += sorted(directory.rglob(pattern))
    if (directory / "imagededup.raw.json").is_file():
        paths.append(directory / "imagededup.raw.json")
    return [safe_evidence_path(directory, path) for path in paths]


def package(source: Path, output: Path, suite: str) -> None:
    """Retain report evidence and logs, excluding databases, media and configs."""
    output.mkdir(parents=True, exist_ok=False)
    paths = [source / "index.json", source / f"{suite}.json"]
    if suite == "imagededup":
        paths.append(source / "imagededup.raw.json")
    paths += sorted(source.rglob("sample.json"))
    paths += sorted(source.rglob("smtp-evidence.json"))
    paths += sorted(source.rglob("*.log"))
    paths += sorted(source.rglob("*.jsonl"))
    paths += sorted(source.rglob("admission.json"))
    for path in paths:
        safe_evidence_path(source, path)
        if path.is_file():
            target = output / path.relative_to(source)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)


def combine(incoming: Path, output: Path, *, run_id: str, commit: str,
            registry: Path = ROOT / "benchmarks/registry.json") -> None:
    suites = json.loads(registry.read_text())["suites"]
    identifiers = [suite["suite_id"] for suite in suites]
    if set(p.name for p in incoming.iterdir() if p.is_dir()) != set(identifiers):
        raise ValueError("Matrix artifacts must contain exactly every registered suite")
    index = {"schema_version": 1, "run_id": run_id, "profile": "full", "status": "passed",
             "complete_selection": True, "required_suites": identifiers,
             "reports": [], "created_at": timestamp()}
    validated = []
    for suite in suites:
        name = suite["suite_id"]
        directory = safe_evidence_path(incoming, incoming / name)
        safe_evidence_path(directory, directory / "index.json")
        part = json.loads((directory / "index.json").read_text())
        if (part.get("run_id"), part.get("profile"), part.get("status")) != (run_id, "full", "passed"):
            raise ValueError(f"Invalid matrix index identity: {name}")
        if part.get("required_suites") != identifiers or len(part["reports"]) != 1:
            raise ValueError(f"Invalid matrix coverage: {name}")
        entry = part["reports"][0]
        path = safe_evidence_path(directory, directory / f"{name}.json")
        if (entry["suite_id"], entry["path"], entry["status"], entry["sha256"]) != (
                name, path.name, "passed", digest(path)):
            raise ValueError(f"Matrix checksum or status mismatch: {name}")
        report = json.loads(path.read_text())
        validate_report(report)
        if suite.get("comparison_contract") == "native-business-workflow-v1":
            if report.get("source", {}).get("comparison_contract") != suite["comparison_contract"]:
                raise ValueError("Missing native benchmark source contract")
            validate_native_report(report, directory, expected_profile=suite["profiles"]["full"])
        if (report["suite_id"], report["run_id"], report["profile"], report["status"]) != (
                name, run_id, "full", "passed"):
            raise ValueError(f"Matrix report identity mismatch: {name}")
        if report["source"].get("local_commit") != commit:
            raise ValueError(f"Matrix source revision mismatch: {name}")
        if suite.get("commit") and report["source"].get("commit") != suite["commit"]:
            raise ValueError(f"Upstream source revision mismatch: {name}")
        profile = report["configuration"].get("profile")
        if name == "imagededup":
            profile = {key: report["configuration"].get(key) for key in suite["profiles"]["full"]}
        if profile != suite["profiles"]["full"]:
            raise ValueError(f"Matrix workload differs from the full profile: {name}")
        if report["environment"].get("runner_class") != "github-hosted-docker":
            raise ValueError(f"Unexpected matrix environment: {name}")
        validated.append((directory, path, entry))
    output.mkdir(parents=True, exist_ok=False)
    for directory, path, entry in validated:
        shutil.copyfile(path, output / path.name)
        for evidence in [directory / "imagededup.raw.json", *directory.rglob("sample.json"), *directory.rglob("smtp-evidence.json"), *directory.rglob("*.jsonl")]:
            safe_evidence_path(directory, evidence)
            if evidence.is_file():
                target = output / evidence.relative_to(directory)
                target.parent.mkdir(parents=True, exist_ok=True)
                if target.exists():
                    raise ValueError("Conflicting matrix evidence paths")
                shutil.copyfile(evidence, target)
        index["reports"].append(entry)
    index["finished_at"] = timestamp()
    write_json(output / "index.json", index)
    (output / "report.md").write_text(render(output, output / "report.md"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    packing = commands.add_parser("package")
    packing.add_argument("--source", type=Path, required=True)
    packing.add_argument("--output", type=Path, required=True)
    packing.add_argument("--suite", required=True)
    merging = commands.add_parser("combine")
    merging.add_argument("--incoming", type=Path, required=True)
    merging.add_argument("--output", type=Path, required=True)
    merging.add_argument("--run-id", required=True)
    merging.add_argument("--commit", required=True)
    args = parser.parse_args()
    if args.command == "package":
        package(args.source, args.output, args.suite)
    else:
        combine(args.incoming, args.output, run_id=args.run_id, commit=args.commit)


if __name__ == "__main__":
    main()
