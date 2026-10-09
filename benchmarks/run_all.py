"""Run registered application experiments in independent virtual environments."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

if __name__ == "__main__":
    try:
        import psutil
    except ModuleNotFoundError:
        # Keep the public entry point usable from a plain Python installation.
        uv = shutil.which("uv")
        if uv is None:
            raise SystemExit("Install uv to provision the private benchmark orchestration environment")
        environment = ROOT / "benchmarks/environments/orchestrator/.venv"
        subprocess.run([uv, "venv", "--allow-existing", "--python", "3.12", str(environment)], check=True)
        interpreter = environment / "bin/python"
        subprocess.run([uv, "pip", "install", "--python", str(interpreter), "-r", str(ROOT / "benchmarks/requirements.txt")], check=True)
        os.execv(str(interpreter), [str(interpreter), *sys.argv])

from benchmarks.common.processes import run_command
from benchmarks.common.reporting import digest, new_report, summarize, timestamp, validate_report, write_json
from benchmarks.common.performance_admission import validate_native_report
from benchmarks.common.native_admission import check_worker_source


def resolve(value: str, root: Path = ROOT) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else root / path


def git(*args: str, cwd: Path = ROOT) -> str:
    return subprocess.check_output(["git", *args], cwd=cwd, text=True, stderr=subprocess.PIPE).strip()


def run_suite(suite: dict[str, Any], *, output: Path, run_id: str, profile: str,
              overrides: dict[str, Any], timeout: float, provision_environments: bool = False,
              uv: str = "uv") -> dict[str, Any]:
    suite_id = suite["suite_id"]
    report_path = output / f"{suite_id}.json"
    report = new_report(suite_id, run_id, profile)
    report["source"] = {"local_commit": git("rev-parse", "HEAD"), "repository": suite["repository"],
                        "expected_commit": suite.get("commit"), "historical": suite.get("historical", False),
                        "comparison_contract": suite.get("comparison_contract")}
    report["environment"] = {"orchestrator_python": sys.version, "platform": platform.platform()}
    report["environment"]["runner_class"] = os.environ.get("BENCHMARK_RUNNER_CLASS", "local-unspecified")
    files = [ROOT / suite["entrypoint"], ROOT / "src/dbworker.py"]
    files += [ROOT / "benchmarks/run_all.py", ROOT / "benchmarks/registry.json",
              ROOT / "benchmarks/provision.py", ROOT / "benchmarks/Dockerfile",
              ROOT / "benchmarks/requirements.txt"]
    files += sorted((ROOT / "benchmarks/locks").glob(f"{suite_id}*.txt"))
    files += sorted((ROOT / "benchmarks/common").glob("*.py"))
    files += sorted((ROOT / "examples/dbworker_integration").glob("*.py"))
    files += sorted((ROOT / f"examples/{suite_id}_dbworker").glob("*.py"))
    if suite_id == "imagededup":
        files += sorted((ROOT / "benchmarks/imagededup_benckmark/src").rglob("*.py"))
        for example in ("imagededup_system_redis_celery", "imagededup_system_dbwork"):
            files += sorted((ROOT / "examples" / example / "src").rglob("*.py"))
    else:
        files += sorted((ROOT / "benchmarks/upstream").glob("*.py"))
    report["source"]["implementation_sha256"] = {
        str(path.relative_to(ROOT) if path.is_relative_to(ROOT) else path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in files if path.is_file()
    }
    write_json(report_path, report)
    baseline = json.loads(json.dumps(report))
    try:
        upstream = resolve(suite["source_path"])
        if suite.get("commit"):
            actual = git("rev-parse", "HEAD", cwd=upstream)
            report["source"]["commit"] = actual
            report["source"]["dirty"] = bool(git("status", "--porcelain", cwd=upstream))
            if actual != suite["commit"] or report["source"]["dirty"]:
                raise ValueError("Upstream checkout must be clean and match the registered commit")
        else:
            report["source"]["commit"] = report["source"]["local_commit"]
        if suite.get("comparison_contract") == "native-business-workflow-v1":
            worker_source = (ROOT / "benchmarks/imagededup_benckmark/src/imagededup_benckmark/runtime.py"
                             if suite_id == "imagededup" else ROOT / f"benchmarks/upstream/{suite_id}_backend.py")
            report["source"]["native_worker_admission"] = check_worker_source(worker_source.read_text(), suite_id)
        if provision_environments:
            provision_log = output / f"{suite_id}.provision.log"
            report["artifacts"]["provision_log"] = provision_log.name
            print(f"{suite_id}: provisioning independent environments", flush=True)
            code = run_command([sys.executable, str(ROOT / "benchmarks/provision.py"),
                                "--suite", suite_id, "--uv", uv], cwd=ROOT,
                               env=os.environ.copy(), log=provision_log, timeout=max(timeout, 3600))
            if code:
                raise RuntimeError(f"Environment provisioning failed with exit code {code}; see {provision_log.name}")
        interpreters = {**suite["interpreters"], **overrides.get(suite_id, {})}
        interpreters = {key: str(resolve(value)) for key, value in interpreters.items()}
        for key in ("benchmark_python", "celery_python", "dbworker_python"):
            if not Path(interpreters[key]).is_file():
                raise FileNotFoundError(f"Missing {key}: {interpreters[key]}; provision this suite's venv first")
        report["environment"]["interpreters"] = interpreters
        config = {"suite": suite, "profile": profile, "run_id": run_id, "interpreters": interpreters,
                  "repository_root": str(ROOT), "output_directory": str(output), "report_path": str(report_path)}
        config_path = output / f"{suite_id}.config.json"
        write_json(config_path, config)
        write_json(report_path, report)
        baseline = json.loads(json.dumps(report))
        env = os.environ.copy()
        env["PYTHONPATH"] = str(ROOT) + os.pathsep + str(ROOT / "src")
        log = output / f"{suite_id}.log"
        print(f"{suite_id}: starting {profile} experiment", flush=True)
        code = run_command([interpreters["benchmark_python"], str(resolve(suite["entrypoint"])),
                            "--config", str(config_path)], cwd=ROOT, env=env, log=log, timeout=timeout)
        report = json.loads(report_path.read_text())
        if (report.get("suite_id"), report.get("run_id"), report.get("profile")) != (suite_id, run_id, profile):
            raise ValueError("Suite returned a different report identity")
        if report.get("source") != baseline["source"]:
            report["source"] = baseline["source"]
            raise ValueError("Suite changed the parent-recorded source evidence")
        for name, expected in baseline["source"]["implementation_sha256"].items():
            path = resolve(name)
            if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise ValueError(f"Benchmark source changed during the experiment: {name}")
        report["artifacts"]["suite_log"] = log.name
        if code:
            report["status"] = "failed" if report["status"] != "blocked" else "blocked"
            report["errors"].append({"phase": "execution", "type": "ExitCode", "message": str(code)})
        validate_report(report)
        if suite.get("comparison_contract") == "native-business-workflow-v1":
            validate_native_report(report, output, expected_profile=suite["profiles"][profile])
        report["summary"] = summarize(report["runs"])
    except Exception as exc:
        # Preserve malformed child output as evidence, but keep the public
        # suite report valid even for NaN, wrong identities or missing fields.
        try:
            if not isinstance(report, dict):
                raise ValueError("Suite report must be an object")
            report["status"] = "failed"
            validate_report(report)
            if (report["suite_id"], report["run_id"], report["profile"]) != (suite_id, run_id, profile):
                raise ValueError("Wrong child identity")
        except Exception:
            invalid = output / f"{suite_id}.invalid.json"
            if report_path.exists():
                invalid.write_bytes(report_path.read_bytes())
            report = baseline
            report["artifacts"]["invalid_report"] = invalid.name
        report["status"] = "blocked" if isinstance(exc, FileNotFoundError) else "failed"
        report["errors"].append({"phase": "orchestration", "type": type(exc).__name__, "message": str(exc)})
    finally:
        report["finished_at"] = timestamp()
        write_json(report_path, report)
    print(f"{suite_id}: {report['status']}", flush=True)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("full",), default="full")
    parser.add_argument("--run-id", help="Shared identity for isolated CI matrix jobs")
    parser.add_argument("--registry", type=Path, default=ROOT / "benchmarks/registry.json")
    parser.add_argument("--interpreters", type=Path, help="JSON mapping suite IDs to interpreter overrides")
    parser.add_argument("--suite", action="append", help="Explicit partial suite selection; repeatable")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--timeout-seconds", type=float, default=7200)
    parser.add_argument("--provision", action="store_true", help="Install independent suite environments before running experiments")
    parser.add_argument("--uv", default="uv", help="Environment provisioner executable")
    args = parser.parse_args(argv)
    registry = json.loads(args.registry.read_text())
    suites = registry["suites"]
    identifiers = [s["suite_id"] for s in suites]
    if registry.get("schema_version") != 1 or not identifiers or len(set(identifiers)) != len(identifiers) or any(
            not re.fullmatch(r"[a-z][a-z0-9_]*", name) for name in identifiers):
        parser.error("Invalid registry schema or duplicate suites")
    if args.suite and set(args.suite) - set(identifiers):
        parser.error("Unknown suite selection")
    if args.timeout_seconds <= 0:
        parser.error("Timeout must be positive")
    selected = [s for s in suites if not args.suite or s["suite_id"] in args.suite]
    run_id = args.run_id or datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        parser.error("Invalid run identity")
    output = (args.output_dir or ROOT / "benchmarks/results" / run_id).resolve()
    output.mkdir(parents=True, exist_ok=False)
    overrides = json.loads(args.interpreters.read_text()) if args.interpreters else {}
    index: dict[str, Any] = {"schema_version": 1, "run_id": run_id, "profile": args.profile,
                             "status": "running", "complete_selection": len(selected) == len(suites),
                             "required_suites": identifiers, "reports": [], "created_at": timestamp()}
    write_json(output / "index.json", index)
    try:
        for suite in selected:
            report = run_suite(suite, output=output, run_id=run_id, profile=args.profile,
                               overrides=overrides, timeout=args.timeout_seconds,
                               provision_environments=args.provision, uv=args.uv)
            path = output / f"{suite['suite_id']}.json"
            index["reports"].append({"suite_id": suite["suite_id"], "path": path.name,
                                     "status": report["status"], "sha256": digest(path)})
            write_json(output / "index.json", index)
        index["status"] = "passed" if all(r["status"] == "passed" for r in index["reports"]) else "failed"
    except BaseException:
        index["status"] = "failed"
        raise
    finally:
        index["finished_at"] = timestamp()
        write_json(output / "index.json", index)
    if index["status"] == "passed":
        from benchmarks.render_results import render
        markdown = output / "report.md"
        markdown.write_text(render(output, markdown, allow_partial=args.profile != "full" or not index["complete_selection"]), encoding="utf-8")
    print(f"Results: {output}")
    return 0 if index["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
