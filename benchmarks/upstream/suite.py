"""Run admitted upstream experiments; preserve concrete admission failures."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from benchmarks.common.processes import run_command
from benchmarks.common.reporting import summarize, timestamp, validate_report, write_json
from benchmarks.common.performance_admission import validate_native_report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    path = Path(config["report_path"])
    report = json.loads(path.read_text())
    suite = config["suite"]["suite_id"]
    output = Path(config["output_directory"])
    try:
        profile = config["suite"]["profiles"][config["profile"]]
        report["configuration"] = {"profile": profile, "sequential": True, "backend_order": "alternating across repetitions"}
        for repetition in range(1, profile["repetitions"] + 1):
            order = ("celery", "dbworker") if repetition % 2 else ("dbworker", "celery")
            for backend in order:
                directory = output / f"{suite}-{repetition}-{backend}"
                log = output / f"{suite}-{repetition}-{backend}.log"
                code = run_command([config["interpreters"][f"{backend}_python"],
                                    str(ROOT / f"benchmarks/upstream/{suite}_backend.py"),
                                    "--config", str(args.config), "--backend", backend,
                                    "--repetition", str(repetition)], cwd=ROOT, env=os.environ.copy(), log=log,
                                   timeout=max(900, profile["requests"] * 20 + 180))
                if code:
                    raise RuntimeError(f"{backend} exited with {code}; see {log.name}")
                row = json.loads((directory / "sample.json").read_text())
                report["runs"].append(row)
                report["environment"].setdefault("backends", {})[backend] = row["environment"]
                report["configuration"].setdefault("backends", {})[backend] = row.get("configuration", {})
                if "dataset" in row:
                    report["dataset"] = row["dataset"]
                if "capabilities" in row:
                    report["capabilities"] = row["capabilities"]
                report["artifacts"].setdefault("samples", []).append(str((directory / "sample.json").relative_to(output)))
                validate_report(report, terminal=False)
                write_json(path, report)
        report["status"] = "passed"
        validate_report(report)
        if config["suite"].get("comparison_contract") == "native-business-workflow-v1":
            validate_native_report(report, output, expected_profile=profile)
        report["summary"] = summarize(report["runs"])
    except Exception as exc:
        report["status"] = "blocked" if isinstance(exc, FileNotFoundError) else "failed"
        report["errors"].append({"phase": "admission" if report["status"] == "blocked" else "execution",
                                 "type": type(exc).__name__, "message": str(exc)})
    finally:
        report["finished_at"] = timestamp()
        write_json(path, report)
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
