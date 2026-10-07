"""Adapt the existing image experiment to the repository-wide report contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).parent / "src"))

from benchmarks.common.reporting import summarize, timestamp, write_json
from imagededup_benckmark.run import main as run_images


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    path = Path(config["report_path"])
    report = json.loads(path.read_text())
    output = Path(config["output_directory"])
    raw = output / "imagededup.raw.json"
    profile = config["suite"]["profiles"][config["profile"]]
    command = ["--images", str(profile["images"]), "--repetitions", str(profile["repetitions"]),
               "--warmup-images", str(profile["warmup_images"]), "--output", str(raw),
               "--dbwork-python", config["interpreters"]["dbworker_python"],
               "--celery-python", config["interpreters"]["celery_python"], "--download"]
    command.extend(["--work-dir", str(output / "imagededup-work")])
    report["environment"]["benchmark_python"] = platform.python_version()
    report["artifacts"]["raw_report"] = raw.name
    code = 0
    try:
        run_images(command)
    except Exception as exc:
        code = 1
        report["errors"].append({"phase": "experiment", "type": type(exc).__name__, "message": str(exc)})
    finally:
        if raw.exists():
            original = json.loads(raw.read_text())
            for key in ("configuration", "dataset"):
                report[key] = original[key]
            report["environment"]["host"] = original["host"]
            report["environment"]["stacks"] = original["stacks"]
            report["capabilities"] = {"verified": ["image_hash", "top_k", "completion_ledger",
                                                    "individual_builds", "bounded_scoring_pages",
                                                    "native_task_origin", "business_quiescence"],
                                      "untested": ["database_outage", "child_crash", "retry_policy_parity", "task_deadline_parity"],
                                      "measurement_notes": original["measurement_notes"]}
            for row in original["runs"]:
                validation = dict(row["validation"])
                validation["output_digest"] = hashlib.sha256(json.dumps(row["validation"], sort_keys=True).encode()).hexdigest()
                report["runs"].append({**row, "backend": "dbworker" if row["backend"] == "dbwork" else "celery",
                                       "comparison_mode": "native_execution", "validation": validation})
            report["status"] = original["status"]
        else:
            report["status"] = "failed"
        if code:
            report["status"] = "failed"
        report["summary"] = summarize(report["runs"])
        report["finished_at"] = timestamp()
        write_json(path, report)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
