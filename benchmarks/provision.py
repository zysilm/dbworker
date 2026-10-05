"""Provision independent environments for admitted suites outside experiment timing."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def run(*command: str) -> None:
    subprocess.run(list(command), cwd=ROOT, check=True)


def provision(selected: list[str] | None = None, *, uv: str = "uv") -> None:
    registry = json.loads((ROOT / "benchmarks/registry.json").read_text())
    selected = selected or [suite["suite_id"] for suite in registry["suites"]]
    projects = {"benchmark": "benchmarks/imagededup_benckmark",
                "celery": "examples/imagededup_system_redis_celery",
                "dbworker": "examples/imagededup_system_dbwork"}
    for suite in registry["suites"]:
        if suite["suite_id"] not in selected:
            continue
        name = suite["suite_id"]
        if name == "sentry":
            from benchmarks.upstream.sentry_provision import provision as sentry_provision
            sentry_provision(uv)
            continue
        for role in ("benchmark", "celery", "dbworker"):
            python = ROOT / suite["interpreters"][f"{role}_python"]
            version = "3.14.7" if name == "posthog" and role != "benchmark" else "3.12"
            run(uv, "venv", "--allow-existing", "--python", version, str(python.parent.parent))
            if suite["suite_id"] == "imagededup":
                constraints = str(ROOT / f"benchmarks/locks/imagededup-{role}.txt")
                # Avoid multi-gigabyte CUDA dependencies for hosted correctness jobs.
                if sys.platform.startswith("linux") and role != "benchmark":
                    run(uv, "pip", "install", "--python", str(python), "--index-url",
                        "https://download.pytorch.org/whl/cpu", "torch==2.14.0", "torchvision==0.29.0")
                run(uv, "pip", "install", "--python", str(python), "--constraint", constraints, projects[role])
                run(uv, "pip", "install", "--python", str(python), "-r", "benchmarks/requirements.txt")
            elif role == "benchmark":
                run(uv, "pip", "install", "--python", str(python), "-r", "benchmarks/requirements.txt")
            elif name == "saleor":
                run(uv, "pip", "install", "--python", str(python), "--constraint", "benchmarks/locks/saleor.txt",
                    "examples/saleor", ".", "psutil", "pytest")
            elif name in ("posthog", "paperless_ngx"):
                run(uv, "pip", "install", "--python", str(python), "-r", f"benchmarks/locks/{name}.txt", ".")
            elif name == "superset":
                command = [uv, "pip", "install", "--python", str(python), "--constraint",
                           "benchmarks/locks/superset.txt", "examples/superset", "examples/superset/superset-core", "rich", "psutil"]
                if role == "dbworker":
                    command.append(".")
                run(*command)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", action="append", choices=("imagededup", "superset", "saleor", "paperless_ngx", "posthog", "sentry"))
    parser.add_argument("--uv", default="uv")
    args = parser.parse_args()
    provision(args.suite, uv=args.uv)


if __name__ == "__main__":
    main()
