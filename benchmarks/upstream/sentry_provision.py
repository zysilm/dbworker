"""Provision pinned Sentry 24.1 mail and paired modern queue environments.

The upstream frozen lock was generated on Python 3.10 and cannot resolve on
3.8: sentry-relay requires >=3.9 and botocore's 3.8 urllib3 range conflicts.
Use the lock's compatible 3.10 interpreter, separately from DBWorker's 3.12.
"""

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def provision(uv="uv"):
    def run(*args):
        subprocess.run([uv, *map(str, args)], cwd=ROOT, check=True)
    for role in ("benchmark", "celery", "dbworker", "upstream"):
        environment = ROOT / "benchmarks/environments/sentry" / role / ".venv"
        run("venv", "--allow-existing", "--python", "3.10.20" if role == "upstream" else "3.12", environment)
        python = environment / "bin/python"
        if role == "benchmark":
            run("pip", "install", "--python", python, "psutil==7.2.2")
        elif role == "upstream":
            run("pip", "install", "--python", python, "-r", "benchmarks/locks/sentry-upstream.txt",
                "--build-constraint", "benchmarks/locks/sentry-build.txt")
        else:
            run("pip", "install", "--python", python, "-r", "benchmarks/locks/sentry-modern.txt", ".")


if __name__ == "__main__":
    provision(sys.argv[1] if len(sys.argv) > 1 else "uv")
