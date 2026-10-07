"""Attempt the same pinned historical Sentry dependency graph on Python 3.12.

Both native application arms require direct in-process compatibility. Resolution,
build or bootstrap errors block admission; there is no Python 3.10 job bridge.
"""

from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]


def provision(uv="uv"):
    def run(*args):
        subprocess.run([uv, *map(str, args)], cwd=ROOT, check=True)
    for role in ("benchmark", "celery", "dbworker"):
        environment = ROOT / "benchmarks/environments/sentry" / role / ".venv"
        run("venv", "--allow-existing", "--python", "3.12", environment)
        python = environment / "bin/python"
        if role == "benchmark":
            run("pip", "install", "--python", python, "psutil==7.2.2")
        else:
            run("pip", "install", "--python", python, "-r", "benchmarks/locks/sentry-native.txt",
                "--build-constraint", "benchmarks/locks/sentry-build.txt")
            # Binary wheels bundle different libxml2 versions. Native Django URL
            # checks import both extensions, so build the unchanged pins against
            # the same system libraries, including on an existing environment.
            run("pip", "install", "--python", python, "--no-cache", "--no-deps",
                "--no-binary", "lxml", "--no-binary", "xmlsec",
                "--reinstall-package", "lxml", "--reinstall-package", "xmlsec",
                "--build-constraint", "benchmarks/locks/sentry-build.txt",
                "lxml==4.9.3", "xmlsec==1.3.17")
            subprocess.run([str(python), "-c",
                "from benchmarks.upstream.sentry_backend import xml_library_linkage; "
                "print(xml_library_linkage())"], cwd=ROOT, check=True)
            if role == "dbworker":
                run("pip", "install", "--python", python, ".")


if __name__ == "__main__":
    provision(sys.argv[1] if len(sys.argv) > 1 else "uv")
