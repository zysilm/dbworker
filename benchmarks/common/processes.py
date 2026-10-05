"""Run suite subprocesses with explicit interpreters and owned-tree cleanup."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path

import psutil


def run_command(command: list[str], *, cwd: Path, env: dict[str, str], log: Path, timeout: float) -> int:
    if timeout <= 0:
        raise ValueError("Command timeout must be positive")
    owned: dict[tuple[int, float], psutil.Process] = {}
    deadline = time.monotonic() + timeout
    with log.open("wb") as stream:
        process = subprocess.Popen(command, cwd=cwd, env=env, stdout=stream, stderr=subprocess.STDOUT,
                                   start_new_session=True)
        try:
            root = psutil.Process(process.pid)
            owned[(root.pid, root.create_time())] = root
            while process.poll() is None:
                for parent in list(owned.values()):
                    try:
                        for child in parent.children(recursive=True):
                            owned[(child.pid, child.create_time())] = child
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Suite command exceeded {timeout}s; see {log}")
                time.sleep(.1)
            return process.returncode
        finally:
            # Child application stacks may create their own process groups.
            # Keep references with creation times instead of killing arbitrary PIDs.
            survivors = []
            for child in reversed(list(owned.values())):
                try:
                    if child.is_running() and child.status() != psutil.STATUS_ZOMBIE:
                        child.terminate()
                        survivors.append(child)
                except psutil.NoSuchProcess:
                    pass
            _, alive = psutil.wait_procs(survivors, timeout=3)
            for child in alive:
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()
