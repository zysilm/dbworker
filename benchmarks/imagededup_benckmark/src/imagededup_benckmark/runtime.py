"""Start and stop one isolated application stack at a time."""

import os
import json
import shutil
import signal
import socket
import subprocess
import time
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, IO

import httpx

REPOSITORY = Path(__file__).resolve().parents[4]
WORKERS = 4


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


class Stack(AbstractContextManager["Stack"]):
    def __init__(self, backend: str, directory: Path, *, page_size: int,
                 python: Path | None = None, redis_server: str = "redis-server") -> None:
        self.backend = backend
        self.directory = directory
        self.database = directory / "application.db"
        self.page_size = page_size
        self.python = python or REPOSITORY / "examples" / f"imagededup_system_{backend}" / ".venv/bin/python"
        self.redis_server = redis_server
        self.api_port = free_port()
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.logs: list[IO[bytes]] = []
        self.client = httpx.Client(base_url=f"http://127.0.0.1:{self.api_port}", timeout=30)
        self.startup_seconds = 0.0
        self.versions: dict[str, Any] = {}
        self.http_samples: list[tuple[str, float]] = []

    def start_process(self, role: str, command: list[str], env: dict[str, str]) -> None:
        log = (self.directory / f"{role}.log").open("wb")
        self.logs.append(log)
        self.processes[role] = subprocess.Popen(command, env=env, cwd=self.directory,
                                                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)

    def check_alive(self) -> None:
        for role, process in self.processes.items():
            if process.poll() is not None:
                raise RuntimeError(f"{role} exited with {process.returncode}; see {self.directory / (role + '.log')}")

    def wait_for_http(self) -> None:
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            self.check_alive()
            try:
                response = self.client.get("/openapi.json")
                if response.status_code == 200:
                    expected = f"imagededup_system_{self.backend}"
                    if response.json()["info"]["title"] != expected:
                        raise RuntimeError("Unexpected application on the selected port")
                    return
            except httpx.ConnectError:
                pass
            time.sleep(0.1)
        raise TimeoutError("API startup exceeded 180 seconds")

    def __enter__(self) -> "Stack":
        self.directory.mkdir(parents=True, exist_ok=False)
        started = time.perf_counter()
        env = os.environ.copy()
        # Both scientific stacks receive the same thread limits. CPU parallelism
        # comes from four worker processes of each type, not nested BLAS pools.
        env.update(OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1",
                   VECLIB_MAXIMUM_THREADS="1", NUMEXPR_NUM_THREADS="1")
        try:
            if not self.python.is_file():
                raise FileNotFoundError(f"Install the example's Poetry environment first: {self.python}")
            code = """import importlib.metadata as m, json, platform
result = {'python': platform.python_version()}
for package in ['sqlalchemy', 'fastapi', 'imagededup', 'numpy', 'celery', 'redis']:
    try: result[package] = m.version(package)
    except m.PackageNotFoundError: result[package] = None
print(json.dumps(result))
"""
            self.versions = json.loads(subprocess.check_output([str(self.python), "-c", code], text=True))
            if self.backend == "dbwork":
                env.update(DBWORKER_DATABASE_URL=f"sqlite:///{self.database}", DBWORKER_BUILD_WORKERS="4",
                           DBWORKER_COMPARISON_WORKERS="4", DBWORKER_COMPARISON_PAGE_SIZE=str(self.page_size))
            else:
                redis_binary = shutil.which(self.redis_server)
                if redis_binary is None:
                    raise FileNotFoundError("redis-server is required for the Redis/Celery benchmark")
                redis_port = free_port()
                redis_directory = self.directory / "redis-data"
                redis_directory.mkdir()
                self.start_process("redis", [redis_binary, "--bind", "127.0.0.1", "--port", str(redis_port),
                                   "--dir", str(redis_directory), "--save", "", "--appendonly", "yes",
                                   "--appendfsync", "everysec"], env)
                env.update(IMAGE_DATABASE_URL=f"sqlite:///{self.database}",
                           IMAGE_BROKER_URL=f"redis://127.0.0.1:{redis_port}/0",
                           IMAGE_COMPARISON_PAGE_SIZE=str(self.page_size), IMAGE_DEPENDENCY_WAIT_SECONDS="1")
            api_module = "main_fastapi" if self.backend == "dbwork" else "main"
            self.start_process("api", [str(self.python), "-m", "uvicorn", f"imagededup_system_{self.backend}.{api_module}:app",
                               "--host", "127.0.0.1", "--port", str(self.api_port), "--no-access-log"], env)
            self.wait_for_http()
            if self.backend == "dbwork":
                self.start_process("worker_service", [str(self.python), "-m",
                                   "imagededup_system_dbwork.main_worker_service"], env)
            if self.backend == "redis_celery":
                celery = [str(self.python), "-m", "celery", "-A", "imagededup_system_redis_celery.celery_app:app"]
                for role, queue in (("build_worker", "image_build"), ("comparison_worker", "image_compare,image_control")):
                    self.start_process(role, celery + ["worker", "-Q", queue, "--pool=prefork", "--concurrency=4",
                                       f"--hostname={role}@%h", "--loglevel=WARNING"], env)
                self.start_process("beat", celery + ["beat", "--schedule", str(self.directory / "beat-schedule"),
                                   "--loglevel=WARNING"], env)
            self.startup_seconds = time.perf_counter() - started
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def request(self, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
        started = time.perf_counter()
        response = self.client.request(method, path, json=body)
        self.http_samples.append((method + " " + path.split("?")[0], time.perf_counter() - started))
        response.raise_for_status()
        return response.json()

    def __exit__(self, *args: object) -> None:
        self.client.close()
        # No subsequent stack starts until this one's process groups are gone.
        for process in reversed(list(self.processes.values())):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
            try:
                os.killpg(process.pid, signal.SIGTERM)
                time.sleep(0.05)
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        for log in self.logs:
            log.close()
