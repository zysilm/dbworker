"""Start and stop one isolated application stack at a time."""

import os
import json
import shutil
import signal
import socket
import subprocess  # nosec B404 -- commands are fixed argument lists and always run with shell=False.
import sys
import time
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, IO

REPOSITORY = Path(__file__).resolve().parents[4]
# The standalone image package also uses the repository-wide admission checks.
sys.path.insert(0, str(REPOSITORY))
WORKERS = 4
SQLITE_BUSY_TIMEOUT_SECONDS = 30
PRODUCER_HTTP_TIMEOUT_SECONDS = 60


def sqlite_database_url(database: Path) -> str:
    """Use identical bounded SQLite writer waiting in both application arms."""
    return f"sqlite:///{database}?timeout={SQLITE_BUSY_TIMEOUT_SECONDS}"


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def native_celery_idle_snapshot(app: Any) -> dict:
    """Record read-only inspector counts and every actual Redis priority lane."""
    inspected = time.time()
    inspect = app.control.inspect(timeout=2)
    responses = {name: getattr(inspect, name)() for name in ("active", "reserved", "scheduled")}
    complete = all(response and len(response) == 2 for response in responses.values())
    worker_sets = [set(response or {}) for response in responses.values()]
    complete = bool(complete and all(workers == worker_sets[0] for workers in worker_sets))
    states = {name: {worker: sum((task.get("request", task)).get("name") in
                                ("images.build", "images.compare") for task in tasks)
                     for worker, tasks in (response or {}).items()}
              for name, response in responses.items()}
    lanes = []
    with app.connection_for_read() as connection:
        if connection.transport.driver_type != "redis":
            raise RuntimeError("Image Celery idle observation requires the native Redis transport")
        channel = connection.channel()
        # Use exactly the LLEN pipeline employed by native Kombu Redis _size.
        # No passive declarations, queue mutations, or message payload reads.
        for queue in ("image_build", "image_compare"):
            with channel.conn_or_acquire() as client:
                with client.pipeline() as pipeline:
                    for priority in channel.priority_steps:
                        pipeline = pipeline.llen(channel._q_for_pri(queue, priority))
                    sizes = pipeline.execute()
            lanes.extend({"queue": queue, "priority": priority, "messages": int(size)}
                         for priority, size in zip(channel.priority_steps, sizes, strict=True))
    idle = complete and not any(count for workers in states.values() for count in workers.values()) and not any(
        lane["messages"] for lane in lanes)
    return {"backend": "celery", "inspect_started": inspected, "observed_at": time.time(),
            "worker_responses_complete": complete, "worker_states": states, "redis_lanes": lanes,
            "redis_priority_steps": list(channel.priority_steps),
            "idle": bool(idle)}


def native_celery_business_idle(app: Any) -> bool:
    return native_celery_idle_snapshot(app)["idle"]


class Stack(AbstractContextManager["Stack"]):
    def __init__(self, backend: str, directory: Path, *, page_size: int,
                 python: Path | None = None, redis_server: str = "redis-server", import_root: Path | None = None) -> None:
        import httpx

        self.backend = backend
        self.directory = directory
        self.database = directory / "application.db"
        self.page_size = page_size
        self.import_root = (import_root or directory).resolve()
        self.python = python or REPOSITORY / "examples" / f"imagededup_system_{backend}" / ".venv/bin/python"
        self.redis_server = redis_server
        self.api_port = free_port()
        self.processes: dict[str, subprocess.Popen[bytes]] = {}
        self.logs: list[IO[bytes]] = []
        self.client = httpx.Client(base_url=f"http://127.0.0.1:{self.api_port}", timeout=30)
        self.startup_seconds = 0.0
        self.versions: dict[str, Any] = {}
        self.http_samples: list[tuple[str, float]] = []
        self.observation_file = directory / "operations.jsonl"
        self.business_commit_file = directory / "business-commits.jsonl"
        self.native_execution: dict[str, Any] = {}

    def start_process(self, role: str, command: list[str], env: dict[str, str]) -> None:
        log = (self.directory / f"{role}.log").open("wb")
        self.logs.append(log)
        self.processes[role] = subprocess.Popen(  # nosec B603 -- command is assembled by this benchmark, never shell input.
            command, env=env, cwd=self.directory, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )

    def check_alive(self) -> None:
        for role, process in self.processes.items():
            if process.poll() is not None:
                raise RuntimeError(f"{role} exited with {process.returncode}; see {self.directory / (role + '.log')}")

    def wait_for_http(self) -> None:
        import httpx

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
        observer_source = REPOSITORY / "benchmarks" / "imagededup_benckmark" / "src"
        example_source = REPOSITORY / "examples" / f"imagededup_system_{self.backend}" / "src"
        env["PYTHONPATH"] = os.pathsep.join([str(observer_source), str(example_source), str(REPOSITORY),
                                           env.get("PYTHONPATH", "")])
        env.update(IMAGE_OBSERVATION_DATABASE=str(self.database), IMAGE_OBSERVATION_FILE=str(self.observation_file),
                   IMAGE_OBSERVATION_BACKEND="dbworker" if self.backend == "dbwork" else "celery")
        # Both scientific stacks receive the same thread limits. CPU parallelism
        # comes from four worker processes of each type, not nested BLAS pools.
        env.update(OMP_NUM_THREADS="1", OPENBLAS_NUM_THREADS="1", MKL_NUM_THREADS="1",
                   VECLIB_MAXIMUM_THREADS="1", NUMEXPR_NUM_THREADS="1")
        self.environment = env
        try:
            from benchmarks.common.native_admission import check_worker_source
            self.native_execution = check_worker_source(Path(__file__).read_text(), "imagededup")
            if not self.python.is_file():
                raise FileNotFoundError(f"Install the example's Poetry environment first: {self.python}")
            code = """import importlib.metadata as m, json, platform
result = {'python': platform.python_version()}
for package in ['sqlalchemy', 'fastapi', 'imagededup', 'numpy', 'celery', 'redis']:
    try: result[package] = m.version(package)
    except m.PackageNotFoundError: result[package] = None
print(json.dumps(result))
"""
            self.versions = json.loads(subprocess.check_output(  # nosec B603 -- interpreter path and code are fixed locally.
                [str(self.python), "-c", code], text=True,
            ))
            if self.backend == "dbwork":
                env.update(DBWORKER_DATABASE_URL=sqlite_database_url(self.database), DBWORKER_BUILD_WORKERS="4",
                           DBWORKER_COMPARISON_WORKERS="4", DBWORKER_COMPARISON_PAGE_SIZE=str(self.page_size),
                           DBWORKER_IMPORT_ROOT=str(self.import_root))
                env["DBWORKER_OBSERVER_MODULE"] = "imagededup_benckmark.observation"
            else:
                admission_code = """import json, sys
from pathlib import Path
from imagededup_system_redis_celery.celery_app import app
import imagededup_system_redis_celery.tasks
from benchmarks.common.native_admission import check_original_tasks
evidence = check_original_tasks(app, ['images.build', 'images.compare', 'images.dispatch'],
    Path(sys.argv[1]),
    expected_application='imagededup_system_redis_celery.celery_app:app',
    configuration={'task_acks_late': app.conf.task_acks_late,
                   'task_reject_on_worker_lost': app.conf.task_reject_on_worker_lost,
                   'worker_prefetch_multiplier': app.conf.worker_prefetch_multiplier,
                   'task_soft_time_limit': app.conf.task_soft_time_limit,
                   'task_time_limit': app.conf.task_time_limit})
print(json.dumps(evidence))
"""
                self.native_execution = json.loads(subprocess.check_output(  # nosec B603 -- fixed interpreter/code argument vector, shell=False.
                    [str(self.python), "-c", admission_code, str(example_source)],
                    env=env, cwd=self.directory, text=True, timeout=30))
                redis_binary = shutil.which(self.redis_server)
                if redis_binary is None:
                    raise FileNotFoundError("redis-server is required for the Redis/Celery benchmark")
                redis_port = free_port()
                redis_directory = self.directory / "redis-data"
                redis_directory.mkdir()
                self.start_process("redis", [redis_binary, "--bind", "127.0.0.1", "--port", str(redis_port),
                                   "--dir", str(redis_directory), "--save", "", "--appendonly", "yes",
                                   "--appendfsync", "everysec"], env)
                env.update(IMAGE_DATABASE_URL=sqlite_database_url(self.database),
                           IMAGE_BROKER_URL=f"redis://127.0.0.1:{redis_port}/0",
                           IMAGE_COMPARISON_PAGE_SIZE=str(self.page_size), IMAGE_DEPENDENCY_WAIT_SECONDS="1",
                           IMAGE_IMPORT_ROOT=str(self.import_root))
            api_module = "main_fastapi" if self.backend == "dbwork" else "main"
            self.start_process("api", [str(self.python), "-m", "uvicorn", f"imagededup_system_{self.backend}.{api_module}:app",
                               "--host", "127.0.0.1", "--port", str(self.api_port), "--no-access-log"], env)
            self.wait_for_http()
            if self.backend == "dbwork":
                self.start_process("worker_service", [str(self.python), "-m",
                                   "imagededup_system_dbwork.main_worker_service"], env)
            if self.backend == "redis_celery":
                # Celery's prefork pool requires fork; macOS defaults to spawn.
                # Select the advertised pool behavior before importing Celery.
                celery = [str(self.python), "-m", "celery", "-A", "imagededup_system_redis_celery.celery_app:app"]
                if sys.platform == "darwin":
                    celery = [str(self.python), "-c", "import billiard; billiard.set_start_method('fork', force=True); from celery.__main__ import main; main()",
                              "-A", "imagededup_system_redis_celery.celery_app:app"]
                for role, queue in (("build_worker", "image_build"), ("comparison_worker", "image_compare,image_control")):
                    self.start_process(role, celery + ["worker", "--include", "imagededup_benckmark.observation",
                                       "-Q", queue, "--pool=prefork", "--concurrency=4",
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

    def producer_client(self) -> Any:
        """Give each concurrent producer an independent HTTP connection pool."""
        import httpx
        return httpx.Client(base_url=f"http://127.0.0.1:{self.api_port}", timeout=PRODUCER_HTTP_TIMEOUT_SECONDS)

    def business_idle_snapshot(self) -> dict:
        if self.backend == "dbwork":
            import sqlite3
            with sqlite3.connect(self.database, timeout=30) as connection:
                connection.execute("PRAGMA query_only=ON")
                counts = {
                    "artifact_build_work": connection.execute(
                        "SELECT COUNT(*) FROM artifact_build_work WHERE execution_status IS NULL OR execution_status!='finished'"
                    ).fetchone()[0],
                    "comparison_work": connection.execute(
                        "SELECT COUNT(*) FROM comparison_work WHERE execution_status IS NULL OR execution_status!='finished'"
                    ).fetchone()[0],
                }
            return {"backend": "dbworker", "observed_at": time.time(), "unfinished_work": counts,
                    "idle": not any(counts.values())}
        code = """import json
from imagededup_system_redis_celery.celery_app import app
from imagededup_benckmark.runtime import native_celery_idle_snapshot
print(json.dumps(native_celery_idle_snapshot(app)))
"""
        result = subprocess.check_output([str(self.python), "-c", code], env=self.environment,  # nosec B603 -- fixed local code and argument vector, shell=False.
                                         cwd=self.directory, text=True, timeout=30)
        snapshot = json.loads(result)
        import sqlite3
        with sqlite3.connect(self.database, timeout=30) as connection:
            connection.execute("PRAGMA query_only=ON")
            pending = connection.execute("SELECT COUNT(*) FROM outbox_message WHERE task_name IN ('images.build','images.compare')").fetchone()[0]
        snapshot["pending_business_outbox"] = pending
        snapshot["idle"] = snapshot["idle"] and pending == 0
        return snapshot

    def native_business_idle(self) -> bool:
        return self.business_idle_snapshot()["idle"]

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
