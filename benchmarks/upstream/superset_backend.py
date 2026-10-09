"""Compare native asynchronous SQL Lab with its DBWorker executor variation."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import socket
import sqlite3
import subprocess
import sys
import time
from contextlib import nullcontext
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "examples/superset"))
sys.path.insert(0, str(ROOT / "examples/superset/superset-core/src"))

from sqlalchemy import select
from benchmarks.common.reporting import write_json
from benchmarks.common.native_observer import operation
from benchmarks.common.timing_evidence import begin_window, end_window, elapsed_seconds

SQL = "SELECT category, SUM(value) + {marker} AS total FROM facts GROUP BY category ORDER BY category"


def operation_marker(identity):
    prefix, number = identity.rsplit(":" if identity.startswith("warmup:") else "-", 1)
    if prefix not in ("query", "warmup") or not number.isdecimal():
        raise ValueError("Unexpected SQL Lab fixture operation")
    return int(number) + (10000 if prefix == "warmup" else 0)


def sql_for_operation(identity):
    return SQL.format(marker=operation_marker(identity))


def expected_rows(identity):
    marker = operation_marker(identity)
    return [{"category": k, "total": sum(range(k, 10000, 10)) + marker} for k in range(10)]


def json_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def wait_for(predicate, *, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.05)
    raise TimeoutError("Native SQL Lab completion deadline exceeded")


def write_configuration(path, directory, port):
    """Load upstream Docker settings; change only isolated fixture/environment paths."""
    upstream = ROOT / "examples/superset/docker/pythonpath_dev/superset_config.py"
    cache = directory / "sql_lab_results"
    path.write_text(
        "from pathlib import Path\n"
        f"_source = Path({str(upstream)!r}).read_text()\n"
        f"exec(compile(_source.replace('/app/superset_home/sqllab', {str(cache)!r}), "
        f"{str(upstream)!r}, 'exec'))\n"
        "SECRET_KEY = 'isolated-benchmark-only-secret'\n"
        f"SQLALCHEMY_DATABASE_URI = {'sqlite:///' + str(directory / 'metadata.db')!r}\n"
        "SQLALCHEMY_ENCRYPTED_FIELD_ENGINE = 'aes-gcm'\n"
        "CONTENT_SECURITY_POLICY_WARNING = False\nTALISMAN_ENABLED = False\n"
        "WTF_CSRF_ENABLED = False\n"
        # Flask test-client sessions use ordinary login cookies over local HTTP.
        "SESSION_COOKIE_SECURE = False\n"
    )
    os.environ.update(SUPERSET_CONFIG_PATH=str(path), REDIS_HOST="127.0.0.1", REDIS_PORT=str(port),
                      REDIS_CELERY_DB="0", REDIS_RESULTS_DB="1")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--backend", required=True, choices=("celery", "dbworker"))
    parser.add_argument("--repetition", required=True, type=int)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    profile = config["suite"]["profiles"][config["profile"]]
    directory = (Path(config["output_directory"]) / f"superset-{args.repetition}-{args.backend}").resolve()
    directory.mkdir()
    trace = directory / "task_trace.jsonl"
    os.environ.update(BENCHMARK_BACKEND=args.backend, BENCHMARK_TRACE_PATH=str(trace),
                      BENCHMARK_TASK_STAGES=json.dumps({"sql_lab.get_sql_results": "sql_lab"}),
                      PYTHONPATH=os.pathsep.join((str(ROOT / "examples/superset"), str(ROOT / "examples/superset/superset-core/src"), str(ROOT), str(ROOT / "src"))))
    warehouse = directory / "warehouse.db"
    with sqlite3.connect(warehouse) as connection:
        connection.execute("CREATE TABLE facts (category INTEGER, value INTEGER)")
        connection.executemany("INSERT INTO facts VALUES (?,?)", ((i % 10, i) for i in range(10000)))
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    write_configuration(directory / "superset_config.py", directory, port)
    children, streams = [], []
    runtime = None
    engine = None

    def launch(command, name):
        stream = (directory / name).open("wb")
        streams.append(stream)
        child = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT)
        children.append(child)
        return child

    try:
        # Redis serves upstream caches/results in both arms, and Celery's broker
        # only in the native arm. There is no benchmark Celery app or task.
        launch(["redis-server", "--bind", "127.0.0.1", "--port", str(port), "--dir", str(directory),
                "--save", "", "--appendonly", "yes", "--appendfsync", "everysec"], "redis.log")
        import redis
        redis_client = redis.Redis(host="127.0.0.1", port=port)

        def redis_ready():
            try:
                return redis_client.ping()
            except redis.ConnectionError:
                return False

        wait_for(redis_ready)
        binary = str(Path(sys.executable).parent / "superset")
        with (directory / "initialization.log").open("wb") as log:
            for command in ([binary, "db", "upgrade"], [binary, "fab", "create-admin", "--username", "benchmark",
                             "--firstname", "Benchmark", "--lastname", "User", "--email", "benchmark@example.test",
                             "--password", "isolated-fixture-password"], [binary, "init"]):
                subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
        # Initialize through the original worker module exactly once. Accessing
        # Celery's registry imports that module too; a separate create_app here
        # would reinitialize the process-global AppBuilder and its view objects.
        from superset.tasks.celery_app import flask_app as app
        from superset import db
        from superset.models.core import Database
        from superset.models.sql_lab import Query
        with app.app_context():
            database = Database(database_name="benchmark", sqlalchemy_uri=f"sqlite:///{warehouse}", allow_run_async=True)
            db.session.add(database)
            db.session.commit()
            database_id = database.id
        from superset.extensions import celery_app
        from superset.sql_lab import get_sql_results
        from benchmarks.common.native_admission import check_original_tasks
        native_execution = check_original_tasks(
            celery_app, [get_sql_results.name], ROOT / "examples/superset",
            expected_application="superset.tasks.celery_app:app",
            configuration={"upstream_config": "docker/pythonpath_dev/superset_config.py",
                           "source_sha256": hashlib.sha256((ROOT / "examples/superset/docker/pythonpath_dev/superset_config.py").read_bytes()).hexdigest(),
                           "overrides": "isolated Redis/cache/metadata/security fixture environment"})
        client = app.test_client()
        login = client.post("/login/", data={"username": "benchmark", "password": "isolated-fixture-password"})
        if login.status_code != 302:
            raise AssertionError(f"Native login failed: {login.status_code}")

        if args.backend == "celery":
            launch([sys.executable, "-m", "celery", "--app=superset.tasks.celery_app:app", "worker",
                    "-O", "fair", "-l", "WARNING", "--concurrency=2",
                    "--include=benchmarks.common.native_observer", "--hostname", f"superset-{port}@localhost"], "worker.log")
            from superset.extensions import celery_app
            wait_for(lambda: celery_app.control.ping(timeout=1))
            executor_context = nullcontext()
        else:
            from examples.superset_dbworker.executor import coordinator, use_dbworker_executor
            runtime, sessions = coordinator(f"sqlite:///{directory / 'requests.db'}", concurrency=2)
            engine = sessions.kw["bind"]
            runtime.start()
            executor_context = use_dbworker_executor(sessions)

        def execute_batch(operations):
            started = begin_window()
            query_ids = []
            bindings = []
            for identity in operations:
                sql = sql_for_operation(identity)
                with operation(identity):
                    response = client.post("/api/v1/sqllab/execute/", json={
                        "database_id": database_id, "sql": sql, "client_id": identity,
                        "queryLimit": 100, "runAsync": True, "select_as_cta": False,
                        "expand_data": False, "templateParams": "{}",
                    })
                if response.status_code != 202:
                    raise AssertionError(f"Native SQL Lab submission failed: {response.status_code} {response.get_data(as_text=True)}")
                with app.app_context():
                    query = db.session.query(Query).filter_by(client_id=identity).one()
                    query_ids.append(query.id)
                    bindings.append({"operation_id": identity, "stage": "sql_lab",
                                     "query_id": query.id, "sql_sha256": hashlib.sha256(sql.encode()).hexdigest(),
                                     "username": "benchmark"})

            def complete():
                if any(child.poll() is not None for child in children):
                    raise RuntimeError("An owned backend service exited")
                with app.app_context():
                    rows = db.session.query(Query).filter(Query.id.in_(query_ids)).all()
                    failed = [q.id for q in rows if q.status in ("failed", "timed_out", "stopped")]
                    if failed:
                        raise AssertionError(f"Native SQL Lab queries failed: {failed}")
                    if len(rows) == len(operations) and all(q.status == "success" and q.results_key for q in rows):
                        return {q.id: q.results_key for q in rows}
                return None

            keys = wait_for(complete)
            results = []
            for binding, query_id in zip(bindings, query_ids):
                response = client.get("/api/v1/sqllab/results/", query_string={"q": json.dumps({"key": keys[query_id]})})
                if response.status_code != 200:
                    raise AssertionError(f"Native SQL Lab results unavailable: {response.status_code} {response.get_data(as_text=True)}")
                value = response.get_json()
                results.append({"data": value["data"], "columns": value["columns"], "status": value["status"]})
                if value["data"] != expected_rows(binding["operation_id"]) or value["status"] != "success":
                    raise AssertionError("Native retrieved SQL Lab output differs from the independent oracle")
                binding.update(results_key=str(keys[query_id]), output_sha256=json_digest(results[-1]))
            # All required scheduler terminal events and DBWorker ledger commits
            # complete inside the same timed interval, after native result reads.
            from benchmarks.common.workflow_graph import read_trace
            def tasks_done():
                events = read_trace(trace)
                successful = {e["operation_id"] for e in events if e["event"] == "succeeded"}
                return set(operations).issubset(successful)
            wait_for(tasks_done)
            if runtime:
                table = runtime.workers["sql_lab"].table
                def ledger_done():
                    with sessions() as session:
                        states = session.execute(select(table.c.execution_status).where(table.c.source_id.in_(query_ids))).scalars().all()
                        return len(states) == len(query_ids) and all(str(s) == "finished" for s in states)
                wait_for(ledger_done)
            measurement_window = end_window(started)
            events = read_trace(trace)
            for binding in bindings:
                submitted = [e for e in events if e["operation_id"] == binding["operation_id"]
                             and e["event"] == "submitted"]
                if len(submitted) != 1:
                    raise AssertionError("SQL Lab operation must bind exactly one original publication")
                binding.update(node_id=submitted[0]["node_id"],
                               argument_sha256=submitted[0]["details"]["argument_sha256"])
            return elapsed_seconds(measurement_window), results, bindings, measurement_window

        warmups = [f"warmup:{i}" for i in range(2)]
        operations = [f"query-{i}" for i in range(profile["requests"])]
        with executor_context:
            execute_batch(warmups)
            seconds, results, bindings, measurement_window = execute_batch(operations)
        from benchmarks.common.workflow_graph import read_trace, validate_graph
        events = read_trace(trace)
        graph = validate_graph(events, operations, {"sql_lab": 1}, [], warmup_operations=warmups)
        packages = {}
        for name in ("apache-superset", "celery", "sqlalchemy", "dbworker", "rich"):
            try:
                packages[name] = importlib.metadata.version(name)
            except importlib.metadata.PackageNotFoundError:
                packages[name] = None
        row = {"scenario": "sql_lab_group_by", "comparison_mode": "native_application_workflow",
               "backend": args.backend, "repetition": args.repetition, "status": "passed",
               "metrics": {"wall_seconds": seconds, "queries_per_second": len(operations) / seconds,
                           "cpu_seconds": None, "peak_rss_bytes": None},
               "unavailable_metrics": {"cpu_seconds": "Total-stack CPU sampling is unavailable",
                                       "peak_rss_bytes": "Total-stack RSS sampling is unavailable"},
               "validation": {"passed": True, "output_digest": hashlib.sha256(json.dumps(results, sort_keys=True).encode()).hexdigest(),
                              "queries": len(results), "rows_per_query": 10, "retrieved_results": len(results)},
               "workflow_graph": graph,
               "business_bindings": bindings, "warmup_operations": warmups,
               "measurement_window": measurement_window,
               "workflow_trace": {"path": str(trace.relative_to(Path(config["output_directory"]).resolve())),
                                  "sha256": hashlib.sha256(trace.read_bytes()).hexdigest()},
               "native_execution": native_execution,
               "configuration": {"concurrency": 2, "native_application": "superset.tasks.celery_app:app",
                                 "native_configuration": "docker/pythonpath_dev/superset_config.py",
                                 "submission": "authenticated original SQL Lab REST API",
                                 "completion": "native success + stored results + authenticated retrieval + scheduler terminal event",
                                 "timing": "first API submission through all result retrieval and scheduler completion",
                                 "metadata_database": "sqlite", "warehouse_database": "sqlite",
                                 "results_backend": "upstream FileSystemCache", "redis_aof": "everysec",
                                 "environment_overrides": ["isolated Redis host/port", "isolated filesystem cache directory",
                                     "isolated SQLite metadata URI", "fixture secret/encryption setting", "local test-client CSRF/TLS disabled"],
                                 "celery_acknowledgements": "upstream early acknowledgements",
                                 "dbworker_timeouts": "No equivalent native soft/hard task limits; normal completion only"},
               "capabilities": {"verified": ["native_sql_lab_submission", "native_task_dispatch", "async_results_storage",
                                               "native_results_retrieval", "one_job_per_query", "business_success"],
                                "untested": ["worker_crash", "publication_failure", "timeouts", "cancellation", "cross_orm_atomicity", "postgresql"],
                                "scope": "Native successful asynchronous SQL Lab SELECT workflow on a declared SQLite fixture"},
               "environment": {"python": platform.python_version(), "interpreter": sys.executable, "packages": packages}}
        write_json(directory / "sample.json", row)
    finally:
        if runtime:
            runtime.stop()
        for child in reversed(children):
            if child.poll() is None:
                child.terminate()
        for child in reversed(children):
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for stream in streams:
            stream.close()
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    main()
