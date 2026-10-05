"""Execute real SQL Lab queries through one isolated backend stack."""

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
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from benchmarks.common.reporting import write_json
from examples.dbworker_integration.runtime import Base, Request, coordinator


def wait_for(predicate, *, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.05)
    raise TimeoutError("Backend made no completion progress before the deadline")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--backend", required=True, choices=("celery", "dbworker"))
    parser.add_argument("--repetition", required=True, type=int)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    profile = config["suite"]["profiles"][config["profile"]]
    directory = Path(config["output_directory"]) / f"superset-{args.repetition}-{args.backend}"
    directory.mkdir()
    url = f"sqlite:///{directory / 'requests.db'}"
    warehouse = directory / "warehouse.db"
    with sqlite3.connect(warehouse) as connection:
        connection.execute("CREATE TABLE facts (category INTEGER, value INTEGER)")
        connection.executemany("INSERT INTO facts VALUES (?,?)", ((i % 10, i) for i in range(10000)))
    expected = [{"category": k, "total": sum(range(k, 10000, 10))} for k in range(10)]
    settings = directory / "superset_config.py"
    settings.write_text(f"SECRET_KEY = 'isolated-benchmark-only-secret'\n"
                        f"SQLALCHEMY_DATABASE_URI = 'sqlite:///{directory / 'metadata.db'}'\n"
                        "SQLALCHEMY_ENCRYPTED_FIELD_ENGINE = 'aes-gcm'\n"
                        "CONTENT_SECURITY_POLICY_WARNING = False\nTALISMAN_ENABLED = False\n"
                        "WTF_CSRF_ENABLED = False\n")
    os.environ["SUPERSET_CONFIG_PATH"] = str(settings)
    os.environ["DBWORKER_DATABASE_URL"] = url
    os.environ["PYTHONPATH"] = str(ROOT) + os.pathsep + str(ROOT / "src")
    # Migrations and initialization happen outside timed work.
    with (directory / "migrations.log").open("wb") as log:
        subprocess.run([str(Path(sys.executable).parent / "superset"), "db", "upgrade"],
                       stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)
    from examples.superset_dbworker.adapter import initialize
    app = initialize()
    from superset import db
    from superset.models.core import Database
    from superset.models.sql_lab import Query

    with app.app_context():
        database = Database(database_name="benchmark", sqlalchemy_uri=f"sqlite:///{warehouse}", allow_run_async=False)
        db.session.add(database)
        db.session.flush()
        query_ids = []
        for i in range(profile["requests"] + 2):
            query = Query(database_id=database.id, client_id=f"bench{i}", limit=100,
                          sql="SELECT category, SUM(value) AS total FROM facts GROUP BY category ORDER BY category",
                          select_as_cta=False)
            db.session.add(query)
            db.session.flush()
            query_ids.append(query.id)
        db.session.commit()
    engine = create_engine(url)
    sessions = sessionmaker(engine)
    runtime = coordinator(url, concurrency=2)
    children = []
    streams = []
    try:
        if args.backend == "celery":
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            redis_url = f"redis://127.0.0.1:{port}/0"
            os.environ["BENCHMARK_REDIS_URL"] = redis_url
            def launch(command, name):
                stream = (directory / name).open("wb")
                streams.append(stream)
                children.append(subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT))
            launch(["redis-server", "--bind", "127.0.0.1", "--port", str(port), "--dir", str(directory),
                    "--save", "", "--appendonly", "yes", "--appendfsync", "everysec"], "redis.log")
            import redis
            client = redis.Redis.from_url(redis_url)
            def redis_ready():
                try:
                    return client.ping()
                except redis.ConnectionError:
                    return False
            wait_for(redis_ready)
            launch([sys.executable, "-m", "celery", "-A", "benchmarks.upstream.celery_app:app", "worker",
                    "--concurrency", "2", "--loglevel", "WARNING", "--hostname", f"suite-{port}@localhost",
                    "--without-gossip", "--without-mingle"], "worker.log")
            from benchmarks.upstream.celery_app import app as celery, execute_request
            wait_for(lambda: celery.control.ping(timeout=1))
        else:
            runtime.start()

        def execute_batch(ids):
            started = time.perf_counter()
            with sessions.begin() as session:
                requests = [Request(suite="superset", payload={"query_id": key,
                            "rendered_query": "SELECT category, SUM(value) AS total FROM facts GROUP BY category ORDER BY category"})
                            for key in ids]
                session.add_all(requests)
                session.flush()
                identities = [r.id for r in requests]
            if args.backend == "celery":
                for key in identities:
                    execute_request.delay(key)
            def complete():
                if any(child.poll() is not None for child in children):
                    raise RuntimeError("An owned backend service exited")
                with sessions() as session:
                    results = session.scalars(select(Request).where(Request.id.in_(identities)).order_by(Request.id)).all()
                    if len(results) == len(ids) and all(r.result is not None for r in results):
                        return [r.result for r in results]
                return None
            results = wait_for(complete)
            return time.perf_counter() - started, results

        execute_batch(query_ids[:2])
        seconds, results = execute_batch(query_ids[2:])
        if any(result["data"] != expected or result["rows"] != 10 for result in results):
            raise AssertionError("SQL Lab output differs from the independent fixture oracle")
        normalized = json.dumps(results, sort_keys=True).encode()
        with app.app_context():
            db.session.remove()
            if any(db.session.get(Query, key).status != "success" for key in query_ids[2:]):
                raise AssertionError("A query did not reach durable business success")
        if args.backend == "dbworker":
            with sessions() as session:
                table = runtime.workers["integration"].table
                states = session.execute(select(table.c.execution_status)).scalars().all()
                if len(states) != len(query_ids) or any(str(state) != "finished" for state in states):
                    raise AssertionError("DBWorker completion ledger is incomplete")
        row = {"scenario": "sql_lab_group_by", "comparison_mode": "paired_durable_request",
               "backend": args.backend, "repetition": args.repetition, "status": "passed",
               "metrics": {"wall_seconds": seconds, "queries_per_second": profile["requests"] / seconds,
                           "cpu_seconds": None, "peak_rss_bytes": None},
               "unavailable_metrics": {"cpu_seconds": "Total-stack sampling is not implemented for this initial suite",
                                       "peak_rss_bytes": "Total-stack sampling is not implemented for this initial suite"},
               "validation": {"passed": True, "output_digest": hashlib.sha256(normalized).hexdigest(),
                              "queries": len(results), "rows_per_query": 10},
               "environment": {"python": platform.python_version(), "interpreter": sys.executable}}
        packages = {}
        for name in ("apache-superset", "celery", "sqlalchemy", "dbworker", "rich"):
            try:
                packages[name] = importlib.metadata.version(name)
            except importlib.metadata.PackageNotFoundError:
                packages[name] = None
        row["environment"]["packages"] = packages
        write_json(directory / "sample.json", row)
    finally:
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
        engine.dispose()


if __name__ == "__main__":
    main()
