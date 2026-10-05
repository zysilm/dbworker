"""Benchmark Saleor's real product CSV export and durable job lifecycle."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import importlib.metadata
import io
import json
import os
import platform
import shutil
import socket
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
from examples.dbworker_integration.runtime import Request, coordinator


def wait_for(predicate, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.05)
    raise TimeoutError("Saleor made no completion progress before the deadline")


def reserve_port():
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return reservation.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--backend", required=True, choices=("celery", "dbworker"))
    parser.add_argument("--repetition", required=True, type=int)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    profile = config["suite"]["profiles"][config["profile"]]
    directory = Path(config["output_directory"]) / f"saleor-{args.repetition}-{args.backend}"
    directory.mkdir()
    children, streams = [], []
    runtime = engine = None

    def launch(command, name):
        stream = (directory / name).open("wb")
        streams.append(stream)
        child = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT)
        children.append(child)
        return child

    def command(command, name, timeout=600):
        with (directory / name).open("wb") as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=timeout)

    try:
        # An owned cluster avoids changing or relying on the developer's databases.
        for binary in ("initdb", "postgres", "psql"):
            if not shutil.which(binary):
                raise FileNotFoundError(f"Saleor requires PostgreSQL executable: {binary}")
        cluster = directory / "postgres"
        command(["initdb", "-D", str(cluster), "-A", "trust", "-U", "benchmark", "--encoding=UTF8"], "initdb.log")
        port = reserve_port()
        launch(["postgres", "-D", str(cluster), "-h", "127.0.0.1", "-p", str(port),
                "-k", "", "-c", "fsync=on"], "postgres.log")
        import psycopg

        def postgres_ready():
            try:
                connection = psycopg.connect(host="127.0.0.1", port=port, user="benchmark", dbname="postgres")
                connection.close()
                return True
            except psycopg.OperationalError:
                return False

        wait_for(postgres_ready)
        command(["psql", "-h", "127.0.0.1", "-p", str(port), "-U", "benchmark", "-d", "postgres",
                 "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE saleor"], "createdb.log")
        os.environ.update(DATABASE_URL=f"postgres://benchmark@127.0.0.1:{port}/saleor",
                          DJANGO_SETTINGS_MODULE="examples.saleor_dbworker.benchmark_settings",
                          SALEOR_BENCHMARK_MEDIA=str(directory / "media"),
                          DBWORKER_DATABASE_URL=f"sqlite:///{directory / 'requests.db'}",
                          PYTHONPATH=os.pathsep.join((str(ROOT), str(ROOT / "src"))))
        # Historical data migrations publish maintenance tasks. Run those real
        # tasks eagerly during empty-database setup only, never during benchmark
        # execution, so neither stack requires an auxiliary migration broker.
        os.environ["SALEOR_BENCHMARK_SETUP"] = "1"
        command([sys.executable, "-m", "django", "migrate", "--noinput"], "migrations.log")
        os.environ.pop("SALEOR_BENCHMARK_SETUP")
        from examples.saleor_dbworker.adapter import initialize, execute
        initialize()
        from django.db import connections
        from saleor.core import JobStatus
        from saleor.core.db.connection import allow_writer
        from saleor.csv import ExportEvents
        from saleor.csv.events import export_started_event
        from saleor.csv.models import ExportFile
        from saleor.product.models import Product, ProductType, ProductVariant

        count = profile.get("products", 256)
        with allow_writer():
            kind = ProductType.objects.create(name="Benchmark type", slug="benchmark-type", kind="NORMAL")
            products = Product.objects.bulk_create([Product(product_type=kind, name=f"Product {i:04d}",
                                                          slug=f"product-{i:04d}") for i in range(count)])
            ProductVariant.objects.bulk_create([ProductVariant(product=p, sku=f"SKU-{i:04d}", sort_order=0)
                                                for i, p in enumerate(products)])
            exports = [ExportFile.objects.create() for _ in range(profile["requests"] + 2)]
            for export in exports:
                export_started_event(export_file=export)
        # Expected rows are built from the deterministic fixture independently of
        # Saleor's transformation and CSV writer.
        expected = [[base64.b64encode(f"Product:{p.pk}".encode()).decode(), f"Product {i:04d}",
                     "Benchmark type", f"SKU-{i:04d}"] for i, p in enumerate(products)]

        def payload(key):
            return {"export_file_id": key, "scope": {"ids": [p.pk for p in products]},
                    "export_info": {"fields": ["name", "product type", "variant sku"]}, "file_type": "csv"}

        # Validate failure lifecycle outside timing, using the real task's hooks.
        with allow_writer():
            failed = ExportFile.objects.create()
            export_started_event(export_file=failed)
        invalid = payload(failed.pk)
        invalid["export_info"] = {"fields": ["unknown-export-field"]}
        from examples.dbworker_integration.runtime import forbid_task_dispatch
        try:
            with forbid_task_dispatch():
                execute(invalid)
        except KeyError:
            pass
        else:
            raise AssertionError("Invalid field unexpectedly succeeded")
        failed.refresh_from_db()
        if failed.status != JobStatus.FAILED or failed.content_file:
            raise AssertionError("Failure hook did not preserve durable failed state")
        if list(failed.events.order_by("pk").values_list("type", flat=True)) != [ExportEvents.EXPORT_PENDING, ExportEvents.EXPORT_FAILED]:
            raise AssertionError("Failure event history is incomplete")
        connections.close_all()
        url = os.environ["DBWORKER_DATABASE_URL"]
        engine = create_engine(url)
        sessions = sessionmaker(engine)
        runtime = coordinator(url, concurrency=2)
        if args.backend == "celery":
            redis_port = reserve_port()
            redis_url = f"redis://127.0.0.1:{redis_port}/0"
            os.environ["BENCHMARK_REDIS_URL"] = redis_url
            launch(["redis-server", "--bind", "127.0.0.1", "--port", str(redis_port), "--dir", str(directory),
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
                    "--concurrency", "2", "--loglevel", "WARNING", "--hostname", f"saleor-{redis_port}@localhost",
                    "--without-gossip", "--without-mingle"], "worker.log")
            from benchmarks.upstream.celery_app import app as celery, execute_request
            wait_for(lambda: celery.control.ping(timeout=1))
        else:
            runtime.start()

        def batch(items):
            started = time.perf_counter()
            with sessions.begin() as session:
                requests = [Request(suite="saleor", payload=payload(item.pk)) for item in items]
                session.add_all(requests)
                session.flush()
                ids = [item.id for item in requests]
            if args.backend == "celery":
                for key in ids:
                    execute_request.delay(key)

            def complete():
                if any(child.poll() is not None for child in children):
                    raise RuntimeError("An owned Saleor service exited")
                with sessions() as session:
                    rows = session.scalars(select(Request).where(Request.id.in_(ids)).order_by(Request.id)).all()
                    if len(rows) == len(ids) and all(row.result is not None for row in rows):
                        return [row.result for row in rows]

            results = wait_for(complete)
            return time.perf_counter() - started, results

        batch(exports[:2])
        seconds, results = batch(exports[2:])
        connections.close_all()
        normalized = []
        for export, result in zip(exports[2:], results, strict=True):
            export.refresh_from_db()
            if export.status != JobStatus.SUCCESS or result["status"] != JobStatus.SUCCESS:
                raise AssertionError("Export did not reach durable success")
            if result["events"] != [ExportEvents.EXPORT_PENDING, ExportEvents.EXPORT_SUCCESS]:
                raise AssertionError("Export did not preserve exact pending/success lifecycle")
            with export.content_file.open("rb") as stream:
                content = stream.read()
            rows = list(csv.reader(io.StringIO(content.decode("utf-8"))))
            if rows != [["id", "name", "product type", "variant sku"], *expected]:
                raise AssertionError("Export CSV differs from the independent product oracle")
            if hashlib.sha256(content).hexdigest() != result["content_sha256"]:
                raise AssertionError("Export file changed after durable completion")
            normalized.append({"rows": rows, "status": result["status"], "events": result["events"]})
        if args.backend == "dbworker":
            with sessions() as session:
                table = runtime.workers["integration"].table
                states = session.execute(select(table.c.execution_status)).scalars().all()
                if len(states) != len(exports) or any(str(state) != "finished" for state in states):
                    raise AssertionError("DBWorker completion ledger is incomplete")
        row = {"scenario": "product_csv_export", "comparison_mode": "paired_durable_request",
               "backend": args.backend, "repetition": args.repetition, "status": "passed",
               "metrics": {"wall_seconds": seconds, "exports_per_second": len(results) / seconds},
               "validation": {"passed": True, "output_digest": hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest(),
                              "exports": len(results), "products_per_export": count, "failure_lifecycle": "passed"},
               "environment": {"python": platform.python_version(), "interpreter": sys.executable,
                               "packages": {name: importlib.metadata.version(name) for name in ("saleor", "Django", "celery", "sqlalchemy", "dbworker")},
                               "postgres": subprocess.check_output(["postgres", "--version"], text=True).strip()}}
        row["configuration"] = {
            "concurrency": 2, "business_database": "postgresql", "postgres_fsync": True,
            "replica": "same primary", "request_database": "sqlite",
            "comparison_mode": "paired_durable_request", "redis_aof": "everysec",
            "plugins": [], "warmup_requests": 2, "profile": profile,
            "setup": "complete upstream migrations; eager maintenance tasks on empty database outside timing",
            "retry": "no automatic retry", "publication": "single pass after durable request commit",
        }
        row["dataset"] = {"products": count, "variants": count,
                          "fields": ["id", "name", "product type", "variant sku"],
                          "generation": "Product i, SKU-i, one variant per product",
                          "exports": len(results)}
        row["capabilities"] = {
            "verified": ["real_product_csv_export", "independent_csv_oracle", "durable_job_success",
                         "exact_export_events", "untimed_failure_lifecycle", "dbworker_ledger"],
            "untested": ["crash_recovery", "replica_lag", "webhook_delivery", "email_delivery",
                         "native_celery_delivery_lifecycle", "outbox_recovery"],
            "scope": "Paired durable-request replacement of the real export body and upstream job hooks; plugins disabled equally",
        }
        write_json(directory / "sample.json", row)
    finally:
        if runtime is not None:
            runtime.stop()
        try:
            from django.db import connections
            connections.close_all()
        except Exception:
            pass
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
