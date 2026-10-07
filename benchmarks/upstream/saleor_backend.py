"""Benchmark Saleor's native export and separate admin-email workflow."""

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
sys.path.insert(0, str(ROOT / "examples" / "saleor"))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from benchmarks.common.reporting import write_json
from examples.saleor_dbworker.runtime import Job, STAGES, coordinator, enqueue
from benchmarks.common.native_observer import operation
from benchmarks.common.workflow_graph import read_trace, validate_graph
from benchmarks.common.native_admission import check_original_tasks


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
    runtime = engine = smtp = None

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
        from aiosmtpd.controller import Controller
        from email import policy
        from email.parser import BytesParser

        class Sink:
            def __init__(self):
                self.messages = []

            async def handle_DATA(self, server, session, envelope):
                self.messages.append((list(envelope.rcpt_tos), envelope.content))
                return "250 Accepted"

        sink = Sink()
        smtp_port = reserve_port()
        smtp = Controller(sink, hostname="127.0.0.1", port=smtp_port)
        smtp.start()
        os.environ.update(EMAIL_URL=f"smtp://127.0.0.1:{smtp_port}",
                          DEFAULT_FROM_EMAIL="benchmark@example.test",
                          BENCHMARK_TRACE_PATH=str((directory / "task-trace.jsonl").resolve()),
                          BENCHMARK_TASK_STAGES=json.dumps(STAGES), BENCHMARK_BACKEND=args.backend)
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
                          PYTHONPATH=os.pathsep.join((str(ROOT / "examples" / "saleor"), str(ROOT), str(ROOT / "src"))))
        # Historical data migrations publish maintenance tasks. Run those real
        # tasks eagerly during empty-database setup only, never during benchmark
        # execution, so neither stack requires an auxiliary migration broker.
        os.environ["SALEOR_BENCHMARK_SETUP"] = "1"
        command([sys.executable, "-m", "django", "migrate", "--noinput"], "migrations.log")
        os.environ.pop("SALEOR_BENCHMARK_SETUP")
        from examples.saleor_dbworker.adapter import initialize
        initialize()
        from django.db import connections
        from saleor.core import JobStatus
        from saleor.core.db.connection import allow_writer
        from saleor.csv import ExportEvents
        from saleor.csv.events import export_started_event
        from saleor.csv.models import ExportFile
        from saleor.product.models import Product, ProductType, ProductVariant
        from saleor.account.models import User
        from saleor.csv.tasks import export_products_task
        from saleor.celeryconf import app as celery
        from saleor.plugins.admin_email import tasks as email_tasks  # noqa: F401
        native_configuration = {key: celery.conf[key] for key in (
            "task_acks_late", "worker_prefetch_multiplier", "task_reject_on_worker_lost",
            "task_serializer", "task_default_queue", "task_ignore_result")}
        native_execution = check_original_tasks(
            celery, [name for name, stage in STAGES.items() if stage in ("export", "email")],
            ROOT / "examples" / "saleor", expected_application="saleor.celeryconf:app",
            configuration=native_configuration)

        count = profile.get("products", 256)
        with allow_writer():
            kind = ProductType.objects.create(name="Benchmark type", slug="benchmark-type", kind="NORMAL")
            products = Product.objects.bulk_create([Product(product_type=kind, name=f"Product {i:04d}",
                                                          slug=f"product-{i:04d}") for i in range(count)])
            ProductVariant.objects.bulk_create([ProductVariant(product=p, sku=f"SKU-{i:04d}", sort_order=0)
                                                for i, p in enumerate(products)])
            users = [User.objects.create(email=f"export-{i}@example.test", is_staff=True)
                     for i in range(profile["requests"] + 2)]
            exports = [ExportFile.objects.create(user=user) for user in users]
            for export in exports:
                export_started_event(export_file=export)
        # Expected rows are built from the deterministic fixture independently of
        # Saleor's transformation and CSV writer.
        expected = [[base64.b64encode(f"Product:{p.pk}".encode()).decode(), f"Product {i:04d}",
                     "Benchmark type", f"SKU-{i:04d}"] for i, p in enumerate(products)]

        def payload(key):
            return {"export_file_id": key, "scope": {"ids": [p.pk for p in products]},
                    "export_info": {"fields": ["name", "product type", "variant sku"]}, "file_type": "csv"}

        connections.close_all()
        url = os.environ["DBWORKER_DATABASE_URL"]
        engine = create_engine(url)
        sessions = sessionmaker(engine)
        runtime = coordinator(url, concurrency=2)
        if args.backend == "celery":
            redis_port = reserve_port()
            redis_url = f"redis://127.0.0.1:{redis_port}/0"
            os.environ["CELERY_BROKER_URL"] = redis_url
            celery.conf.broker_url = redis_url
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
            launch([sys.executable, "-m", "celery", "-A", "saleor.celeryconf:app", "worker", "-E",
                    "--include", "benchmarks.common.native_observer",
                    "--concurrency", "2", "--loglevel", "WARNING", "--hostname", f"saleor-{redis_port}@localhost"], "worker.log")
            wait_for(lambda: celery.control.ping(timeout=1))
        else:
            runtime.start()

        def batch(items, warmup=False):
            operations = [(f"warmup:{item.pk}" if warmup else str(item.pk)) for item in items]
            started = time.perf_counter()
            for item, op in zip(items, operations, strict=True):
                data = payload(item.pk)
                arguments = (item.pk, data["scope"], data["export_info"], data["file_type"])
                if args.backend == "celery":
                    with operation(op):
                        export_products_task.delay(*arguments)
                else:
                    enqueue("export-products", arguments, {}, op)

            def complete():
                if any(child.poll() is not None for child in children):
                    raise RuntimeError("An owned Saleor service exited")
                connections.close_all()
                if ExportFile.objects.filter(pk__in=[item.pk for item in items],
                                              status=JobStatus.SUCCESS).count() != len(items):
                    return False
                if sum(ExportEvents.EXPORTED_FILE_SENT in list(item.events.values_list("type", flat=True))
                       for item in items) != len(items):
                    return False
                if len([message for item in items for recipients, message in sink.messages
                        if recipients == [item.user.email]]) != len(items):
                    return False
                if args.backend == "dbworker":
                    with sessions() as session:
                        jobs = session.scalars(select(Job).where(Job.operation_id.in_(operations))).all()
                        if len(jobs) != len(items) * 2:
                            return False
                        table = runtime.workers["saleor"].table
                        states = session.execute(select(table.c.execution_status).where(
                            table.c.source_id.in_([job.id for job in jobs]))).scalars().all()
                        if len(states) != len(jobs) or any(str(state) != "finished" for state in states):
                            return False
                trace = read_trace(os.environ["BENCHMARK_TRACE_PATH"])
                successes = [event for event in trace if event["operation_id"] in operations
                             and event["event"] == "succeeded"]
                return len(successes) == len(items) * 2

            wait_for(complete)
            return time.perf_counter() - started

        batch(exports[:2], warmup=True)
        seconds = batch(exports[2:])
        connections.close_all()
        if len(sink.messages) != len(exports):
            raise AssertionError("Unexpected SMTP deliveries outside the configured workload")
        normalized = []
        for export in exports[2:]:
            export.refresh_from_db()
            if export.status != JobStatus.SUCCESS:
                raise AssertionError("Export did not reach durable success")
            events = list(export.events.values_list("type", flat=True))
            if sorted(events) != sorted([ExportEvents.EXPORT_PENDING, ExportEvents.EXPORT_SUCCESS,
                                         ExportEvents.EXPORTED_FILE_SENT]):
                raise AssertionError("Export did not preserve pending/success/email lifecycle")
            with export.content_file.open("rb") as stream:
                content = stream.read()
            rows = list(csv.reader(io.StringIO(content.decode("utf-8"))))
            if rows != [["id", "name", "product type", "variant sku"], *expected]:
                raise AssertionError("Export CSV differs from the independent product oracle")
            accepted = [data for recipients, data in sink.messages if recipients == [export.user.email]]
            if len(accepted) != 1:
                raise AssertionError("Missing or duplicate export email")
            message = BytesParser(policy=policy.default).parsebytes(accepted[0])
            html = message.get_body(preferencelist=("html",))
            if message["To"] != export.user.email or message["Subject"] != "Your exported products data is ready":
                raise AssertionError("Export notification identity or subject differs")
            if html is None or export.content_file.url not in html.get_content():
                raise AssertionError("Export notification does not contain its artifact download link")
            normalized.append({"rows": rows, "status": export.status, "events": sorted(events),
                               "recipient": export.user.email, "subject": str(message["Subject"])})
        graph = validate_graph(read_trace(os.environ["BENCHMARK_TRACE_PATH"]),
                               [str(item.pk) for item in exports[2:]],
                               {"export": 1, "email": 1}, [("export", "email")])
        row = {"scenario": "product_csv_export", "comparison_mode": "native_application_workflow",
               "backend": args.backend, "repetition": args.repetition, "status": "passed",
               "metrics": {"wall_seconds": seconds, "exports_per_second": len(exports[2:]) / seconds},
               "validation": {"passed": True, "output_digest": hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest(),
                              "exports": len(exports[2:]), "products_per_export": count, "email_messages": len(exports[2:]), "business_jobs": graph["nodes"]},
               "environment": {"python": platform.python_version(), "interpreter": sys.executable,
                               "packages": {name: importlib.metadata.version(name) for name in ("saleor", "Django", "celery", "sqlalchemy", "dbworker")},
                               "postgres": subprocess.check_output(["postgres", "--version"], text=True).strip()}}
        row["configuration"] = {
            "concurrency": 2, "business_database": "postgresql", "postgres_fsync": True,
            "replica": "same primary", "coordination_database": "sqlite" if args.backend == "dbworker" else "redis",
            "comparison_mode": "native_application_workflow", "redis_aof": "everysec",
            "plugins": "upstream defaults; admin email active; no webhook subscriptions", "warmup_requests": 2, "profile": profile,
            "setup": "complete upstream migrations; eager maintenance tasks on empty database outside timing",
            "retry": "upstream export and email tasks do not declare automatic retry",
            "publication": "one task per export; one separately queued email task per export",
            "environment_overrides": {
                "DATABASE_URL": "owned PostgreSQL cluster; replica alias uses same primary",
                "CELERY_BROKER_URL": "owned Redis instance for the native baseline",
                "EMAIL_URL": "owned local SMTP receiver; upstream templates and transport",
                "MEDIA_ROOT": "owned filesystem artifact directory",
                "STORAGES": "local filesystem instead of cloud infrastructure",
                "CACHES": "process-local cache; no external deployment cache required",
                "CELERY_TASK_ALWAYS_EAGER": "setup-only eager maintenance tasks; disabled during measurement",
                "worker_concurrency": "two processes per backend for the entire workflow",
            },
        }
        row["dataset"] = {"products": count, "variants": count,
                          "fields": ["id", "name", "product type", "variant sku"],
                          "generation": "Product i, SKU-i, one variant per product",
                          "exports": len(exports[2:])}
        row["capabilities"] = {
            "verified": ["native_export_task", "real_product_csv_export", "independent_csv_oracle",
                         "durable_job_success", "export_and_email_events", "separate_email_jobs",
                         "real_smtp_acceptance", "observed_business_job_graph"],
            "untested": ["crash_recovery", "replica_lag", "webhook_delivery", "failure_and_retry_lifecycle", "outbox_recovery"],
            "scope": "Native Saleor export and separate admin-email workflow; upstream plugins restored, no webhook subscriptions",
        }
        row["workflow_graph"] = graph
        trace_path = Path(os.environ["BENCHMARK_TRACE_PATH"])
        row["workflow_trace"] = {
            "path": str(trace_path.relative_to(Path(config["output_directory"]))),
            "sha256": hashlib.sha256(trace_path.read_bytes()).hexdigest(),
        }
        if args.backend == "celery":
            row["native_execution"] = native_execution
        row["configuration"]["native_celery"] = native_configuration
        row["configuration"]["timing_boundary"] = "task publication through export, SMTP acceptance, email event and both completed jobs"
        write_json(directory / "sample.json", row)
    finally:
        if smtp is not None:
            smtp.stop()
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
