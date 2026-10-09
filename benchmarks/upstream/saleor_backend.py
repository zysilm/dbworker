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

from benchmarks.common.load import run_load
from benchmarks.common.load_evidence import task_load_metrics
from benchmarks.common.reporting import write_json
from examples.saleor_dbworker.runtime import Job, STAGES, coordinator, durable_children
from benchmarks.common.native_observer import operation, argument_digest
from benchmarks.common.workflow_graph import read_trace, validate_graph
from benchmarks.common.native_admission import check_original_tasks
from benchmarks.common.timing_evidence import begin_window, end_window, elapsed_seconds
from examples.saleor_dbworker.producer import post_export, export_key


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
        # Generate an isolated signing key in memory: original JWT authentication
        # runs normally, and upstream debug-key creation must not modify checkout.
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        signing_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        os.environ["RSA_PRIVATE_KEY"] = signing_key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption()).decode()
        from examples.saleor_dbworker.adapter import initialize
        initialize()
        from django.db import connections
        from saleor.core import JobStatus
        from saleor.core.db.connection import allow_writer
        from saleor.csv import ExportEvents
        from saleor.csv.models import ExportFile
        from saleor.product.models import Product, ProductType, ProductVariant
        from saleor.account.models import User
        from saleor.permission.models import Permission
        from saleor.permission.enums import ProductPermissions
        from saleor.core.jwt import create_access_token
        from django.test import Client
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
            permission = Permission.objects.get(codename="manage_products", content_type__app_label="product")
            for user in users:
                user.user_permissions.add(permission)
            denied_user = User.objects.create(email="denied@example.test", is_staff=True)
        if any(user.is_superuser or not user.has_perm(ProductPermissions.MANAGE_PRODUCTS.value)
               for user in users):
            raise AssertionError("Export fixtures must have real MANAGE_PRODUCTS permission without superuser bypass")
        graphql_client = Client()
        tokens = [create_access_token(user) for user in users]
        product_ids = [base64.b64encode(f"Product:{product.pk}".encode()).decode() for product in products]
        # Exercise real JWT/permission enforcement outside measurement. A denied
        # request must neither create an export nor publish any business job.
        before = ExportFile.objects.count()
        denied = post_export(graphql_client, create_access_token(denied_user), product_ids)
        errors = denied.get("errors") or []
        if not any((error.get("extensions") or {}).get("exception", {}).get("code") == "PermissionDenied"
                   for error in errors) or ExportFile.objects.count() != before:
            raise AssertionError("Original GraphQL export permission enforcement failed")
        # Expected rows are built from the deterministic fixture independently of
        # Saleor's transformation and CSV writer.
        expected = [[base64.b64encode(f"Product:{p.pk}".encode()).decode(), f"Product {i:04d}",
                     "Benchmark type", f"SKU-{i:04d}"] for i, p in enumerate(products)]

        connections.close_all()
        url = os.environ["DBWORKER_DATABASE_URL"]
        engine = create_engine(url)
        sessions = sessionmaker(engine)
        runtime = coordinator(url, concurrency=profile.get("worker_concurrency", 8))
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
                    "--concurrency", str(profile.get("worker_concurrency", 8)), "--loglevel", "WARNING", "--hostname", f"saleor-{redis_port}@localhost"], "worker.log")
            wait_for(lambda: celery.control.ping(timeout=1))
        else:
            runtime.start()

        operation_bindings = []

        def batch(indices, warmup=False):
            operations = [f"warmup:{i}" if warmup else str(i - 2) for i in indices]
            items = []
            started = begin_window()
            def producer_factory(producer_index):
                graphql_client = Client()
                def submit(index, position):
                    op = operations[position]
                    try:
                        with operation(op):
                            if args.backend == "celery":
                                content = post_export(graphql_client, tokens[index], product_ids)
                            else:
                                # The original mutation owns permission checks,
                                # export creation and unchanged task publication.
                                with durable_children(op, None):
                                    content = post_export(graphql_client, tokens[index], product_ids)
                        return {"export_file_id": export_key(content), "user_index": index, "operation_id": op}
                    finally:
                        # Django connections are thread-local; never leak them
                        # across producer threads or retain transaction state.
                        connections.close_all()
                return submit
            load = run_load(indices, None, producers=1 if warmup else profile.get("producers", 8),
                            duration_seconds=0 if warmup else profile.get("submission_window_seconds", 60),
                            producer_factory=producer_factory)
            # Inspect publications once, after concurrent requests, rather than
            # repeatedly scanning the growing trace during load generation.
            observed_submissions = {}
            for event in read_trace(os.environ["BENCHMARK_TRACE_PATH"]):
                if event["stage"] == "export" and event["event"] == "submitted":
                    observed_submissions.setdefault(event["operation_id"], []).append(event)
            for submitted in load["results"]:
                index, op = submitted["user_index"], submitted["operation_id"]
                item = ExportFile.objects.get(pk=submitted["export_file_id"])
                if item.user_id != users[index].pk:
                    raise AssertionError("GraphQL producer associated an incorrect authenticated user")
                submissions = observed_submissions.get(op, [])
                if len(submissions) != 1:
                    raise AssertionError("Original GraphQL producer did not publish exactly one export task")
                expected_arguments = (item.pk, {"ids": [str(product.pk) for product in products]},
                                      {"fields": ["name", "product type", "variant sku"]}, "csv")
                expected_fingerprint = argument_digest(expected_arguments, {})
                if submissions[0]["details"].get("argument_sha256") != expected_fingerprint:
                    raise AssertionError("Original GraphQL publication does not match returned export and public input")
                operation_bindings.append({"operation_id": op, "export_file_id": item.pk,
                                           "user_email": users[index].email,
                                           "root_node_id": submissions[0]["node_id"],
                                           "root_argument_sha256": expected_fingerprint})
                items.append(item)

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
            return items, end_window(started), load

        warmup_exports, _, _ = batch(list(range(2)), warmup=True)
        measured_exports, measurement_window, load = batch(list(range(2, len(users))))
        exports = warmup_exports + measured_exports
        seconds = elapsed_seconds(measurement_window)
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
                               [str(index) for index in range(profile["requests"])],
                               {"export": 1, "email": 1}, [("export", "email")],
                               warmup_operations=["warmup:0", "warmup:1"])
        row = {"scenario": "product_csv_export", "comparison_mode": "native_application_workflow",
               "backend": args.backend, "repetition": args.repetition, "status": "passed",
               "metrics": {"wall_seconds": seconds, "exports_per_second": len(exports[2:]) / seconds,
                           "load": {key: value for key, value in load.items() if key != "results"},
                           "task_load": task_load_metrics(read_trace(os.environ["BENCHMARK_TRACE_PATH"]),
                                                          [str(index) for index in range(profile["requests"])])},
               "validation": {"passed": True, "output_digest": hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest(),
                              "exports": len(exports[2:]), "products_per_export": count, "email_messages": len(exports[2:]), "business_jobs": graph["nodes"]},
               "environment": {"python": platform.python_version(), "interpreter": sys.executable,
                               "packages": {name: importlib.metadata.version(name) for name in ("saleor", "Django", "celery", "sqlalchemy", "dbworker")},
                               "postgres": subprocess.check_output(["postgres", "--version"], text=True).strip()}}
        row["configuration"] = {
            "concurrency": profile.get("worker_concurrency", 8), "business_database": "postgresql", "postgres_fsync": True,
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
                "worker_concurrency": "eight processes per backend for the entire workflow",
            },
        }
        row["dataset"] = {"products": count, "variants": count,
                          "fields": ["id", "name", "product type", "variant sku"],
                          "generation": "Product i, SKU-i, one variant per product",
                          "exports": len(exports[2:])}
        row["capabilities"] = {
            "verified": ["authenticated_graphql_export_producer", "real_manage_products_permission", "native_export_task", "real_product_csv_export", "independent_csv_oracle",
                         "durable_job_success", "export_and_email_events", "separate_email_jobs",
                         "real_smtp_acceptance", "observed_business_job_graph"],
            "untested": ["crash_recovery", "replica_lag", "webhook_delivery", "failure_and_retry_lifecycle", "outbox_recovery"],
            "scope": "Original authenticated GraphQL ExportProducts mutation and separate admin-email workflow; upstream plugins restored, no webhook subscriptions",
        }
        row["measurement_window"] = measurement_window
        row["warmup_operations"] = ["warmup:0", "warmup:1"]
        row["operation_binding"] = {
            "producer": "saleor.graphql.csv.mutations.export_products.ExportProducts",
            "entrypoint": "authenticated POST /graphql/ exportProducts",
            "public_input": {"product_ids": [str(product.pk) for product in products],
                             "fields": ["name", "product type", "variant sku"], "file_type": "csv"},
            "bindings": operation_bindings,
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
        row["configuration"]["timing_boundary"] = "authenticated GraphQL request including producer normalization, permissions, export creation and publication through export, SMTP acceptance, email event and both completed jobs"
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
