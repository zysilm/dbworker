"""Benchmark authentic PostHog rendering, delivery records and SMTP acceptance."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
from email import policy
from email.parser import BytesParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "examples/posthog"))

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from dbworker import ExecutionStatus

from benchmarks.common.reporting import write_json
from examples.posthog_dbworker.runtime import Job, STAGES, coordinator, enqueue
from benchmarks.common.native_observer import operation
from benchmarks.common.native_admission import check_original_tasks
from benchmarks.common.workflow_graph import read_trace, validate_graph
from benchmarks.common.timing_evidence import begin_window, end_window, elapsed_seconds, validate_timing_window
from examples.posthog_dbworker import producer


def port():
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        return reservation.getsockname()[1]


def wait_for(predicate, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.05)
    raise TimeoutError("PostHog did not reach durable delivery before the deadline")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--backend", required=True, choices=("celery", "dbworker"))
    parser.add_argument("--repetition", required=True, type=int)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    profile = config["suite"]["profiles"][config["profile"]]
    directory = Path(config["output_directory"]).resolve() / f"posthog-{args.repetition}-{args.backend}"
    directory.mkdir()
    children, streams = [], []
    runtime = engine = smtp = None

    def launch(command, name):
        stream = (directory / name).open("wb")
        streams.append(stream)
        child = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT)
        children.append(child)
        return child

    def command(command, name):
        with (directory / name).open("wb") as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)

    try:
        for binary in ("initdb", "postgres", "psql", "pg_dump", "pg_restore", "redis-server"):
            if not shutil.which(binary):
                raise FileNotFoundError(f"Missing local service binary: {binary}")
        cluster = directory / "postgres"
        command(["initdb", "-D", str(cluster), "-A", "trust", "-U", "benchmark", "--encoding=UTF8"], "initdb.log")
        pg_port = port()
        launch(["postgres", "-D", str(cluster), "-h", "127.0.0.1", "-p", str(pg_port),
                "-k", ""], "postgres.log")
        import psycopg
        pg = f"host=127.0.0.1 port={pg_port} user=benchmark dbname=postgres"
        def pg_ready():
            try:
                with psycopg.connect(pg):
                    return True
            except psycopg.OperationalError:
                return False
        wait_for(pg_ready)
        with psycopg.connect(pg, autocommit=True) as connection:
            connection.execute("CREATE DATABASE notification")
            connection.execute("CREATE DATABASE requests")
        url = f"postgresql+psycopg://benchmark@127.0.0.1:{pg_port}/requests"
        os.environ.update(DBWORKER_DATABASE_URL=url, POSTHOG_BENCHMARK_DATABASE="notification",
                          POSTHOG_BENCHMARK_PG_PORT=str(pg_port), POSTHOG_BENCHMARK_PG_USER="benchmark",
                          PYTHONPATH=str(ROOT) + os.pathsep + str(ROOT / "src"))
        from aiosmtpd.controller import Controller
        class Sink:
            def __init__(self):
                self.messages = []
            async def handle_DATA(self, server, session, envelope):
                self.messages.append({"mail_from": envelope.mail_from, "rcpt_tos": envelope.rcpt_tos,
                                      "content": bytes(envelope.original_content)})
                return "250 Message accepted"
        sink = Sink()
        smtp_port = port()
        smtp = Controller(sink, hostname="127.0.0.1", port=smtp_port)
        smtp.start()
        os.environ.update(EMAIL_HOST="127.0.0.1", EMAIL_PORT=str(smtp_port),
                          EMAIL_DEFAULT_FROM="sender@benchmark.invalid", EMAIL_REPLY_TO="reply@benchmark.invalid",
                          EMAIL_USE_TLS="false", EMAIL_USE_SSL="false", EMAIL_TIMEOUT="30", EMAIL_ENABLED="true",
                          EMAIL_HOST_USER="", EMAIL_HOST_PASSWORD="")
        redis_port = port()
        redis_url = f"redis://127.0.0.1:{redis_port}/0"
        os.environ.update(BENCHMARK_REDIS_URL=redis_url, REDIS_URL=redis_url,
                          DATABASE_URL=f"postgres://benchmark@127.0.0.1:{pg_port}/notification",
                          OPT_OUT_CAPTURE="true", OTEL_SDK_DISABLED="true", TEST="false",
                          SECRET_KEY="isolated-posthog-native-benchmark-only-secret",
                          BENCHMARK_BACKEND=args.backend, CELERY_METRICS_PORT=str(port()))
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
        from examples.posthog_dbworker.bootstrap import initialize
        app = initialize()
        from benchmarks.upstream.posthog_setup import prepare_native_schema
        setup = prepare_native_schema(output_directory=config['output_directory'], arm_directory=directory,
            source_root=ROOT / 'examples/posthog', pg_port=pg_port,
            rebuild=os.environ.get('POSTHOG_BENCHMARK_REBUILD_SETUP_CACHE') == '1')
        from posthog.models import User
        from posthog.models.messaging import MessagingRecord
        native_execution = check_original_tasks(app, list(STAGES), ROOT / "examples/posthog",
            expected_application="posthog.celery:app", configuration={
                "native_settings": "posthog.settings", "task_queue": "email",
                "overrides": {"database": "owned PostgreSQL", "broker_and_cache": "owned Redis",
                    "smtp": "owned local receiver", "CUSTOMER_IO_API_KEY": "empty for native SMTP fallback",
                    "OTEL_SDK_DISABLED": True, "OPT_OUT_CAPTURE": True, "TEST": False},
                "autoretry": "original Exception max3 with backoff/jitter",
                "worker_prefetch_multiplier": app.conf.worker_prefetch_multiplier,
                "worker_total_concurrency": 2, "dbworker_shared_stage_pool": 2,
                "task_acks_late": app.conf.task_acks_late})
        users, expected, fixtures = [], {}, []
        for index in range(profile["requests"] + 2):
            recipient = f"recipient-{index:04d}@benchmark.invalid"
            user = User.objects.create_user(email=recipient, password=None,
                first_name=f"Benchmark User {index:04d}", distinct_id=f"benchmark-user-{index:04d}")
            users.append(user)
            fixtures.append(producer.prepare(user))
            expected[recipient] = {"recipient": recipient, "name": user.first_name,
                                  "subject": "You've enabled 2FA protection"}
        trace = directory / "workflow.jsonl"
        os.environ.update(BENCHMARK_TRACE_PATH=str(trace), BENCHMARK_TASK_STAGES=json.dumps(STAGES),
                          PYTHONPATH=os.pathsep.join((str(ROOT), str(ROOT / "src"), str(ROOT / "examples/posthog"))))
        engine = create_engine(url)
        sessions = sessionmaker(engine)
        runtime = coordinator(url, concurrency=2) if args.backend == "dbworker" else None
        if args.backend == "celery":
            launch([sys.executable, "-m", "celery", "-A", "posthog.celery:app", "worker",
                    "--include", "benchmarks.common.native_observer", "--queues", "email",
                    "--concurrency", "2", "--loglevel", "WARNING", "--hostname", f"posthog-{redis_port}@localhost"], "worker.log")
            wait_for(lambda: app.control.ping(timeout=1))
        else:
            runtime.start()

        warmup_operations = [f"warmup:notification-{i:04d}" for i in range(2)]
        producer_identities, api_outcomes = [], []

        def execute_batch(values, fixture_values, warmup=False):
            ops = [("warmup:" if warmup else "") + f"notification-{i:04d}" for i in range(len(values))]
            started = begin_window()
            for op, user, fixture in zip(ops, values, fixture_values, strict=True):
                # Capture identity before the original endpoint can publish work.
                producer_identities.append(producer.identity(fixture, op))
                with operation(op):
                    if args.backend == "celery":
                        outcome = producer.validate(fixture)
                    else:
                        def submit(user_id):
                            with sessions.begin() as session:
                                job = enqueue(session, op, user_id)
                                session.flush()
                                return job.id
                        with producer.route_notification(submit, user.pk):
                            outcome = producer.validate(fixture)
                api_outcomes.append({"operation_id": op, **outcome})
            recipients = {user.email for user in values}
            latest_graph_error = None
            def complete():
                nonlocal latest_graph_error
                if any(child.poll() is not None for child in children):
                    raise RuntimeError("A native PostHog service exited")
                accepted = [row for row in sink.messages if row["rcpt_tos"][0] in recipients]
                if len(accepted) != len(values):
                    return False
                try:
                    graph = validate_graph(read_trace(trace), ops,
                        {"notification": 1, "delivery": 1}, [("notification", "delivery")],
                        warmup_operations=[] if warmup else warmup_operations)
                except ValueError as error:
                    latest_graph_error = str(error)
                    return False
                if args.backend == "dbworker":
                    with sessions() as session:
                        if session.query(Job).filter(Job.operation_id.in_(ops), Job.complete.is_(True)).count() != 2 * len(values):
                            return False
                        jobs = session.scalars(select(Job).where(Job.operation_id.in_(ops))).all()
                        if any(runtime.execution_status(session, worker="posthog_workflow", source_id=job.id)
                               != ExecutionStatus.FINISHED for job in jobs):
                            return False
                return graph
            try:
                graph = wait_for(complete, timeout=max(180, len(values) * 10))
            except TimeoutError as error:
                accepted = sum(row['rcpt_tos'][0] in recipients for row in sink.messages)
                raise TimeoutError(f'{error}; accepted SMTP messages: {accepted}/{len(values)}; '
                                   f'latest workflow admission error: {latest_graph_error}') from error
            window = end_window(started)
            seconds = elapsed_seconds(window)
            validate_timing_window(window, seconds, read_trace(trace), ops)
            return seconds, graph, window
        execute_batch(users[:2], fixtures[:2], warmup=True)
        seconds, graph, measurement_window = execute_batch(users[2:], fixtures[2:])
        expected_recipients = sorted(user.email for user in users[2:])
        normalized, ids = [], set()
        for envelope in sink.messages:
            if len(envelope["rcpt_tos"]) != 1:
                raise AssertionError("Unexpected SMTP recipient count")
            recipient = envelope["rcpt_tos"][0]
            fixture = expected[recipient]
            message = BytesParser(policy=policy.default).parsebytes(envelope["content"])
            plain_part = message.get_body(preferencelist=("plain",))
            plain = plain_part.get_content().replace("\r\n", "\n").rstrip("\n") if plain_part else ""
            html = message.get_body(preferencelist=("html",)).get_content().replace("\r\n", "\n").rstrip("\n")
            actual = {"recipient": recipient, "subject": str(message["Subject"]), "text": plain, "html": html}
            if (actual["subject"] != fixture["subject"] or plain != "" or fixture["name"] not in html
                    or recipient not in html or "two-factor authentication (2FA)" not in html
                    or not html.startswith("<!DOCTYPE html>") or "style=" not in html
                    or envelope["mail_from"] != "sender@benchmark.invalid"):
                raise AssertionError("Native notification content differs from the independent user oracle")
            if message["To"].addresses[0].addr_spec != recipient or message["From"].addresses[0].addr_spec != envelope["mail_from"]:
                raise AssertionError("SMTP envelope and MIME identity differ")
            if message["Reply-To"].addresses[0].addr_spec != "reply@benchmark.invalid" or not message["Date"]:
                raise AssertionError("Delivery lost configured MIME headers")
            message_id = str(message["Message-ID"])
            if not message_id or message_id == "None" or message_id in ids:
                raise AssertionError("Missing or duplicate transport message identity")
            ids.add(message_id)
            if recipient in expected_recipients:
                normalized.append(actual)
        if len(sink.messages) != len(users) or sorted(row["recipient"] for row in normalized) != expected_recipients:
            raise AssertionError("Missing or duplicate SMTP acceptance")
        records = list(MessagingRecord.objects.order_by("campaign_key"))
        if len(records) != len(users) or any(record.sent_at is None for record in records):
            raise AssertionError("Native delivery ledger is incomplete")
        from posthog.models.messaging import get_email_hash
        for user in users:
            matching = [record for record in records if record.email_hash == get_email_hash(user.email)]
            if len(matching) != 1 or not matching[0].campaign_key.startswith(f"2fa_enabled_{user.uuid}-"):
                raise AssertionError("Native user/campaign mapping differs")
        normalized.sort(key=lambda item: item["recipient"])
        source = ROOT / "examples/posthog/posthog"
        source_files = {relative: hashlib.sha256((source / relative).read_bytes()).hexdigest()
                        for relative in ("email.py", "tasks/email.py", "celery.py", "settings/celery.py", "api/user.py",
                                         "helpers/session_cache.py", "session/activity.py")}
        packages = {name: importlib.metadata.version(name) for name in ("django", "celery", "css-inline", "sqlalchemy", "posthoganalytics")}
        row = {"scenario": "native_two_factor_notification", "comparison_mode": "native_execution",
               "backend": args.backend, "repetition": args.repetition, "status": "passed",
               "metrics": {"wall_seconds": seconds, "messages_per_second": profile["requests"] / seconds,
                           "cpu_seconds": None, "peak_rss_bytes": None},
               "unavailable_metrics": {"cpu_seconds": "Not collected by this suite",
                                       "peak_rss_bytes": "Not collected by this suite"},
               "validation": {"passed": True, "output_digest": hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest(),
                              "messages": len(normalized), "warmup_messages": 2, "duplicate_acceptance": 0},
               "environment": {"python": platform.python_version(), "interpreter": sys.executable, "packages": packages,
                               "upstream_files": source_files},
               "configuration": {"concurrency": 2, "database": "postgresql", "transport": "real_local_smtp",
                                 "rendering_timed": True, "rendering_validated": True,
                                 "application_scope": "original authenticated two_factor_validate DRF handler plus notification and delivery",
                                 "producer_api": "posthog.api.user.UserViewSet.two_factor_validate",
                                 "request_transport": "DRF handler with native session authentication, CSRF, permissions and throttles; no full middleware/server",
                                 "producer_api_timed": True, "producer_api_effects_validated": True,
                                 "celery_publication": "original API .delay and native nested delivery publication",
                                 "fault_recovery": "untested"},
               "dataset": {"template": "2fa_enabled", "requests": profile["requests"], "warmup_requests": 2,
                           "content": "real users and original 2FA notification content"},
               "capabilities": {"verified": ["upstream_template_rendering", "css_inlining", "smtp_acceptance", "messaging_records", "two_stage_workflow", "native_task_origin", "two_factor_validation_api",
                                             "totp_device", "session_verification", "setup_cache_cleanup", "other_session_revocation"],
                                "untested": ["whole_http_middleware_stack", "clickhouse_queries", "delivery_retries", "ambiguous_acceptance_recovery",
                                             "campaign_deduplication", "salt_rotation", "rejection_and_error_capture"]}}
        row["measurement_window"] = measurement_window
        row["warmup_operations"] = warmup_operations
        row["producer_identities"] = producer_identities
        row["producer_api_outcomes"] = api_outcomes
        row["producer_execution"] = {
            "passed": True, "api": "posthog.api.user.UserViewSet.two_factor_validate",
            "source_file": "posthog/api/user.py", "sha256": source_files["api/user.py"],
            "measured_calls": profile["requests"], "warmup_calls": 2,
            "effects": ["verified_totp_device", "persistent_session_flags", "setup_cache_cleanup", "other_session_revocation"],
            "root_jobs_per_call": 1, "delivery_jobs_per_call": 1}
        row["native_execution"] = native_execution
        row["setup"] = setup
        row["workflow_graph"] = graph
        row["workflow_trace"] = {"path": str(trace.relative_to(Path(config["output_directory"]).resolve())),
                                 "sha256": hashlib.sha256(trace.read_bytes()).hexdigest()}
        write_json(directory / "sample.json", row)
    except Exception as exc:
        write_json(directory / "admission.json", {"schema_version": 1, "suite_id": "posthog",
            "status": "blocked", "native_execution": {"passed": False},
            "phase": "native_application_admission", "errors": [{"type": type(exc).__name__, "message": str(exc)}],
            "performance_sample_created": False})
        raise
    finally:
        if runtime is not None:
            runtime.stop()
            runtime.session_factory.kw["bind"].dispose()
        if engine is not None:
            engine.dispose()
        if smtp is not None:
            smtp.stop()
        if "django.db" in sys.modules:
            from django.db import connections
            connections.close_all()
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


if __name__ == "__main__":
    main()
