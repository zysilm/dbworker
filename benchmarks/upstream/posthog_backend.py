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

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from benchmarks.common.reporting import write_json
from examples.dbworker_integration.runtime import Request, coordinator


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
    directory = Path(config["output_directory"]) / f"posthog-{args.repetition}-{args.backend}"
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
        for binary in ("initdb", "postgres", "psql", "redis-server"):
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
                          EMAIL_HOST_USER="", EMAIL_HOST_PASSWORD="", BENCHMARK_INITIALIZE_ADAPTER="posthog")
        from examples.posthog_dbworker.bootstrap import initialize
        initialize()
        from django.db import connection
        from posthog.models.messaging import MessagingRecord
        from posthog.models.instance_setting import InstanceSetting
        from posthog.email import EmailMessage
        with connection.schema_editor() as editor:
            editor.create_model(InstanceSetting)
            editor.create_model(MessagingRecord)

        payloads, expected = [], {}
        for index in range(profile["requests"] + 2):
            recipient = f"recipient-{index:04d}@benchmark.invalid"
            message = EmailMessage(campaign_key=f"benchmark-{index:04d}", template_name="2fa_enabled",
                                   subject=f"PostHog notification {index:04d}",
                                   template_context={"user_name": f"Benchmark User {index:04d}", "user_email": recipient},
                                   headers={"X-Benchmark": f"fixture-{index:04d}"})
            message.add_recipient(recipient)
            if (f"Benchmark User {index:04d}" not in message.html_body or recipient not in message.html_body
                    or "two-factor authentication (2FA)" not in message.html_body
                    or not message.html_body.startswith("<!DOCTYPE html>") or "style=" not in message.html_body):
                raise AssertionError("The upstream template failed independent fixture semantics")
            message.txt_body = f"Notification {index:04d}: two-factor authentication enabled. Unicode: café."
            payload = {"campaign_key": message.campaign_key, "to": message.to, "subject": message.subject,
                       "headers": message.headers, "txt_body": message.txt_body, "html_body": message.html_body,
                       "template_name": message.template_name, "reply_to": message.reply_to,
                       "use_http": False, "properties": message.properties}
            payloads.append(payload)
            expected[recipient] = {"recipient": recipient, "subject": message.subject,
                                   "text": message.txt_body, "html": message.html_body,
                                   "fixture": message.headers["X-Benchmark"]}
        engine = create_engine(url)
        sessions = sessionmaker(engine)
        runtime = coordinator(url, concurrency=2)
        if args.backend == "celery":
            redis_port = port()
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
                    "--concurrency", "2", "--loglevel", "WARNING", "--hostname", f"posthog-{redis_port}@localhost",
                    "--without-gossip", "--without-mingle"], "worker.log")
            from benchmarks.upstream.celery_app import app as celery, execute_request
            wait_for(lambda: celery.control.ping(timeout=1))
        else:
            runtime.start()

        def execute_batch(values):
            started = time.perf_counter()
            with sessions.begin() as session:
                requests = [Request(suite="posthog", payload=payload) for payload in values]
                session.add_all(requests)
                session.flush()
                identities = [request.id for request in requests]
            if args.backend == "celery":
                for identity in identities:
                    execute_request.delay(identity)
            def complete():
                if any(child.poll() is not None for child in children):
                    raise RuntimeError("A PostHog backend service exited")
                with sessions() as session:
                    rows = session.scalars(select(Request).where(Request.id.in_(identities)).order_by(Request.id)).all()
                    if len(rows) == len(values) and all(row.result is not None for row in rows):
                        return [row.result for row in rows]
                return None
            return_results = wait_for(complete)
            return time.perf_counter() - started, return_results

        execute_batch(payloads[:2])
        seconds, results = execute_batch(payloads[2:])
        expected_recipients = sorted(value["to"][0]["raw_email"] for value in payloads[2:])
        normalized, ids = [], set()
        for envelope in sink.messages:
            if len(envelope["rcpt_tos"]) != 1:
                raise AssertionError("Unexpected SMTP recipient count")
            recipient = envelope["rcpt_tos"][0]
            fixture = expected[recipient]
            message = BytesParser(policy=policy.default).parsebytes(envelope["content"])
            plain = message.get_body(preferencelist=("plain",)).get_content().replace("\r\n", "\n").rstrip("\n")
            html = message.get_body(preferencelist=("html",)).get_content().replace("\r\n", "\n").rstrip("\n")
            actual = {"recipient": recipient, "subject": str(message["Subject"]), "text": plain, "html": html,
                      "fixture": str(message["X-Benchmark"])}
            oracle = {**fixture, "html": fixture["html"].replace("\r\n", "\n").rstrip("\n")}
            if actual != oracle or envelope["mail_from"] != "sender@benchmark.invalid":
                raise AssertionError("PostHog SMTP message differs from its real rendered fixture")
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
        if len(sink.messages) != len(payloads) or sorted(row["recipient"] for row in normalized) != expected_recipients:
            raise AssertionError("Missing or duplicate SMTP acceptance")
        if sorted(row["accepted_recipients"][0] for row in results) != expected_recipients:
            raise AssertionError("Delivery records disagree with receiver acceptance")
        if MessagingRecord.objects.filter(sent_at__isnull=False).count() != len(payloads):
            raise AssertionError("Upstream delivery ledger is incomplete")
        if args.backend == "dbworker":
            with sessions() as session:
                table = runtime.workers["integration"].table
                states = session.scalars(select(table.c.execution_status)).all()
                if len(states) != len(payloads) or any(str(state) != "finished" for state in states):
                    raise AssertionError("DBWorker completion ledger is incomplete")
        normalized.sort(key=lambda item: item["recipient"])
        from examples.posthog_dbworker.bootstrap import SOURCE, PROJECTIONS
        used = {"email.py", "models/messaging.py", "models/instance_setting.py", "uuidt.py", "helpers/email_utils.py",
                "settings/dynamic_settings.py", "templatetags/posthog_assets.py", "templatetags/posthog_filters.py",
                "templates/email/2fa_enabled.html", "templates/email/base.html", "templates/email/styles.html"}
        used.update(value[0] for value in PROJECTIONS.values())
        source_files = {relative: hashlib.sha256((SOURCE / relative).read_bytes()).hexdigest() for relative in sorted(used)}
        packages = {name: importlib.metadata.version(name) for name in ("django", "celery", "css-inline", "sqlalchemy", "posthoganalytics")}
        row = {"scenario": "pre_rendered_notification_smtp", "comparison_mode": "paired_durable_request",
               "backend": args.backend, "repetition": args.repetition, "status": "passed",
               "metrics": {"wall_seconds": seconds, "messages_per_second": profile["requests"] / seconds},
               "validation": {"passed": True, "output_digest": hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest(),
                              "messages": len(normalized), "warmup_messages": 2, "duplicate_acceptance": 0},
               "environment": {"python": platform.python_version(), "interpreter": sys.executable, "packages": packages,
                               "upstream_files": source_files, "source_projections": PROJECTIONS},
               "configuration": {"concurrency": 2, "database": "postgresql", "transport": "real_local_smtp",
                                 "rendering_timed": False, "rendering_validated": True,
                                 "application_scope": "unchanged PostHog notification models, rendering and SMTP business code; scoped bootstrap",
                                 "celery_publication": "single pass after durable request commit", "fault_recovery": "untested"},
               "dataset": {"template": "2fa_enabled", "requests": profile["requests"], "warmup_requests": 2,
                           "content": "deterministic recipients and Unicode notification content"},
               "capabilities": {"verified": ["upstream_template_rendering", "css_inlining", "smtp_acceptance", "messaging_records", "completion_ledger"],
                                "untested": ["whole_posthog_application", "clickhouse", "delivery_retries", "ambiguous_acceptance_recovery",
                                             "campaign_deduplication", "salt_rotation", "rejection_and_error_capture"]}}
        write_json(directory / "sample.json", row)
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
