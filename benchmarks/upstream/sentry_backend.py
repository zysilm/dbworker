"""Execute historical Sentry email SMTP delivery through paired subprocess bridges."""

from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
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
    directory = Path(config["output_directory"]) / f"sentry-{args.repetition}-{args.backend}"
    directory.mkdir()
    url = f"sqlite:///{directory / 'requests.db'}"
    os.environ["SENTRY_UPSTREAM_PYTHON"] = config["interpreters"]["upstream_python"]
    os.environ["SENTRY_SOURCE_PATH"] = str(ROOT / "examples/sentry/src")
    os.environ["DBWORKER_DATABASE_URL"] = url
    os.environ["PYTHONPATH"] = str(ROOT) + os.pathsep + str(ROOT / "src")
    from aiosmtpd.controller import Controller
    class Sink:
        def __init__(self):
            self.messages = []
        async def handle_DATA(self, server, session, envelope):
            self.messages.append((envelope.mail_from, envelope.rcpt_tos, envelope.content))
            return "250 Accepted"
    sink = Sink()
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        smtp_port = reservation.getsockname()[1]
    smtp = Controller(sink, hostname="127.0.0.1", port=smtp_port)
    smtp.start()
    os.environ["SENTRY_SMTP_PORT"] = str(smtp_port)
    settings_directory = directory / "sentry-config"
    settings_directory.mkdir()
    (settings_directory / "config.yml").write_text(
        "system.secret-key: isolated-historical-sentry-benchmark\n"
        "mail.backend: smtp\nmail.host: 127.0.0.1\n"
        f"mail.port: {smtp_port}\nmail.use-tls: false\nmail.use-ssl: false\n")
    (settings_directory / "sentry.conf.py").write_text(
        "from sentry.conf.server import *\n"
        "SENTRY_CACHE = 'sentry.cache.redis.RedisCache'\n"
        "SENTRY_CACHE_OPTIONS = {'cluster': 'default'}\n")
    os.environ["SENTRY_CONF"] = str(settings_directory)
    from examples.sentry_dbworker.adapter import initialize
    initialize()
    def payload(index):
        return {"to": [f"recipient{index}@benchmark.invalid"],
                "from": "sender@benchmark.invalid", "subject": f"Historical Sentry fixture {index}",
                "text": f"Plain body {index} with unicode: café", "html": f"<p>HTML body {index}: café</p>",
                "headers": {"Message-Id": f"<sentry-{index}@benchmark.invalid>", "X-Benchmark": str(index)}}
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
                requests = [Request(suite="sentry", payload=payload(key)) for key in ids]
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
            results = wait_for(complete, timeout=max(120, len(ids) * 15))
            return time.perf_counter() - started, results

        execute_batch([-2, -1])
        sink.messages.clear()
        ids = list(range(profile["requests"]))
        seconds, results = execute_batch(ids)
        wait_for(lambda: len(sink.messages) == len(ids))
        from email import policy
        from email.parser import BytesParser
        normalized = []
        for sender, recipients, raw in sink.messages:
            message = BytesParser(policy=policy.default).parsebytes(raw)
            index = int(message["X-Benchmark"])
            expected = payload(index)
            parts = {part.get_content_type(): part.get_content().rstrip("\r\n")
                     for part in message.walk() if part.get_content_maintype() == "text"}
            actual = {"from": sender, "to": recipients, "subject": str(message["Subject"]),
                      "text": parts.get("text/plain"), "html": parts.get("text/html"),
                      "headers": {"Message-Id": str(message["Message-Id"]), "X-Benchmark": str(message["X-Benchmark"])}}
            if actual != expected:
                raise AssertionError(f"SMTP content differs from fixture: {actual!r}")
            if str(message["From"]) != expected["from"] or str(message["To"]) != expected["to"][0]:
                raise AssertionError("Visible sender/recipient headers differ")
            normalized.append(actual)
        normalized.sort(key=lambda row: int(row["headers"]["X-Benchmark"]))
        if normalized != [payload(i) for i in ids] or any(result != {"accepted": 1} for result in results):
            raise AssertionError("SMTP delivery contains missing or duplicate messages")
        write_json(directory / "smtp-delivery.json", normalized)
        normalized = json.dumps(normalized, sort_keys=True).encode()
        if args.backend == "dbworker":
            with sessions() as session:
                table = runtime.workers["integration"].table
                states = session.execute(select(table.c.execution_status)).scalars().all()
                if len(states) != profile["requests"] + 2 or any(str(state) != "finished" for state in states):
                    raise AssertionError("DBWorker completion ledger is incomplete")
        row = {"scenario": "historical_smtp_email", "comparison_mode": "paired_subprocess_bridge",
               "backend": args.backend, "repetition": args.repetition, "status": "passed",
               "metrics": {"wall_seconds": seconds, "messages_per_second": profile["requests"] / seconds,
                           "cpu_seconds": None, "peak_rss_bytes": None},
               "unavailable_metrics": {"cpu_seconds": "Total-stack sampling is not implemented for this initial suite",
                                       "peak_rss_bytes": "Total-stack sampling is not implemented for this initial suite"},
               "validation": {"passed": True, "output_digest": hashlib.sha256(normalized).hexdigest(),
                              "messages": len(results), "smtp_envelope_and_mime": True, "missing_or_duplicate": 0},
               "environment": {"python": platform.python_version(), "interpreter": sys.executable}}
        packages = {}
        for name in ("celery", "sqlalchemy", "dbworker", "aiosmtpd"):
            try:
                packages[name] = importlib.metadata.version(name)
            except importlib.metadata.PackageNotFoundError:
                packages[name] = None
        row["environment"]["packages"] = packages
        upstream = subprocess.check_output(
            [config["interpreters"]["upstream_python"], "-c",
             "import json,platform,importlib.metadata as m; print(json.dumps({'python':platform.python_version(),"
             "'packages':{name:m.version(name) for name in ['Django','celery','sentry-sdk','xmlsec']}}))"],
            text=True)
        row["environment"]["upstream"] = json.loads(upstream)
        row["environment"]["upstream"]["interpreter"] = config["interpreters"]["upstream_python"]
        row["environment"]["upstream"]["version_deviations"] = {
            "python": "3.10.20 matches the frozen lock generation; setup.py's suggested 3.8 is incompatible with that lock",
            "xmlsec": "1.3.17 replaces 1.3.13 for an ARM-compatible wheel; XML signing is outside the SMTP workload"}
        row["configuration"] = {
            "concurrency": 2, "request_database": "sqlite", "comparison_mode": "paired_subprocess_bridge",
            "redis_aof": "everysec", "smtp": "isolated local SMTP receiver", "warmup_requests": 2,
            "timing": "Durable publication through receipt and saved results; each request starts the same complete historical Sentry process",
            "celery_retry": "none", "dbworker_retry": "none", "profile": profile}
        row["dataset"] = {"messages": profile["requests"], "recipients_per_message": 1,
                          "content": "Deterministic subject, Unicode plain/HTML alternatives and custom message headers"}
        row["capabilities"] = {
            "verified": ["real_sentry_24_1_send_messages", "real_smtp_delivery", "mime_content_oracle",
                         "sender_recipient_headers", "no_missing_or_duplicate_delivery", "dbworker_ledger"],
            "untested": ["current_sentry_taskbroker", "notification_templates", "notification_models",
                         "crash_recovery", "outbox_recovery", "native_celery_task_lifecycle"],
            "scope": "Historical synchronous SMTP utility with full Sentry app initialization and identical per-request bridge overhead"}
        write_json(directory / "sample.json", row)
    finally:
        runtime.stop()
        smtp.stop()
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
