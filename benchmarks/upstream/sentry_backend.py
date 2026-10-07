"""Compare native historical Sentry email fan-out with individual DBWorker jobs."""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.metadata
import json
import os
import platform
import re
import socket
import subprocess
import sys
import threading
import time
from contextlib import nullcontext
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "examples/sentry/src"
sys.path[:0] = [str(SOURCE), str(ROOT)]
TASKS = ["sentry.tasks.email.send_email", "sentry.tasks.email.send_email_control"]


def xml_library_linkage():
    """Admit the original SAML imports only with matching system libxml2 builds."""
    from lxml import etree
    import xmlsec
    versions = {
        "lxml_compiled": tuple(etree.LIBXML_COMPILED_VERSION),
        "lxml_runtime": tuple(etree.LIBXML_VERSION),
        "xmlsec_compiled": tuple(xmlsec.get_libxml_compiled_version()),
        "xmlsec_runtime": tuple(xmlsec.get_libxml_version()),
    }
    if len(set(versions.values())) != 1:
        raise RuntimeError(f"Sentry XML extensions must share system libxml2: {versions}")
    return {"libxml2_versions": versions,
            "libxmlsec_version": tuple(xmlsec.get_libxmlsec_version()),
            "build_policy": "Pinned lxml and xmlsec rebuilt from source against the same system libraries"}


def application_dependencies():
    """Require both application arms to retain the same checked-in graph."""
    packages = {}
    for line in (ROOT / "benchmarks/locks/sentry-native.txt").read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        name, expected = line.split("==", 1)
        actual = importlib.metadata.version(name)
        if actual != expected:
            raise RuntimeError(f"Native historical dependency lock mismatch: {name}: {actual} != {expected}")
        packages[name] = actual
    return packages


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def recipients(operation_id):
    stem = str(operation_id).replace(":", "-")
    return [f"recipient-{stem}-a@benchmark.invalid", f"recipient-{stem}-b@benchmark.invalid"]


def wait_for(predicate, *, children=(), timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if any(child.poll() is not None for child in children):
            raise RuntimeError("An owned native backend service exited")
        result = predicate()
        if result:
            return result
        time.sleep(.05)
    raise TimeoutError("Native email delivery made no complete workflow progress")


def initialize_native():
    from sentry.runner import configure
    configure(skip_service_validation=True)
    from sentry.celery import app
    import sentry.tasks.email
    from benchmarks.common.native_admission import check_original_tasks
    evidence = check_original_tasks(app, TASKS, SOURCE,
        expected_application="sentry.celery:app", configuration={
            "task_protocol": app.conf.task_protocol, "task_serializer": app.conf.task_serializer,
            "accept_content": sorted(app.conf.accept_content), "task_acks_late": app.conf.task_acks_late,
            "worker_prefetch_multiplier": app.conf.worker_prefetch_multiplier,
            "result_backend": app.conf.result_backend,
            "delivery_queues": [{"name": queue.name, "exchange": queue.exchange.name,
                                  "routing_key": queue.routing_key, "durable": queue.durable,
                                  "auto_delete": queue.auto_delete}
                                 for queue in app.conf.task_queues if queue.name in ("email", "email.control")],
            "delivery_task_policies": {name: {"max_retries": app.tasks[name].max_retries,
                "default_retry_delay": app.tasks[name].default_retry_delay,
                "trail": app.tasks[name].trail} for name in TASKS}})
    if app.conf.task_protocol != 1 or app.conf.task_serializer != "pickle":
        raise RuntimeError("Original historical Sentry protocol/serializer was changed")
    return app, evidence


class ResourceSampler:
    """Sample the producer, receiver, coordinator, Redis and persistent children."""
    def __init__(self):
        import psutil
        self.psutil = psutil
        self.initial, self.latest = {}, {}
        self.peak = 0
        self.stop_event = threading.Event()
        self.sample(baseline=True)
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def sample(self, baseline=False):
        parent = self.psutil.Process()
        rss = 0
        for process in [parent, *parent.children(recursive=True)]:
            try:
                key = (process.pid, process.create_time())
                cpu = process.cpu_times()
                value = cpu.user + cpu.system
                self.initial.setdefault(key, value if baseline else 0)
                self.latest[key] = value
                rss += process.memory_info().rss
            except (self.psutil.NoSuchProcess, self.psutil.AccessDenied):
                pass
        self.peak = max(self.peak, rss)

    def run(self):
        while not self.stop_event.wait(.05):
            self.sample()

    def finish(self):
        self.stop_event.set()
        self.thread.join()
        self.sample()
        return {"cpu_seconds": sum(max(0, value - self.initial[key]) for key, value in self.latest.items()),
                "peak_summed_rss_bytes": self.peak}


def preflight(directory, backend):
    """Retain a structured blocker instead of restoring the subprocess bridge."""
    try:
        if backend == "dbworker" and sys.version_info < (3, 12):
            raise RuntimeError("DBWorker requires Python >=3.12; historical Python 3.10 cannot substitute")
        import django
        import sentry.runner
        if backend == "dbworker":
            import dbworker
        return {"python": platform.python_version(), "django": django.get_version(), "interpreter": sys.executable}
    except Exception as exc:
        write_json(directory / "admission.json", {
            "status": "blocked", "phase": "runtime_compatibility", "backend": backend,
            "native_baseline": False, "workflow_parity": False,
            "error": {"type": type(exc).__name__, "message": str(exc)},
            "required": "Pinned Sentry must initialize in the supported DBWorker Python >=3.12 environment without per-job bridges",
            "python": platform.python_version(), "interpreter": sys.executable})
        raise RuntimeError(f"Sentry native runtime admission blocked: {exc}") from exc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--backend", required=True, choices=("celery", "dbworker"))
    parser.add_argument("--repetition", required=True, type=int)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    profile = config["suite"]["profiles"][config["profile"]]
    directory = (Path(config["output_directory"]) / f"sentry-{args.repetition}-{args.backend}").resolve()
    directory.mkdir()
    environment = preflight(directory, args.backend)
    try:
        environment["application_dependencies"] = application_dependencies()
        environment["xml_library_linkage"] = xml_library_linkage()
    except Exception as exc:
        write_json(directory / "admission.json", {"status": "blocked", "phase": "dependency_lock",
            "backend": args.backend, "native_baseline": False, "workflow_parity": False,
            "error": {"type": type(exc).__name__, "message": str(exc)}, "environment": environment})
        raise
    trace = directory / "workflow.jsonl"
    os.environ.update(BENCHMARK_TRACE_PATH=str(trace), BENCHMARK_BACKEND=args.backend,
                      BENCHMARK_TASK_STAGES=json.dumps(dict.fromkeys(TASKS, "delivery")),
                      PYTHONPATH=os.pathsep.join([str(SOURCE), str(ROOT), str(ROOT / "src")]),
                      SENTRY_SILO_MODE="MONOLITH", SENTRY_LOG_LEVEL="WARNING")
    from benchmarks.common import native_observer as observer
    from benchmarks.common.workflow_graph import read_trace, validate_graph
    from aiosmtpd.controller import Controller
    class Sink:
        def __init__(self):
            self.messages = []
        async def handle_DATA(self, server, session, envelope):
            self.messages.append((envelope.mail_from, envelope.rcpt_tos, envelope.content))
            return "250 Accepted"
    def port():
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            return reservation.getsockname()[1]
    children, streams = [], []
    runtime = engine = smtp = None
    sampler = None
    def launch(command, name):
        stream = (directory / name).open("wb")
        streams.append(stream)
        children.append(subprocess.Popen(command, env=os.environ.copy(), stdout=stream, stderr=subprocess.STDOUT))
    try:
        smtp_port, redis_port = port(), port()
        redis_url = f"redis://127.0.0.1:{redis_port}/0"
        launch(["redis-server", "--bind", "127.0.0.1", "--port", str(redis_port), "--dir", str(directory),
                "--save", "", "--appendonly", "yes", "--appendfsync", "everysec"], "redis.log")
        import redis
        client = redis.Redis.from_url(redis_url)
        def redis_ready():
            try:
                return client.ping()
            except redis.ConnectionError:
                return False
        wait_for(redis_ready, children=children)
        sink = Sink()
        smtp = Controller(sink, hostname="127.0.0.1", port=smtp_port)
        smtp.start()
        settings_directory = directory / "sentry-config"
        settings_directory.mkdir()
        templates = settings_directory / "templates"
        templates.mkdir()
        (templates / "benchmark.txt").write_text("Plain body {{ operation_id }} with unicode: café")
        (templates / "benchmark.html").write_text('<html><head><style>.fixture {color: red;}</style></head><body><p class="fixture">HTML body {{ operation_id }}: café</p></body></html>')
        (settings_directory / "config.yml").write_text(
            "system.secret-key: isolated-historical-sentry-benchmark\nmail.backend: smtp\nmail.host: 127.0.0.1\n"
            f"mail.port: {smtp_port}\nmail.from: sender@benchmark.invalid\nmail.use-tls: false\nmail.use-ssl: false\n")
        (settings_directory / "sentry.conf.py").write_text(
            "from sentry.conf.server import *\n"
            f"BROKER_URL = {redis_url!r}\n"
            "SENTRY_CACHE = 'sentry.cache.redis.RedisCache'\nSENTRY_CACHE_OPTIONS = {'cluster': 'default'}\n"
            f"SENTRY_OPTIONS['redis.clusters'] = {{'default': {{'hosts': {{0: {{'host': '127.0.0.1', 'port': {redis_port}, 'db': 1}}}}}}}}\n"
            f"TEMPLATES[0]['DIRS'] = [{str(templates)!r}] + TEMPLATES[0]['DIRS']\n")
        os.environ["SENTRY_CONF"] = str(settings_directory)
        try:
            app, native = initialize_native()
        except Exception as exc:
            write_json(directory / "admission.json", {"status": "blocked", "phase": "native_application_bootstrap",
                "backend": args.backend, "native_baseline": False, "workflow_parity": False,
                "error": {"type": type(exc).__name__, "message": str(exc)}, "environment": environment})
            raise
        from sentry.utils.email import MessageBuilder
        if args.backend == "celery":
            # Only bootstrap differs from plain CLI: configure the app once first.
            launch([sys.executable, "-c", "from sentry.runner import configure; configure(skip_service_validation=True); from celery.__main__ import main; main()",
                    "-A", "sentry.celery:app", "worker", "-Q", "email,email.control",
                    "--pool=prefork", "--concurrency=2", "--include", "benchmarks.common.native_observer",
                    "--loglevel=WARNING"], "worker.log")
            wait_for(lambda: app.control.ping(timeout=1), children=children)
            publication = nullcontext(([], []))
        else:
            from examples.sentry_dbworker.adapter import coordinator, initialize, publication_to_dbworker
            initialize()
            runtime, engine, sessions = coordinator(f"sqlite:///{directory / 'deliveries.db'}", concurrency=2)
            runtime.start()
            publication = publication_to_dbworker(sessions)

        with publication as (published, publication_errors):
            def execute_batch(operation_ids):
                started = time.perf_counter()
                for operation_id in operation_ids:
                    with observer.operation(operation_id):
                        destination = recipients(operation_id)
                        MessageBuilder(subject=f"Historical Sentry fixture {operation_id}\nDiscarded subject line",
                            context={"operation_id": str(operation_id)}, template="benchmark.txt", html_template="benchmark.html",
                            headers={"X-Benchmark": str(operation_id)}, from_email="sender@benchmark.invalid").send_async(
                                to=[*destination, destination[0], ""])
                if publication_errors:
                    raise RuntimeError("Native safe_execute suppressed a DBWorker publication error") from publication_errors[0]
                def complete():
                    events = read_trace(trace) if trace.exists() else []
                    if any(row.get("event") in ("failed", "retried", "revoked", "unknown") for row in events):
                        raise RuntimeError("Success-only native Sentry workflow observed a non-successful attempt")
                    try:
                        graph = validate_graph(events, operation_ids, {"delivery": 2}, [])
                    except (ValueError, FileNotFoundError):
                        return None
                    wanted = set(map(str, operation_ids))
                    from email.parser import BytesParser
                    from email import policy
                    matched = sum(str(BytesParser(policy=policy.default).parsebytes(raw)["X-Benchmark"]) in wanted
                                  for _, _, raw in sink.messages)
                    return graph if matched == len(operation_ids) * 2 else None
                graph = wait_for(complete, children=children, timeout=max(120, len(operation_ids) * 5))
                return time.perf_counter() - started, graph

            execute_batch(["warmup:-2", "warmup:-1"])
            sink.messages.clear()
            operations = list(map(str, range(profile["requests"])))
            sampler = ResourceSampler()
            seconds, graph = execute_batch(operations)
            metrics = sampler.finish()
            sampler = None
        # Retain received bytes before validation, including failed-run evidence.
        # Native Message-ID generation has a finite random range and can collide.
        receipts = directory / "smtp-receipts.log"
        write_json(receipts, [{"sender": sender, "destinations": destinations,
            "raw_base64": base64.b64encode(raw).decode("ascii")}
            for sender, destinations, raw in sink.messages])
        normalized = validate_messages(sink.messages, operations)
        if args.backend == "dbworker":
            from sqlalchemy import select
            table = runtime.workers["sentry_delivery"].table
            with sessions() as session:
                states = list(session.scalars(select(table.c.execution_status)))
                if len(states) != (len(operations) + 2) * 2 or any(str(state) != "finished" for state in states):
                    raise AssertionError("DBWorker individual delivery ledger is incomplete")
        write_json(directory / "smtp-delivery.json", normalized)
        environment["packages"] = {name: importlib.metadata.version(name)
                                   for name in ("Django", "celery", "sentry-sdk", "psutil", "aiosmtpd")}
        environment["dependency_lock"] = "benchmarks/locks/sentry-native.txt"
        environment["runtime_deviation"] = "Historical source targets its Python 3.10 lock generation; both application arms now require proved Python 3.12 compatibility"
        environment["compatibility_deviations"] = {"hiredis": {
            "historical": "0.3.1", "selected": "2.3.2",
            "reason": "Original setup imports imp, removed in Python 3.12; both arms use the same Redis parser pin"},
            "xmlsec": {"historical": "1.3.13", "selected": "1.3.14",
                       "reason": "Minimum release removes obsolete SOAP constants for modern libxmlsec and accepts original lxml4.9.3 build headers; both extensions use identical system libxml2"},
            "typing-extensions": {"historical": "4.5.0", "selected": "4.6.0",
                       "reason": "SQLAlchemy's declared dependency requires >=4.6.0; both application environments use the same minimum compatible pin"},
            "grpcio": {"historical": "1.56.0", "selected": "1.59.3",
                       "reason": "Original C++ build failed with modern Clang; 1.59.3 provides verified Python 3.12 wheels and satisfies original grpcio-status>=1.56 constraint"}}
        row = {"scenario": "historical_native_email_fanout", "comparison_mode": "native_execution",
            "backend": args.backend, "repetition": args.repetition, "status": "passed",
            "metrics": {**metrics, "wall_seconds": seconds, "messages_per_second": len(normalized) / seconds},
            "validation": {"passed": True, "messages": len(normalized), "smtp_envelope_and_mime": True,
                "missing_or_duplicate": 0, "delivery_identity": "Exact unique operation ID and recipient pair",
                "generated_message_ids": message_id_evidence(sink.messages),
                "output_digest": hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()},
            "smtp_receipts": {"storage": "Matrix artifact diagnostics; excluded from published result evidence",
                              "path": str(receipts.relative_to(Path(config["output_directory"]).resolve())),
                              "sha256": hashlib.sha256(receipts.read_bytes()).hexdigest()},
            "native_execution": native, "workflow_graph": graph,
            "workflow_trace": {"path": str(trace.relative_to(Path(config["output_directory"]).resolve())),
                               "sha256": hashlib.sha256(trace.read_bytes()).hexdigest()},
            "environment": environment,
            "configuration": {"concurrency": 2, "native_protocol": 1, "native_serializer": "pickle",
                "silo": "MONOLITH", "warmup_operations": 2, "profile": profile,
                "application_log_level": "WARNING",
                "environment_overrides": {"SENTRY_LOG_LEVEL": "Match producer/native worker/DBWorker child logging through upstream's supported setting",
                                          "SENTRY_SILO_MODE": "Declare the native monolith email fixture; preserve native region/control guards"},
                "native_dependency_lock_sha256": hashlib.sha256((ROOT / "benchmarks/locks/sentry-native.txt").read_bytes()).hexdigest(),
                "timing": "Native MessageBuilder rendering/publication through SMTP receipt and terminal job success",
                "rss_note": "Sum across producer/receiver/coordinator/Redis/worker processes; shared pages can be counted twice",
                "success_only": True},
            "dataset": {"operations": len(operations), "recipients_per_operation": 2,
                "delivery_jobs": len(operations) * 2, "duplicate_and_empty_addresses_deduplicated_by_native_producer": True},
            "capabilities": {"verified": ["native_MessageBuilder_send_async", "native_email_tasks", "recipient_fanout",
                "template_rendering", "css_inlining", "smtp_delivery", "individual_delivery_graph", "dbworker_ledger"],
                "untested": ["current_sentry_taskbroker", "group_thread_models", "preceding_notification_tasks", "crash_recovery", "outage_recovery"],
                "scope": "Historical 24.1 native two-recipient MessageBuilder delivery, successful workload only"}}
        write_json(directory / "sample.json", row)
    finally:
        if sampler:
            sampler.finish()
        if runtime:
            runtime.stop()
        if smtp:
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
        if engine:
            engine.dispose()


def message_id_evidence(messages):
    """Report native random header collisions separately from delivery duplicates."""
    from collections import Counter
    from email import policy
    from email.parser import BytesParser
    identifiers = Counter(str(BytesParser(policy=policy.default).parsebytes(raw)["Message-Id"])
                          for _, _, raw in messages)
    return {"distinct": len(identifiers), "collisions": sum(count - 1 for count in identifiers.values()),
            "duplicate_values": sorted(key for key, count in identifiers.items() if count > 1),
            "generation": "Original MessageBuilder UTC second, producer PID, and randrange(100000)",
            "normalization": "Generated Message-ID, Date, and MIME boundaries are excluded from the business digest"}


def validate_messages(messages, operations):
    from email import policy
    from email.parser import BytesParser
    from lxml import html
    normalized, observed = [], set()
    for sender, destinations, raw in messages:
        message = BytesParser(policy=policy.default).parsebytes(raw)
        operation_id = str(message["X-Benchmark"])
        to = str(message["To"])
        identity = (operation_id, to)
        if operation_id not in operations or to not in recipients(operation_id) or identity in observed:
            raise AssertionError("SMTP output has unknown or duplicate delivery identities")
        if sender != "sender@benchmark.invalid" or destinations != [to] or str(message["From"]) != sender:
            raise AssertionError("SMTP envelope and visible address headers differ")
        if str(message["Subject"]) != f"Historical Sentry fixture {operation_id}":
            raise AssertionError("Native subject normalization differs")
        other = next(recipient for recipient in recipients(operation_id) if recipient != to)
        if str(message["Reply-To"]) != other:
            raise AssertionError("Native multi-recipient reply header differs")
        parts = {part.get_content_type(): part.get_content().rstrip("\r\n")
                 for part in message.walk() if part.get_content_maintype() == "text"}
        if parts.get("text/plain") != f"Plain body {operation_id} with unicode: café":
            raise AssertionError("Native text template output differs")
        document = html.fromstring(parts.get("text/html", ""))
        paragraphs = document.xpath('//p[@class="fixture"]')
        if len(paragraphs) != 1 or paragraphs[0].text_content() != f"HTML body {operation_id}: café":
            raise AssertionError("Native HTML template output differs")
        style = paragraphs[0].get("style", "").replace(" ", "").lower()
        if "color:red" not in style:
            raise AssertionError("Native CSS was not inlined")
        msgid = str(message["Message-Id"])
        if len(message.get_all("Message-Id", [])) != 1 or not re.fullmatch(r"<\d{14}\.\d+\.\d{1,5}@benchmark\.invalid>", msgid):
            raise AssertionError("Native generated Message-ID header is absent or malformed")
        observed.add(identity)
        normalized.append({"operation_id": operation_id, "from": sender, "to": [to],
            "subject": str(message["Subject"]), "reply_to": other, "text": parts["text/plain"], "html": parts["text/html"]})
    expected = {(operation_id, recipient) for operation_id in operations for recipient in recipients(operation_id)}
    if observed != expected:
        raise AssertionError("SMTP delivery contains missing distinct recipient messages")
    return sorted(normalized, key=lambda row: (row["operation_id"], row["to"]))


if __name__ == "__main__":
    main()
