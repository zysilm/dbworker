"""Consume real scanned images and verify OCR, durable files, and Tantivy search."""
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
sys.path[:0] = [str(ROOT), str(ROOT / "src"), str(ROOT / "examples/paperless_ngx/src")]
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from benchmarks.common.reporting import write_json
from examples.dbworker_integration.runtime import Request, coordinator


def wait_for(predicate, timeout=240):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(.05)
    raise TimeoutError("Paperless ingestion did not complete before the deadline")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--backend", choices=("celery", "dbworker"), required=True)
    parser.add_argument("--repetition", type=int, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    profile = config["suite"]["profiles"][config["profile"]]
    directory = Path(config["output_directory"]) / f"paperless_ngx-{args.repetition}-{args.backend}"
    directory.mkdir()
    for name in ("data/log", "data/index", "media", "consume", "scratch"):
        (directory / name).mkdir(parents=True, exist_ok=True)
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    redis_url = f"redis://127.0.0.1:{port}/0"
    url = f"sqlite:///{directory / 'requests.db'}"
    os.environ.update({"DJANGO_SETTINGS_MODULE": "paperless.settings",
        "PAPERLESS_SECRET_KEY": "isolated-benchmark-key-not-for-production",
        "PAPERLESS_DATA_DIR": str(directory / "data"), "PAPERLESS_MEDIA_ROOT": str(directory / "media"),
        "PAPERLESS_CONSUMPTION_DIR": str(directory / "consume"), "PAPERLESS_SCRATCH_DIR": str(directory / "scratch"),
        "PAPERLESS_OCR_LANGUAGE": "eng", "PAPERLESS_OCR_OUTPUT_TYPE": "pdf", "PAPERLESS_OCR_CLEAN": "none",
        "PAPERLESS_OCR_DESKEW": "false", "PAPERLESS_OCR_ROTATE_PAGES": "false",
        "PAPERLESS_OCR_MODE": "force", "PAPERLESS_REDIS": redis_url,
        "BENCHMARK_REDIS_URL": redis_url, "DBWORKER_DATABASE_URL": url,
        "PYTHONPATH": os.pathsep.join((str(ROOT), str(ROOT / "src"), str(ROOT / "examples/paperless_ngx/src")))})
    # Redis supports upstream progress notifications in BOTH stacks; only Celery uses it as a broker.
    children, streams = [], []
    def launch(command, name):
        stream = (directory / name).open("wb")
        streams.append(stream)
        child = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT)
        children.append(child)
        return child
    runtime = None
    engine = None
    try:
        launch(["redis-server", "--bind", "127.0.0.1", "--port", str(port), "--dir", str(directory),
                "--save", "", "--appendonly", "yes", "--appendfsync", "everysec"], "redis.log")
        import redis
        client = redis.Redis.from_url(redis_url)
        def ready():
            try:
                return client.ping()
            except redis.ConnectionError:
                return False
        wait_for(ready, 30)
        from examples.paperless_ngx_dbworker.adapter import initialize
        initialize()
        from django.core.management import call_command
        with (directory / "migrations.log").open("w") as migration_log:
            call_command("migrate", interactive=False, verbosity=0, skip_checks=True, stdout=migration_log)
        # Initialize the real index before concurrent writers open it; a missing
        # directory otherwise makes upstream open an in-memory read index.
        from documents.search import get_backend
        get_backend()
        from documents.models import Document
        from PIL import Image, ImageDraw, ImageFont
        # Raster-only PNG inputs have no embedded text; content must be recovered by real Tesseract.
        import reportlab
        font = ImageFont.truetype(str(Path(reportlab.__file__).parent / "fonts/Vera.ttf"), 52)
        fixtures = []
        for i in range(profile["requests"] + 2):
            term = f"INVOICE {i:05d}"
            image = Image.new("RGB", (1800, 700), "white")
            draw = ImageDraw.Draw(image)
            draw.text((90, 110), term, fill="black", font=font)
            draw.text((90, 220), "Benchmark orchard payment received", fill="black", font=font)
            path = directory / "consume" / f"invoice-{i:05d}.png"
            image.save(path, dpi=(200, 200))
            fixtures.append({"source": 2, "original_file": str(path), "request_identity": f"benchmark-{i}",
                             "search_term": "orchard", "overrides": {"title": f"Invoice {i:05d}"},
                             "expected_sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "expected_marker": term})
        engine = create_engine(url)
        sessions = sessionmaker(engine)
        runtime = coordinator(url, concurrency=2)
        celery_pool = "threads" if sys.platform == "darwin" else "prefork"
        if args.backend == "celery":
            launch([sys.executable, "-m", "celery", "-A", "benchmarks.upstream.celery_app:app", "worker",
                    "--concurrency", "2", "--pool", celery_pool, "--loglevel", "WARNING", "--hostname", f"paperless-{port}@localhost",
                    "--without-gossip", "--without-mingle"], "worker.log")
            from benchmarks.upstream.celery_app import app, execute_request
            def worker_ready():
                if children[-1].poll() is not None:
                    raise RuntimeError("The Paperless Celery worker exited during startup")
                return app.control.ping(timeout=1)
            wait_for(worker_ready, 60)
        else:
            runtime.start()
        def execute_batch(payloads):
            started = time.perf_counter()
            with sessions.begin() as session:
                requests = [Request(suite="paperless_ngx", payload=payload) for payload in payloads]
                session.add_all(requests)
                session.flush()
                identities = [request.id for request in requests]
            if args.backend == "celery":
                for identity in identities:
                    execute_request.delay(identity)
            def completed():
                if any(child.poll() is not None for child in children):
                    raise RuntimeError("An owned Paperless service exited")
                with sessions() as session:
                    if args.backend == "dbworker":
                        states = session.execute(select(runtime.workers["integration"].table.c.execution_status)).scalars().all()
                        if any(str(state) == "failed" for state in states):
                            raise RuntimeError("A Paperless DBWorker handler failed; see the backend log")
                    rows = session.scalars(select(Request).where(Request.id.in_(identities)).order_by(Request.id)).all()
                    if len(rows) == len(payloads) and all(row.result is not None for row in rows):
                        return [row.result for row in rows]
            results = wait_for(completed)
            seconds = time.perf_counter() - started
            for fixture, result in zip(payloads, results, strict=True):
                if fixture["expected_marker"] not in result["content"] or "orchard" not in result["content"].lower():
                    raise AssertionError("Real OCR missed an independently specified fixture marker")
                if result["original_sha256"] != fixture["expected_sha256"]:
                    raise AssertionError("Durable original differs from the supplied scan")
            return seconds, results
        execute_batch(fixtures[:2])
        seconds, results = execute_batch(fixtures[2:])
        if Document.objects.count() != len(fixtures):
            raise AssertionError("Consumption produced missing or extra documents")
        if args.backend == "dbworker":
            with sessions() as session:
                states = session.execute(select(runtime.workers["integration"].table.c.execution_status)).scalars().all()
                if len(states) != len(fixtures) or any(str(state) != "finished" for state in states):
                    raise AssertionError("DBWorker ingestion completion ledger is incomplete")
        packages = {name: importlib.metadata.version(name) for name in ("django", "celery", "ocrmypdf", "tantivy", "sqlalchemy", "dbworker")}
        row = {"scenario": "scanned_image_ingestion", "comparison_mode": "paired_durable_request",
               "backend": args.backend, "repetition": args.repetition, "status": "passed",
               "metrics": {"wall_seconds": seconds, "documents_per_second": len(results) / seconds},
               "validation": {"passed": True, "documents": len(results), "raster_only_ocr": True,
                   "original_files": True, "thumbnails": True, "tantivy_search": True,
                   "output_digest": hashlib.sha256(json.dumps(results, sort_keys=True).encode()).hexdigest()},
               "environment": {"python": platform.python_version(), "interpreter": sys.executable, "packages": packages,
                   "tesseract": subprocess.check_output(["tesseract", "--version"], text=True).splitlines()[0]}}
        row["configuration"] = {"concurrency": 2, "application_database": "sqlite", "request_database": "sqlite",
            "comparison_mode": "paired_durable_request", "redis_aof": "everysec", "redis_auxiliary_progress_both_backends": True,
            "ocr_language": "eng", "ocr_mode": "force", "ocr_output_type": "pdf", "ocr_clean": "none",
            "ocr_deskew": False, "ocr_rotate_pages": False, "warmup_documents": 2,
            "retry": "none", "profile": profile, "celery_pool": celery_pool,
            "dbworker_pool": "spawn processes",
            "platform_pool_limitation": "macOS parser libraries crash in Celery fork children; local Celery uses threads" if sys.platform == "darwin" else None}
        row["dataset"] = {"documents": profile["requests"], "format": "raster-only PNG", "size": [1800, 700],
            "dpi": 200, "text": "INVOICE <five-digit-number>; Benchmark orchard payment received",
            "generation": "Pillow raster drawing using ReportLab Vera.ttf", "independent_oracle": "expected markers and original SHA256"}
        row["capabilities"] = {"verified": ["unchanged_consume_file_plugin_pipeline", "real_tesseract_ocr",
            "durable_document_rows", "original_file_sha256", "real_thumbnails", "tantivy_content_search", "dbworker_ledger"],
            "untested": ["crash_recovery", "postgresql", "classification_training", "barcode_split", "workflow_continuations", "AI"],
            "scope": "Paired durable requests running the unchanged upstream ingestion body; auxiliary Redis progress is shared infrastructure"}
        write_json(directory / "sample.json", row)
    finally:
        if runtime is not None:
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
