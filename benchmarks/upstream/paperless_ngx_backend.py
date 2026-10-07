"""Compare native Paperless folder-consumption tasks with durable DBWorker jobs."""
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
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "examples/paperless_ngx"
sys.path[:0] = [str(ROOT), str(ROOT / "src"), str(SOURCE / "src")]
from benchmarks.common.reporting import write_json
from benchmarks.common.native_observer import operation
from benchmarks.common.workflow_graph import read_trace, validate_graph


def wait_for(predicate, timeout=300):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.05)
    raise TimeoutError("Paperless native workflow did not reach complete business success")


def assert_native_lifecycle_log(worker_log):
    """Fail closed when upstream explicitly reports a swallowed tracking error."""
    if not worker_log.exists():
        return
    content = worker_log.read_text(errors="replace")
    for message in ("Creating PaperlessTask failed", "Setting PaperlessTask started failed", "Updating PaperlessTask failed"):
        if message in content:
            raise RuntimeError(f"Original Paperless lifecycle failed: {message}; inspect {worker_log}")


def inspect_outputs(documents, fixtures):
    """Independent post-timing oracle; never a replacement task body."""
    from documents.parsers import get_default_thumbnail
    from PIL import Image
    results = []
    for document, fixture in zip(documents, fixtures, strict=True):
        if fixture["marker"] not in document.content or "orchard" not in document.content.lower():
            raise AssertionError("Actual OCR missed independently specified scan text")
        original = hashlib.sha256(document.source_path.read_bytes()).hexdigest()
        if original != fixture["sha256"]:
            raise AssertionError("Durable original differs from submitted scan")
        thumbnail = document.thumbnail_path.read_bytes()
        if thumbnail == get_default_thumbnail().read_bytes():
            raise AssertionError("Thumbnail is a generic fallback")
        with Image.open(document.thumbnail_path) as image:
            thumbnail_size = list(image.size)
            thumbnail_pixels = hashlib.sha256(image.convert("RGB").tobytes()).hexdigest()
        if not document.archive_path or not document.archive_path.is_file():
            raise AssertionError("The expected raster-scan PDF archive is missing")
        archive_text = subprocess.check_output(["pdftotext", str(document.archive_path), "-"], text=True)
        if fixture["marker"] not in archive_text or "orchard" not in archive_text.lower():
            raise AssertionError("Archive has no expected OCR text layer")
        import pikepdf
        with pikepdf.open(document.archive_path) as archive:
            pages = len(archive.pages)
        # The original Tesseract parser only reports source page_count for PDFs;
        # PNG source documents retain NULL although their archive has one page.
        if document.page_count is not None or pages != 1 or document.owner_id is not None:
            raise AssertionError("Unexpected page count or folder-consumption owner")
        if any(value is not None for value in (document.correspondent_id, document.document_type_id, document.storage_path_id)):
            raise AssertionError("Unconfigured matching rules unexpectedly changed document metadata")
        results.append({"content": document.content, "original_sha256": original,
                        "title": document.title, "checksum": document.checksum,
                        "mime_type": document.mime_type, "page_count": document.page_count,
                        "owner_id": document.owner_id, "tags": sorted(document.tags.values_list("name", flat=True)),
                        "created": document.created.isoformat(),
                        "correspondent_id": document.correspondent_id, "document_type_id": document.document_type_id,
                        "storage_path_id": document.storage_path_id, "thumbnail_pixel_sha256": thumbnail_pixels,
                        "thumbnail_size": thumbnail_size, "archive_pages": pages,
                        "archive_text": archive_text.strip(), "search_ready": True})
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--backend", choices=("celery", "dbworker"), required=True)
    parser.add_argument("--repetition", type=int, required=True)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    profile = config["suite"]["profiles"][config["profile"]]
    output = Path(config["output_directory"]).resolve()
    directory = output / f"paperless_ngx-{args.repetition}-{args.backend}"
    directory.mkdir()
    for name in ("data/log", "data/index", "media", "consume", "scratch"):
        (directory / name).mkdir(parents=True, exist_ok=True)
    for binary in ("redis-server", "tesseract", "gs", "convert", "pdftotext"):
        if not shutil.which(binary):
            raise FileNotFoundError(f"Missing real ingestion dependency: {binary}")
    if args.backend == "celery" and sys.platform == "darwin":
        raise RuntimeError("Native Paperless prefork/child-recycling admission requires Linux; macOS threads are not a native fallback")
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    redis_url = f"redis://127.0.0.1:{port}/0"
    url = f"sqlite:///{directory / 'jobs.db'}"
    trace = directory / "workflow.jsonl"
    os.environ.update({"DJANGO_SETTINGS_MODULE": "paperless.settings",
        "PAPERLESS_SECRET_KEY": "isolated-native-benchmark-key-not-production",
        "PAPERLESS_DATA_DIR": str(directory / "data"), "PAPERLESS_MEDIA_ROOT": str(directory / "media"),
        "PAPERLESS_CONSUMPTION_DIR": str(directory / "consume"), "PAPERLESS_SCRATCH_DIR": str(directory / "scratch"),
        "PAPERLESS_OCR_LANGUAGE": "eng", "PAPERLESS_OCR_OUTPUT_TYPE": "pdf", "PAPERLESS_OCR_CLEAN": "none",
        "PAPERLESS_OCR_DESKEW": "false", "PAPERLESS_OCR_ROTATE_PAGES": "false", "PAPERLESS_OCR_MODE": "force",
        "PAPERLESS_REDIS": redis_url, "PAPERLESS_TASK_WORKERS": "2", "PAPERLESS_THREADS_PER_WORKER": "1",
        "DBWORKER_DATABASE_URL": url, "BENCHMARK_TRACE_PATH": str(trace), "BENCHMARK_BACKEND": args.backend,
        "BENCHMARK_TASK_STAGES": json.dumps({"documents.tasks.consume_file": "ingestion"}),
        "PYTHONPATH": os.pathsep.join((str(ROOT), str(ROOT / "src"), str(SOURCE / "src")))})
    children, streams = [], []
    runtime = engine = None
    def launch(command, name):
        stream = (directory / name).open("wb")
        streams.append(stream)
        child = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT)
        children.append(child)
        return child
    try:
        launch(["redis-server", "--bind", "127.0.0.1", "--port", str(port), "--dir", str(directory),
                "--save", "", "--appendonly", "yes", "--appendfsync", "everysec"], "redis.log")
        import redis
        client = redis.Redis.from_url(redis_url)
        def redis_ready():
            try:
                return client.ping()
            except redis.ConnectionError:
                return False
        wait_for(redis_ready, 30)
        from examples.paperless_ngx_dbworker.adapter import initialize
        initialize()
        from django.core.management import call_command
        with (directory / "migrations.log").open("w") as log:
            call_command("migrate", interactive=False, verbosity=0, skip_checks=True, stdout=log)
        from documents.search import get_backend
        get_backend()
        from documents.models import Document, PaperlessTask, Workflow, Tag
        if Workflow.objects.exists() or Tag.objects.exists():
            raise RuntimeError("Native restricted fixture unexpectedly contains workflows or matching tags")
        from paperless.config import AIConfig
        if AIConfig().llm_index_enabled:
            raise RuntimeError("AI indexing is outside this declared graph")
        from documents.management.commands.document_consumer import _consume_file
        from paperless.celery import app
        from benchmarks.common.native_admission import check_original_tasks
        native_settings = {"task_serializer": app.conf.task_serializer,
                           "result_serializer": app.conf.result_serializer,
                           "result_backend": app.conf.result_backend,
                           "worker_max_tasks_per_child": app.conf.worker_max_tasks_per_child,
                           "task_track_started": app.conf.task_track_started,
                           "task_time_limit": app.conf.task_time_limit,
                           "worker_concurrency": app.conf.worker_concurrency,
                           "task_acks_late": app.conf.task_acks_late,
                           "worker_prefetch_multiplier": app.conf.worker_prefetch_multiplier}
        if native_settings["task_serializer"] != "signed-pickle" or native_settings["worker_max_tasks_per_child"] != 1:
            raise RuntimeError("Native serializer or worker child-recycling settings changed")
        native_execution = check_original_tasks(app, ["documents.tasks.consume_file"], SOURCE,
            expected_application="paperless.celery:app", configuration=native_settings)
        from PIL import Image, ImageDraw, ImageFont
        import reportlab
        font = ImageFont.truetype(str(Path(reportlab.__file__).parent / "fonts/Vera.ttf"), 52)
        fixtures = []
        for i in range(profile["requests"] + 2):
            image = Image.new("RGB", (1800, 700), "white")
            draw = ImageDraw.Draw(image)
            marker = f"INVOICE {i:05d}"
            draw.text((90, 110), marker, fill="black", font=font)
            draw.text((90, 220), "Benchmark orchard payment received", fill="black", font=font)
            path = directory / "consume" / f"invoice-{i:05d}.png"
            image.save(path, dpi=(200, 200))
            # Native folder consumption derives creation date from file mtime
            # when OCR has no date. Supply the identical timestamp to both arms.
            os.utime(path, (1609459200, 1609459200))
            fixtures.append({"path": path, "marker": marker, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                             "operation": f"warmup:{i}" if i < 2 else f"invoice:{i:05d}"})
        from sqlalchemy import create_engine, select
        from sqlalchemy.orm import sessionmaker
        from examples.paperless_ngx_dbworker.runtime import coordinator, route_tasks, Job
        engine = create_engine(url)
        sessions = sessionmaker(engine)
        if args.backend == "celery":
            launch([sys.executable, "-m", "celery", "-A", "paperless.celery:app", "worker",
                    "--include", "benchmarks.common.native_observer", "--loglevel", "WARNING",
                    "--hostname", f"paperless-native-{port}@localhost"], "worker.log")
            def worker_ready():
                if children[-1].poll() is not None:
                    raise RuntimeError("The native Paperless worker exited during startup")
                return app.control.ping(timeout=1)
            wait_for(worker_ready, 120)
        else:
            runtime = coordinator(url, concurrency=2)
            runtime.start()

        def execute_batch(batch):
            started = time.perf_counter()
            for fixture in batch:
                with operation(fixture["operation"]):
                    if args.backend == "celery":
                        queued = _consume_file(fixture["path"], directory / "consume", subdirs_as_tags=False)
                    else:
                        with route_tasks(fixture["operation"]):
                            queued = _consume_file(fixture["path"], directory / "consume", subdirs_as_tags=False)
                if not queued:
                    raise RuntimeError("The original folder producer failed to submit a scan")
            ops = [fixture["operation"] for fixture in batch]
            native_terminal_since = None
            def completed():
                nonlocal native_terminal_since
                if any(child.poll() is not None for child in children):
                    raise RuntimeError("An owned Paperless service exited")
                if args.backend == "celery":
                    assert_native_lifecycle_log(directory / "worker.log")
                events = read_trace(trace) if trace.exists() else []
                selected = [event for event in events if event["operation_id"] in ops]
                if any(event["stage"] != "ingestion" or event["event"] in {"failed", "retried", "revoked"} for event in selected):
                    raise RuntimeError("The declared unsplit single-attempt ingestion graph changed")
                submitted = [event for event in selected if event["event"] == "submitted"]
                if len(submitted) != len(batch) or sum(event["event"] == "succeeded" for event in selected) != len(batch):
                    return None
                graph = validate_graph(selected, ops, {"ingestion": 1}, [])
                tasks = {task.task_id: task for task in PaperlessTask.objects.filter(task_id__in=[e["node_id"] for e in submitted])}
                # Celery stores SUCCESS before native task_postrun tracking. Allow
                # that signal to finish, but never synthesize its missing state.
                if args.backend == "celery" and all(app.AsyncResult(e["node_id"]).ready() for e in submitted):
                    if native_terminal_since is None:
                        native_terminal_since = time.monotonic()
                    incomplete = [e["node_id"] for e in submitted if e["node_id"] not in tasks
                        or tasks[e["node_id"]].status != PaperlessTask.Status.SUCCESS
                        or not tasks[e["node_id"]].date_done]
                    if incomplete and time.monotonic() - native_terminal_since > 30:
                        raise RuntimeError("Celery completed but original Paperless lifecycle remained incomplete after 30s: "
                                           + ", ".join(incomplete))
                documents = []
                for op in ops:
                    identity = next(e["node_id"] for e in submitted if e["operation_id"] == op)
                    tracked = tasks.get(identity)
                    if not tracked or tracked.status != PaperlessTask.Status.SUCCESS or not tracked.date_done:
                        return None
                    if not tracked.date_started or tracked.duration_seconds is None or tracked.wait_time_seconds is None:
                        raise AssertionError("Native-equivalent task lifecycle timestamps are incomplete")
                    if tracked.trigger_source != PaperlessTask.TriggerSource.FOLDER_CONSUME:
                        raise AssertionError("Folder submission lost trigger metadata")
                    result = tracked.result_data
                    if args.backend == "celery":
                        async_result = app.AsyncResult(identity)
                        if not async_result.ready():
                            return None
                        if async_result.get(timeout=1) != result:
                            raise AssertionError("Native Redis result and business lifecycle record disagree")
                    else:
                        with sessions() as session:
                            job = session.get(Job, identity)
                            if job.result is None:
                                return None
                            if job.result["value"] != result:
                                raise AssertionError("DBWorker result and PaperlessTask disagree")
                            state = runtime.execution_status(session, worker="paperless", source_id=identity)
                            if str(state) != "finished":
                                return None
                    document = Document.objects.get(pk=result["document_id"])
                    get_backend().close()
                    if document.pk not in get_backend().search_ids("orchard", None):
                        return None
                    documents.append(document)
                return documents
            documents = wait_for(completed, max(300, len(batch) * 15))
            return time.perf_counter() - started, documents

        execute_batch(fixtures[:2])
        from benchmarks.upstream.paperless_ngx_metrics import StackMetrics
        with StackMetrics(os.getpid()) as resource_measurement:
            seconds, documents = execute_batch(fixtures[2:])
        results = inspect_outputs(documents, fixtures[2:])
        if Document.objects.count() != len(fixtures) or PaperlessTask.objects.count() != len(fixtures):
            raise AssertionError("Missing or duplicate documents or tracked ingestion jobs")
        graph = validate_graph(read_trace(trace), [f["operation"] for f in fixtures[2:]], {"ingestion": 1}, [])
        if args.backend == "dbworker":
            with sessions() as session:
                if len(session.scalars(select(Job)).all()) != len(fixtures):
                    raise AssertionError("Unexpected DBWorker continuation jobs")
        packages = {name: importlib.metadata.version(name) for name in ("django", "celery", "ocrmypdf", "tantivy", "sqlalchemy", "dbworker")}
        row = {"scenario": "native_unsplit_scan_ingestion", "comparison_mode": "native_application_workflow",
            "backend": args.backend, "repetition": args.repetition, "status": "passed",
            "metrics": {"wall_seconds": seconds, "documents_per_second": len(results) / seconds, **resource_measurement.values()},
            "validation": {"passed": True, "documents": len(results), "native_lifecycle": True,
                           "output_digest": hashlib.sha256(json.dumps(results, sort_keys=True).encode()).hexdigest()},
            "workflow_graph": graph,
            "workflow_trace": {"path": str(trace.relative_to(output)), "sha256": hashlib.sha256(trace.read_bytes()).hexdigest()},
            "native_execution": native_execution if args.backend == "celery" else None,
            "environment": {"python": platform.python_version(), "interpreter": sys.executable, "packages": packages},
            "configuration": {"concurrency": 2, "application_database": "sqlite", "dbworker_job_database": "sqlite",
                "native_application": "paperless.celery:app", "native_settings": native_settings, "profile": profile,
                "submission": "original folder _consume_file producer", "completion": "all native-equivalent tracked task states/results, search readiness and scheduler terminal events",
                "timing": "first original producer call through final business and scheduler completion; oracle hashing outside timer",
                "warmup_documents": 2, "redis_aof": "everysec", "auxiliary_progress_redis_both": True,
                "overrides": ["isolated paths/Redis/secret", "English forced OCR", "PDF output", "no clean/deskew/rotate", "2 worker slots, 1 OCR thread"],
                "celery_pool": "native prefork with max_tasks_per_child=1", "dbworker_pool": "persistent spawn processes",
                "lifecycle_limits": "DBWorker has no equivalent hard timeout, cancellation or child recycling; success contract only"},
            "dataset": {"documents": profile["requests"], "format": "raster-only PNG", "size": [1800, 700], "dpi": 200},
            "capabilities": {"verified": ["original_celery_app_and_task", "original_folder_producer", "tracked_lifecycle", "signed_results", "real_ocr", "originals_thumbnails_archives_search", "one_ingestion_job_per_scan"],
                "untested": ["barcode_split", "workflow_webhook", "AI", "mail_chords", "index_retry", "crash_recovery", "timeouts", "cancellation", "cross_orm_atomicity"],
                "scope": "Restricted successful unsplit folder scans; no configured workflows/AI. Any unexpected child graph fails admission."}}
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
