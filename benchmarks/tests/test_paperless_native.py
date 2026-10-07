"""Native source, signed objects, durable publication and lifecycle offline checks."""
import json
import os
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

from benchmarks.common.native_admission import check_worker_source

ROOT = Path(__file__).resolve().parents[2]


class PaperlessNativeTests(unittest.TestCase):
    def test_native_tracking_error_fails_closed_without_repairing_state(self):
        from benchmarks.upstream.paperless_ngx_backend import assert_native_lifecycle_log
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "worker.log"
            assert_native_lifecycle_log(log)
            log.write_text("Task documents.tasks.consume_file succeeded\n")
            assert_native_lifecycle_log(log)
            for message in ("Creating PaperlessTask failed", "Setting PaperlessTask started failed", "Updating PaperlessTask failed"):
                log.write_text("Task succeeded\n[ERROR] [paperless.handlers] " + message + "\n")
                with self.assertRaisesRegex(RuntimeError, "Original Paperless lifecycle failed"):
                    assert_native_lifecycle_log(log)

    def test_worker_uses_original_application_and_no_wrapper(self):
        source = (ROOT / "benchmarks/upstream/paperless_ngx_backend.py").read_text()
        self.assertTrue(check_worker_source(source, "paperless_ngx")["passed"])
        self.assertNotIn("execute_request", source)
        self.assertNotIn("consume_file.run", source)

    def test_real_producer_serializer_and_persistent_lifecycle_without_services(self):
        python = ROOT / "benchmarks/environments/paperless_ngx/dbworker/.venv/bin/python"
        if not python.exists():
            self.skipTest("Provision the pinned Paperless environment for offline native checks")
        with tempfile.TemporaryDirectory() as temporary:
            env = os.environ.copy()
            env.update(PYTHONPATH=os.pathsep.join((str(ROOT), str(ROOT / "src"))),
                       PAPERLESS_TEST_DIRECTORY=temporary)
            script = r'''
import json, os
from pathlib import Path
from types import SimpleNamespace
root = Path(os.environ['PAPERLESS_TEST_DIRECTORY'])
for name in ('data/log', 'data/index', 'media', 'consume', 'scratch'):
    (root / name).mkdir(parents=True, exist_ok=True)
os.environ.update(DJANGO_SETTINGS_MODULE='paperless.settings',
    PAPERLESS_SECRET_KEY='offline-native-paperless-test-key',
    PAPERLESS_DATA_DIR=str(root / 'data'), PAPERLESS_MEDIA_ROOT=str(root / 'media'),
    PAPERLESS_CONSUMPTION_DIR=str(root / 'consume'), PAPERLESS_SCRATCH_DIR=str(root / 'scratch'),
    PAPERLESS_OCR_LANGUAGE='eng', DBWORKER_DATABASE_URL='sqlite:///' + str(root / 'jobs.db'),
    BENCHMARK_TRACE_PATH=str(root / 'trace.jsonl'), BENCHMARK_BACKEND='dbworker')
from examples.paperless_ngx_dbworker.adapter import initialize
initialize()
from django.test.utils import override_settings
# A real process-local cache supports offline migration/control tests. The
# performance runner retains its real Redis cache and cannot use this fixture.
offline_cache = override_settings(
    CACHES={'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache'}},
    CHANNEL_LAYERS={'default': {'BACKEND': 'channels.layers.InMemoryChannelLayer'}},
    OCR_OUTPUT_TYPE='pdf', OCR_MODE='force', OCR_CLEAN='none', OCR_DESKEW=False,
    OCR_ROTATE_PAGES=False, THREADS_PER_WORKER=1)
offline_cache.enable()
from django.core.management import call_command
call_command('migrate', interactive=False, verbosity=0, skip_checks=True)
from paperless.celery import app
from paperless.signed_pickle import signed_pickle_dumps, signed_pickle_loads
from documents.data_models import ConsumableDocument, DocumentSource, DocumentMetadataOverrides
from documents.tasks import consume_file
from documents.management.commands.document_consumer import _consume_file
from documents.models import PaperlessTask
from documents.signals.handlers import task_prerun_handler, task_postrun_handler
from benchmarks.common.native_admission import check_original_tasks
from benchmarks.common.workflow_graph import read_trace, validate_graph, WorkflowMismatch
from examples.paperless_ngx_dbworker.runtime import coordinator, route_tasks, Job, decode_arguments
from sqlalchemy import select
from sqlalchemy.orm import Session
runtime = coordinator(os.environ['DBWORKER_DATABASE_URL'])
assert app.conf.task_serializer == 'signed-pickle'
assert app.conf.worker_max_tasks_per_child == 1
assert app.conf.task_track_started is True
assert check_original_tasks(app, ['documents.tasks.consume_file'],
    Path('examples/paperless_ngx').resolve(), expected_application='paperless.celery:app')['passed']
from PIL import Image
Image.new('RGB', (20, 20), 'white').save(root / 'scan.png')
obj = ConsumableDocument(source=DocumentSource.ConsumeFolder, original_file=root / 'scan.png')
restored = signed_pickle_loads(signed_pickle_dumps(obj))
assert restored.source == obj.source and restored.original_file == obj.original_file
for i in range(3):
    path = root / 'consume' / ('scan-%s.png' % i)
    Image.new('RGB', (20, 20), 'white').save(path)
    with route_tasks('scan:%s' % i):
        assert _consume_file(path, root / 'consume', subdirs_as_tags=False)
with Session(runtime.session_factory.kw['bind']) as session:
    jobs = session.scalars(select(Job).order_by(Job.operation_id)).all()
    assert len(jobs) == 3 and len({job.id for job in jobs}) == 3
    for i, job in enumerate(jobs):
        args, kwargs = decode_arguments(job.arguments)
        assert args == () and kwargs['input_doc'].original_file.name == 'scan-%s.png' % i
        assert job.result is None and job.parent_id is None
        tracked = PaperlessTask.objects.get(task_id=job.id)
        assert tracked.status == PaperlessTask.Status.PENDING
        assert tracked.trigger_source == PaperlessTask.TriggerSource.FOLDER_CONSUME
    first = jobs[0].id
    # Exercise original control-state helpers, not a fabricated business task.
    context = SimpleNamespace(name='documents.tasks.consume_file')
    task_prerun_handler(task_id=first, task=context)
    tracked = PaperlessTask.objects.get(task_id=first)
    assert tracked.status == PaperlessTask.Status.STARTED and tracked.date_started
    task_postrun_handler(task_id=first, task=context, retval=None, state='SUCCESS')
    tracked.refresh_from_db()
    assert tracked.status == PaperlessTask.Status.SUCCESS and tracked.date_done
    assert tracked.duration_seconds is not None and tracked.wait_time_seconds is not None
with route_tasks('scan:child', first):
    child = consume_file.apply_async(kwargs={'input_doc': obj, 'overrides': DocumentMetadataOverrides()}, countdown=60)
with Session(runtime.session_factory.kw['bind']) as session:
    queued_child = session.get(Job, child.id)
    assert queued_child.parent_id == first and queued_child.result is None
    assert queued_child.available_at > jobs[0].available_at
assert len(read_trace(root / 'trace.jsonl')) == 4
try:
    validate_graph(read_trace(root / 'trace.jsonl'), ['scan:0', 'scan:1', 'scan:2'], {'ingestion': 1}, [])
except WorkflowMismatch:
    pass
else:
    raise AssertionError('A publication-only trace must never claim successful execution')
from celery.app.task import Task
original = Task.apply_async
try:
    with route_tasks('unsupported'):
        from documents.tasks import train_classifier
        train_classifier.apply_async()
except RuntimeError:
    pass
else:
    raise AssertionError('Unknown business task must not be silently discarded')
assert Task.apply_async is original
import hashlib, reportlab
from PIL import ImageDraw, ImageFont
from documents.search import get_backend
get_backend()
scan = root / 'consume' / 'offline-real-ocr.png'
image = Image.new('RGB', (1800, 700), 'white')
font = ImageFont.truetype(str(Path(reportlab.__file__).parent / 'fonts/Vera.ttf'), 52)
draw = ImageDraw.Draw(image)
draw.text((90, 110), 'INVOICE 00420', fill='black', font=font)
draw.text((90, 220), 'Benchmark orchard payment received', fill='black', font=font)
image.save(scan, dpi=(200, 200))
original_digest = hashlib.sha256(scan.read_bytes()).hexdigest()
with route_tasks('offline-real-ocr'):
    assert _consume_file(scan, root / 'consume', subdirs_as_tags=False)
from examples.paperless_ngx_dbworker.runtime import handle
with Session(runtime.session_factory.kw['bind']) as session:
    actual_job = session.scalar(select(Job).where(Job.operation_id == 'offline-real-ocr'))
    identity = actual_job.id
    # Genuine business execution in-process: this is not a native worker or
    # performance admission, and no DBWorker ledger success is fabricated.
    handle(actual_job, session)
    session.commit()
from documents.models import Document
tracked = PaperlessTask.objects.get(task_id=identity)
assert tracked.status == PaperlessTask.Status.SUCCESS
document = Document.objects.get(pk=tracked.result_data['document_id'])
from benchmarks.upstream.paperless_ngx_backend import inspect_outputs
oracle = inspect_outputs([document], [{'marker': 'INVOICE 00420', 'sha256': original_digest}])
assert oracle[0]['archive_pages'] == 1 and 'orchard' in oracle[0]['content'].lower()
assert document.pk in get_backend().search_ids('orchard', None)
print(json.dumps({'native_origin': True, 'signed_serializer': True,
                  'original_folder_producer_jobs': 3, 'durable_child_jobs': 1,
                  'original_lifecycle_helpers': True, 'publication_is_not_execution': True,
                  'real_inprocess_ocr_and_archive': True}))
'''
            script = "try:\n" + textwrap.indent(script, "    ") + "\nexcept BaseException as exc:\n    import os, traceback\n    os.write(2, traceback.format_exc().encode())\n    raise\n"
            completed = subprocess.run([str(python), "-c", script], cwd=ROOT, env=env,
                                       capture_output=True, text=True, timeout=120)
            application_logs = "\n".join(path.read_text(errors="replace")[-12000:]
                for path in Path(temporary).rglob("*.log")) if completed.returncode else ""
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr + application_logs)
            result = json.loads(completed.stdout.splitlines()[-1])
            self.assertEqual(result["original_folder_producer_jobs"], 3)
            self.assertEqual(result["durable_child_jobs"], 1)
            self.assertTrue(result["publication_is_not_execution"])
            self.assertTrue(result["real_inprocess_ocr_and_archive"])


if __name__ == "__main__":
    unittest.main()
