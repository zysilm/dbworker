"""Offline checks of native upload API permissions, payloads and queue boundaries."""
import ast
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


class PaperlessProducerTests(unittest.TestCase):
    def test_both_arms_use_authenticated_routed_api(self):
        source = (ROOT / "benchmarks/upstream/paperless_ngx_backend.py").read_text()
        ast.parse(source)
        self.assertNotIn("_consume_file", source)
        self.assertNotIn("force_authenticate", source)
        self.assertIn('client.post("/api/documents/post_document/"', source)
        self.assertIn('HTTP_AUTHORIZATION="Token "', source)
        self.assertEqual(source.count("queued = submit_api_scan(isolated_client, fixture, upload)"), 2)
        self.assertIn("PaperlessTask.TriggerSource.API_UPLOAD", source)
        self.assertIn('fixture["task_id"] = queued', source)

    def test_concurrent_routing_keeps_originals_until_last_producer_exits(self):
        from celery import Celery
        from celery.app.task import Task
        from kombu import Producer
        from examples.paperless_ngx_dbworker import runtime
        app = Celery("paperless-concurrent-unit", broker="memory://")
        self.addCleanup(app.close)
        @app.task(name="documents.tasks.consume_file")
        def consume(value):
            raise AssertionError("Producer routing must never execute ingestion")
        originals = (Task.apply_async, Producer.publish, Task.apply)
        both_entered = threading.Barrier(2)
        first_exited = threading.Event()
        observed = []
        def capture(task, args, kwargs, **options):
            observed.append((runtime._current.get()[0], args[0]))
            return args[0]
        def run(operation_id):
            with runtime.route_tasks(operation_id):
                both_entered.wait(timeout=5)
                if operation_id == "second":
                    self.assertTrue(first_exited.wait(timeout=5))
                self.assertEqual(consume.delay(operation_id), operation_id)
            if operation_id == "first":
                first_exited.set()
        with patch.object(runtime, "enqueue", side_effect=capture):
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [pool.submit(run, operation_id) for operation_id in ("first", "second")]
                for future in futures:
                    future.result(timeout=10)
        self.assertEqual(sorted(observed), [("first", "first"), ("second", "second")])
        self.assertEqual((Task.apply_async, Producer.publish, Task.apply), originals)

    def test_real_api_permissions_payload_and_dbworker_publication_without_worker(self):
        interpreter = Path(os.environ.get("PAPERLESS_TEST_PYTHON", str(ROOT / "benchmarks/environments/paperless_ngx/dbworker/.venv/bin/python")))
        if not interpreter.exists():
            self.skipTest("Provision the pinned Paperless environment for native API checks")
        script = r'''
import os, hashlib, io, faulthandler
faulthandler.dump_traceback_later(45, repeat=True)
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
root = Path(os.environ['PAPERLESS_API_TEST_DIRECTORY'])
for name in ('data/log', 'data/index', 'media', 'consume', 'scratch'):
    (root / name).mkdir(parents=True, exist_ok=True)
os.environ.update(DJANGO_SETTINGS_MODULE='paperless.settings',
 PAPERLESS_SECRET_KEY='offline-native-api-test-key', PAPERLESS_DATA_DIR=str(root/'data'),
 PAPERLESS_MEDIA_ROOT=str(root/'media'), PAPERLESS_CONSUMPTION_DIR=str(root/'consume'),
 PAPERLESS_SCRATCH_DIR=str(root/'scratch'), DBWORKER_DATABASE_URL='sqlite:///'+str(root/'jobs.db'),
 BENCHMARK_TRACE_PATH=str(root/'trace.jsonl'), BENCHMARK_BACKEND='dbworker')
from examples.paperless_ngx_dbworker.adapter import initialize
initialize()
from django.test.utils import override_settings
with override_settings(CACHES={'default': {'BACKEND': 'django.core.cache.backends.locmem.LocMemCache'}},
 CHANNEL_LAYERS={'default': {'BACKEND': 'channels.layers.InMemoryChannelLayer'}}):
 from django.core.management import call_command
 call_command('migrate', interactive=False, verbosity=0, skip_checks=True)
 from django.contrib.auth import get_user_model
 from django.contrib.auth.models import Permission
 from rest_framework.authtoken.models import Token
 from rest_framework.test import APIClient
 from django.core.files.uploadedfile import SimpleUploadedFile
 from documents.tasks import consume_file
 from documents.models import PaperlessTask
 from documents.data_models import DocumentSource
 from examples.paperless_ngx_dbworker.runtime import coordinator, route_tasks, Job, decode_arguments
 from sqlalchemy.orm import Session
 from sqlalchemy import select
 from PIL import Image
 buffer=io.BytesIO(); Image.new('RGB',(20,20),'white').save(buffer,format='PNG'); content=buffer.getvalue()
 def upload(): return SimpleUploadedFile('scan.png',content,content_type='image/png')
 client=APIClient()
 with patch.object(consume_file,'apply_async',return_value=SimpleNamespace(id='00000000-0000-4000-8000-000000000001')) as dispatch:
  response=client.post('/api/documents/post_document/',{'document':upload()},format='multipart')
  assert response.status_code in (401,403) and not dispatch.called
 user=get_user_model().objects.create_user(username='uploader', password='offline-test')
 token=Token.objects.create(user=user)
 client.credentials(HTTP_AUTHORIZATION='Token '+token.key)
 with patch.object(consume_file,'apply_async',return_value=SimpleNamespace(id='00000000-0000-4000-8000-000000000001')) as dispatch:
  response=client.post('/api/documents/post_document/',{'document':upload()},format='multipart')
  assert response.status_code == 403 and not dispatch.called
 user.user_permissions.add(Permission.objects.get(content_type__app_label='documents',codename='add_document'))
 with patch.object(consume_file,'apply_async',return_value=SimpleNamespace(id='00000000-0000-4000-8000-000000000001')) as dispatch:
  response=client.post('/api/documents/post_document/',{'document':SimpleUploadedFile('bad.png',b'\x00\x01\x00\x00unsupported\x00' * 20)},format='multipart')
  assert response.status_code == 400 and not dispatch.called
 native_calls=[]
 def capture(*args,**kwargs):
  native_calls.append((args,kwargs)); return SimpleNamespace(id='00000000-0000-4000-8000-000000000001')
 with patch.object(consume_file,'apply_async',side_effect=capture):
  response=client.post('/api/documents/post_document/',{'document':upload(),'created':'2021-01-01T00:00:00Z','title':'scan'},format='multipart')
  assert response.status_code == 200
 args,native=native_calls[0]
 assert not args and native['headers']['trigger_source'] == PaperlessTask.TriggerSource.API_UPLOAD
 assert native['kwargs']['input_doc'].source == DocumentSource.ApiUpload
 assert native['kwargs']['input_doc'].original_file.read_bytes()==content
 assert native['kwargs']['overrides'].owner_id==user.pk
 runtime=coordinator(os.environ['DBWORKER_DATABASE_URL'])
 for i in range(3):
  with route_tasks(str(i)):
   response=client.post('/api/documents/post_document/',{'document':upload(),'created':'2021-01-01T00:00:00Z','title':'scan'},format='multipart')
   assert response.status_code == 200
  with Session(runtime.session_factory.kw['bind']) as session:
   job=session.get(Job,response.data); args,kwargs=decode_arguments(job.arguments)
   assert not args and kwargs['input_doc'].source==DocumentSource.ApiUpload
   assert hashlib.sha256(kwargs['input_doc'].original_file.read_bytes()).digest()==hashlib.sha256(content).digest()
   assert kwargs['overrides']==native['kwargs']['overrides']
   assert job.headers==native['headers'] and job.parent_id is None and job.result is None
   tracked=PaperlessTask.objects.get(task_id=job.id)
   assert tracked.status==PaperlessTask.Status.PENDING and tracked.trigger_source==PaperlessTask.TriggerSource.API_UPLOAD
 with Session(runtime.session_factory.kw['bind']) as session:
  assert len(session.scalars(select(Job)).all()) == 3
 from benchmarks.common.workflow_graph import read_trace
 events=read_trace(root/'trace.jsonl')
 assert len(events)==3 and {e['operation_id'] for e in events}=={'0','1','2'}
 assert all(e['event']=='submitted' for e in events)
 print('native authenticated upload API and durable per-upload jobs passed')
'''
        with tempfile.TemporaryDirectory() as temporary:
            environment = os.environ.copy()
            environment.update(PAPERLESS_API_TEST_DIRECTORY=temporary,
                PYTHONPATH=os.pathsep.join((str(ROOT), str(ROOT / "src"))),
                PYTHONPYCACHEPREFIX=str(Path(temporary) / "pycache"))
            try:
                result = subprocess.run([str(interpreter), "-c", script], cwd=ROOT,
                    env=environment, capture_output=True, text=True, timeout=180)
            except subprocess.TimeoutExpired as error:
                diagnostic = error.stderr or b""
                if isinstance(diagnostic, bytes):
                    diagnostic = diagnostic.decode(errors="replace")
                self.fail("Native API correctness check timed out:\n" + diagnostic)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertIn("durable per-upload jobs passed", result.stdout)


if __name__ == "__main__":
    unittest.main()
