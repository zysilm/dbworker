import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient
from kombu.exceptions import OperationalError
from PIL import Image
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from imagededup_system_redis_celery.api.routes import router
from imagededup_system_redis_celery.db.engine import Base
from imagededup_system_redis_celery.db.models import (
    ComparisonRequest, ExecutionStatus, FeatureArtifact, OutboxMessage,
)
from imagededup_system_redis_celery.domain.artifact_build import build_artifact
from imagededup_system_redis_celery.outbox import enqueue, publish_pending
from imagededup_system_redis_celery.tasks import ImageTask


class ApiAndOutboxTest(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name)
        self.engine = create_engine(f"sqlite:///{self.path / 'test.db'}")
        self.addCleanup(self.engine.dispose)
        Base.metadata.create_all(self.engine)
        self.session_factory = sessionmaker(self.engine, expire_on_commit=False)
        app = FastAPI()
        app.state.session_factory = self.session_factory
        app.include_router(router)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)
        self.publish = patch("imagededup_system_redis_celery.api.routes.publish_pending", return_value=0)
        self.publish.start()
        self.addCleanup(self.publish.stop)
        self.workspace = self.client.post("/workspaces", json={"name": "images"}).json()["id"]

    def import_image(self) -> int:
        Image.new("RGB", (32, 32), "black").save(self.path / "image.png")
        response = self.client.post(f"/workspaces/{self.workspace}/imports", json={"directory": str(self.path)})
        self.assertEqual(response.status_code, 200, response.text)
        return int(response.json()["artifact_ids"][0])

    def test_api_import_build_and_missing_resources(self) -> None:
        key = self.import_image()
        repeated = self.client.post(f"/workspaces/{self.workspace}/imports", json={"directory": str(self.path)})
        self.assertEqual(repeated.json(), {"imported_images": 0, "artifact_ids": []})
        self.assertIsNone(self.client.get(f"/artifacts/{key}").json()["execution_status"])
        build_artifact(key, 0, self.session_factory)
        # Redelivery must leave revision and persisted output untouched.
        build_artifact(key, 0, self.session_factory)
        result = self.client.get(f"/artifacts/{key}").json()
        self.assertEqual(result["execution_status"], "finished")
        self.assertEqual(self.client.get(f"/artifacts/{key}/image").status_code, 200)
        self.assertEqual(len(self.client.get(f"/workspaces/{self.workspace}/artifacts").json()), 1)
        for route in ["/artifacts/999", "/comparisons/999", "/comparisons/999/results", "/workspaces/999/artifacts"]:
            self.assertEqual(self.client.get(route).status_code, 404)
        with self.session_factory() as session:
            artifact = session.get(FeatureArtifact, key)
            self.assertEqual(artifact.revision, 1)
            self.assertEqual(len(artifact.hash_value), 16)
            message = session.scalar(select(OutboxMessage))
            self.assertEqual((message.task_name, message.source_id, message.source_revision), ("images.build", key, 0))

    def test_comparison_submission_and_validation(self) -> None:
        key = self.import_image()
        response = self.client.post(f"/comparisons/{key}", json={"max_distance": 64, "retained_max_k": 2})
        self.assertEqual(response.status_code, 200)
        self.assertIsNone(response.json()["execution_status"])
        request_id = response.json()["id"]
        self.assertEqual(self.client.get(f"/comparisons/{request_id}").json()["candidates_scored_count"], 0)
        self.assertEqual(self.client.get(f"/comparisons/{request_id}/results").json(), [])
        self.assertEqual(self.client.post(f"/comparisons/{key}", json={"max_distance": 65}).status_code, 422)
        with self.session_factory() as session:
            stored = session.get(ComparisonRequest, request_id)
            self.assertEqual((stored.max_distance, stored.retained_max_k), (64, 2))

    def test_failed_build_reset_and_stale_failure_fencing(self) -> None:
        key = self.import_image()
        task = ImageTask()
        task.name = "images.build"
        with patch("imagededup_system_redis_celery.tasks.get_session_factory", return_value=self.session_factory):
            task.on_failure(ValueError("broken image"), "task", [key, 0], {}, None)
        self.assertEqual(self.client.get(f"/artifacts/{key}").json()["execution_status"], "failed")
        self.client.post(f"/workspaces/{self.workspace}/build-all")
        with self.session_factory() as session:
            artifact = session.get(FeatureArtifact, key)
            self.assertEqual((artifact.revision, artifact.execution_status), (2, ExecutionStatus.UNFINISHED))
        build_artifact(key, 2, self.session_factory)
        with patch("imagededup_system_redis_celery.tasks.get_session_factory", return_value=self.session_factory):
            task.on_failure(ValueError("stale failure"), "task", [key, 0], {}, None)
        self.assertEqual(self.client.get(f"/artifacts/{key}").json()["execution_status"], "finished")

    def test_broker_failure_retains_messages_and_recovery_deletes_only_sent(self) -> None:
        with self.session_factory.begin() as session:
            first = enqueue(session, "images.build", 1)
            second = enqueue(session, "images.compare", 2, 7, delay=1)
        with patch("imagededup_system_redis_celery.outbox.app.producer_or_acquire", return_value=nullcontext(object())):
            with patch("imagededup_system_redis_celery.outbox.app.send_task", side_effect=[None, OperationalError("offline")]):
                self.assertEqual(publish_pending(self.session_factory), 1)
            with self.session_factory() as session:
                self.assertIsNone(session.get(OutboxMessage, first))
                self.assertIsNotNone(session.get(OutboxMessage, second))
            with patch("imagededup_system_redis_celery.outbox.app.send_task") as send:
                self.assertEqual(publish_pending(self.session_factory), 1)
                self.assertEqual(send.call_args.kwargs["args"], [2, 7])
                self.assertEqual(send.call_args.kwargs["task_id"], second)
            with self.session_factory() as session:
                self.assertEqual(list(session.scalars(select(OutboxMessage))), [])

    def test_outbox_is_rolled_back_with_source_transaction(self) -> None:
        with self.assertRaises(ValueError):
            with self.session_factory.begin() as session:
                enqueue(session, "images.build", 1)
                raise ValueError("abort")
        with self.session_factory() as session:
            self.assertEqual(list(session.scalars(select(OutboxMessage))), [])
