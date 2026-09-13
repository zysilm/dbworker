import asyncio
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from durable_worker_example import main
from durable_worker_example.db.engine import Base, create_engine_and_session_factory
from durable_worker_example.db.models import ComparisonRequest, Document, FeatureArtifact, Workspace
from dbworker import ExecutionStatus
from sqlalchemy import update
from sqlalchemy.orm import Session
from durable_worker_example.api.routes import (
    ComparisonInput, ImportInput, WorkspaceInput, create_comparison,
    create_workspace, get_artifact, get_comparison, get_results, import_text_files,
)


class ApiTest(unittest.TestCase):
    def test_finished_response_reads_progress_after_completion(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            engine, session_factory = create_engine_and_session_factory(f"sqlite:///{directory}/response.db")
            try:
                Base.metadata.create_all(engine)
                with session_factory.begin() as session:
                    session.add(Workspace(id=1, name="test"))
                    session.add(Document(id=1, workspace_id=1, name="query", text="apple"))
                    session.add(FeatureArtifact(id=1, workspace_id=1, document_id=1))
                    session.add(ComparisonRequest(id=1, workspace_id=1, query_artifact_id=1))
                request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
                    session_factory=session_factory, coordinator=main.coordinator,
                )))

                def finish_before_status_read(session: Session, *, worker: str, source_id: object) -> ExecutionStatus:
                    with session_factory.begin() as writer:
                        writer.execute(update(ComparisonRequest).where(ComparisonRequest.id == source_id)
                                       .values(candidates_scored_count=1))
                    return ExecutionStatus.FINISHED

                with patch.object(main.coordinator, "execution_status", side_effect=finish_before_status_read):
                    response = get_comparison(1, request)
                self.assertIs(response["execution_status"], ExecutionStatus.FINISHED)
                self.assertEqual(response["candidates_scored_count"], 1)
            finally:
                engine.dispose()

    def test_lifespan_runs_registered_handlers_in_process_pools(self) -> None:
        async def scenario(directory: str) -> None:
            settings = replace(main.settings, database_url=f"sqlite:///{directory}/app.db", poll_seconds=0.02)
            engine, session_factory = create_engine_and_session_factory(settings.database_url)
            self.addCleanup(engine.dispose)
            with (
                patch.object(main, "engine", engine),
                patch.object(main, "session_factory", session_factory),
                patch.object(main.coordinator, "session_factory", session_factory),
                patch.object(main.coordinator, "database_url", settings.database_url),
                patch.object(main.coordinator, "poll_seconds", settings.poll_seconds),
            ):
                app = main.create_app()
                async with main.lifespan(app):
                    request = SimpleNamespace(app=app)
                    workspace = create_workspace(WorkspaceInput(name="smoke"), request)
                    Path(directory, "a.txt").write_text("apple apple banana")
                    Path(directory, "b.txt").write_text("apple banana")
                    imported = import_text_files(workspace["id"], ImportInput(directory=directory), request)
                    self.assertEqual(imported["imported_documents"], 2)
                    comparison = create_comparison(1, ComparisonInput(retained_max_k=1), request)
                    self.assertIsNone(comparison["execution_status"])
                    deadline = time.monotonic() + 20
                    while time.monotonic() < deadline:
                        status = get_comparison(comparison["id"], request)
                        if status["execution_status"] == "finished":
                            break
                        await asyncio.sleep(0.02)
                    self.assertEqual(status["execution_status"], "finished", status)
                    self.assertEqual(status["candidates_scored_count"], 1)
                    self.assertEqual(get_artifact(1, request)["execution_status"], "finished")
                    self.assertEqual(get_artifact(2, request)["execution_status"], "finished")
                    results = get_results(comparison["id"], request)
                    self.assertEqual(results[0]["candidate_artifact_id"], 2)
                    self.assertGreater(results[0]["score"], 0.9)
                self.assertFalse(app.state.coordinator._running)
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(scenario(directory))
