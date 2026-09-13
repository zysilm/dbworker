import asyncio
import tempfile
import time
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from durable_worker_example import main
from durable_worker_example.api.routes import (
    ComparisonInput, ImportInput, WorkspaceInput, create_comparison,
    create_workspace, get_artifact, get_comparison, get_results, import_text_files,
)


class ApiTest(unittest.TestCase):
    def test_lifespan_runs_registered_handlers_in_process_pools(self) -> None:
        async def scenario(directory: str) -> None:
            settings = replace(main.settings, database_url=f"sqlite:///{directory}/app.db", poll_seconds=0.02)
            with patch.object(main, "settings", settings):
                app = main.create_app()
                async with main.lifespan(app):
                    request = SimpleNamespace(app=app)
                    workspace = create_workspace(WorkspaceInput(name="smoke"), request)
                    Path(directory, "a.txt").write_text("apple apple banana")
                    Path(directory, "b.txt").write_text("apple banana")
                    imported = import_text_files(workspace["id"], ImportInput(directory=directory), request)
                    self.assertEqual(imported["imported_documents"], 2)
                    comparison = create_comparison(1, ComparisonInput(retained_max_k=1), request)
                    self.assertIsNone(comparison["status"])
                    deadline = time.monotonic() + 20
                    while time.monotonic() < deadline:
                        status = get_comparison(comparison["id"], request)
                        if status["status"] == "finished":
                            break
                        await asyncio.sleep(0.02)
                    self.assertEqual(status["status"], "finished", status)
                    self.assertEqual(status["candidates_scored_count"], 1)
                    self.assertEqual(get_artifact(1, request)["status"], "finished")
                    self.assertEqual(get_artifact(2, request)["status"], "finished")
                    results = get_results(comparison["id"], request)
                    self.assertEqual(results[0]["candidate_artifact_id"], 2)
                    self.assertGreater(results[0]["score"], 0.9)
                self.assertFalse(app.state.coordinator._running)
        with tempfile.TemporaryDirectory() as directory:
            asyncio.run(scenario(directory))
