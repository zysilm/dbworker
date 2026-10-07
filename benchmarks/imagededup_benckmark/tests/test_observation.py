import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from imagededup_benckmark.observation import observe_handler, read_records

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session
except ImportError:
    Session = None


@unittest.skipIf(Session is None, "Run commit observer checks in an application environment with SQLAlchemy")
class ObservationTest(unittest.TestCase):
    def test_success_evidence_waits_for_business_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            database, output = Path(directory, "application.db"), Path(directory, "operations.jsonl")
            engine = create_engine(f"sqlite:///{database}")
            with engine.begin() as connection:
                connection.exec_driver_sql("CREATE TABLE feature_artifact(id INTEGER, workspace_id INTEGER, hash_value TEXT)")
                connection.exec_driver_sql("INSERT INTO feature_artifact VALUES(1, 1, NULL)")
            environment = {"IMAGE_OBSERVATION_DATABASE": str(database), "IMAGE_OBSERVATION_FILE": str(output),
                           "IMAGE_OBSERVATION_BACKEND": "dbworker"}
            with patch.dict(os.environ, environment), Session(engine) as session:
                with observe_handler("build", 1, session):
                    session.execute(text("UPDATE feature_artifact SET hash_value='abc' WHERE id=1"))
                self.assertEqual([row["event"] for row in read_records(output)], ["started"])
                session.commit()
                records = read_records(output)
                self.assertEqual(records[-1]["state"], "SUCCESS")
                self.assertEqual(records[-1]["page_items"], 1)
            engine.dispose()

    def test_failure_does_not_claim_committed_work(self):
        with tempfile.TemporaryDirectory() as directory:
            database, output = Path(directory, "application.db"), Path(directory, "operations.jsonl")
            engine = create_engine(f"sqlite:///{database}")
            with engine.begin() as connection:
                connection.exec_driver_sql("CREATE TABLE comparison_request(id INTEGER, workspace_id INTEGER, candidates_scored_count INTEGER)")
                connection.exec_driver_sql("INSERT INTO comparison_request VALUES(1, 1, 0)")
            environment = {"IMAGE_OBSERVATION_DATABASE": str(database), "IMAGE_OBSERVATION_FILE": str(output),
                           "IMAGE_OBSERVATION_BACKEND": "dbworker"}
            with patch.dict(os.environ, environment), Session(engine) as session:
                with self.assertRaisesRegex(ValueError, "failure"):
                    with observe_handler("comparison", 1, session):
                        session.execute(text("UPDATE comparison_request SET candidates_scored_count=2 WHERE id=1"))
                        raise ValueError("failure")
                session.rollback()
                record = read_records(output)[-1]
                self.assertEqual((record["state"], record["page_items"]), ("FAILURE", 0))
            engine.dispose()
