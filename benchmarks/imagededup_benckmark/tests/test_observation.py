import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from imagededup_benckmark.observation import observe_handler, read_records
from imagededup_benckmark import observation
from imagededup_benckmark.evidence import verify

try:
    from sqlalchemy import Integer, create_engine, text
    from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
except ImportError:
    Session = None


if Session is not None:
    class Base(DeclarativeBase):
        pass

    class ScoredCandidate(Base):
        __tablename__ = "scored_candidate"
        request_id: Mapped[int] = mapped_column(Integer, primary_key=True)
        candidate_artifact_id: Mapped[int] = mapped_column(Integer, primary_key=True)


@unittest.skipIf(Session is None, "Run commit observer checks in an application environment with SQLAlchemy")
class ObservationTest(unittest.TestCase):
    def test_continuation_commits_before_previous_postrun_do_not_inflate_page_work(self):
        with tempfile.TemporaryDirectory() as directory:
            database, output = Path(directory, "application.db"), Path(directory, "operations.jsonl")
            engine = create_engine(f"sqlite:///{database}")
            Base.metadata.create_all(engine)
            with engine.begin() as connection:
                connection.exec_driver_sql("CREATE TABLE comparison_request(id INTEGER, workspace_id INTEGER, candidates_scored_count INTEGER)")
                connection.exec_driver_sql("INSERT INTO comparison_request VALUES(1, 1, 0)")
            environment = {"IMAGE_OBSERVATION_DATABASE": str(database), "IMAGE_OBSERVATION_FILE": str(output),
                           "IMAGE_OBSERVATION_BACKEND": "celery"}
            observation.install_transaction_observers()
            with patch.dict(os.environ, environment):
                first = observation.begin("comparison", 1, task_id="first-page")
                token = observation._current_attempt.set(first)
                try:
                    with Session(engine) as session:
                        session.add_all(ScoredCandidate(request_id=1, candidate_artifact_id=key) for key in (2, 3))
                        session.flush()
                        session.execute(text("UPDATE comparison_request SET candidates_scored_count=2 WHERE id=1"))
                        session.commit()
                    # Original Celery publishes the continuation before postrun.
                    second = observation.begin("comparison", 1, task_id="second-page")
                    second_token = observation._current_attempt.set(second)
                    try:
                        with Session(engine) as session:
                            session.add(ScoredCandidate(request_id=1, candidate_artifact_id=4))
                            session.flush()
                            session.execute(text("UPDATE comparison_request SET candidates_scored_count=3 WHERE id=1"))
                            session.commit()
                        observation.end(second, "SUCCESS")
                    finally:
                        observation._current_attempt.reset(second_token)
                    observation.end(first, "SUCCESS")
                finally:
                    observation._current_attempt.reset(token)
            records = read_records(output)
            first_finish = next(row for row in records if row["event"] == "finished" and row["task_id"] == "first-page")
            self.assertEqual(first_finish["items_after"], 3)
            self.assertEqual(first_finish["page_items"], 2)
            self.assertEqual(first_finish["scored_rows"], [[1, 2], [1, 3]])
            self.assertTrue(verify(records, workspace=1, artifact_ids=[1, 2, 3, 4], request_ids=[1],
                                   new_builds=False, page_size=2, record_offset=0)["passed"])
            engine.dispose()

    def test_flushed_scoring_rows_rolled_back_are_not_claimed_as_committed_work(self):
        with tempfile.TemporaryDirectory() as directory:
            database, output = Path(directory, "application.db"), Path(directory, "operations.jsonl")
            engine = create_engine(f"sqlite:///{database}")
            Base.metadata.create_all(engine)
            with engine.begin() as connection:
                connection.exec_driver_sql("CREATE TABLE comparison_request(id INTEGER, workspace_id INTEGER, candidates_scored_count INTEGER)")
                connection.exec_driver_sql("INSERT INTO comparison_request VALUES(1, 1, 0)")
            environment = {"IMAGE_OBSERVATION_DATABASE": str(database), "IMAGE_OBSERVATION_FILE": str(output),
                           "IMAGE_OBSERVATION_BACKEND": "dbworker"}
            with patch.dict(os.environ, environment), Session(engine) as session:
                with observe_handler("comparison", 1, session):
                    session.add(ScoredCandidate(request_id=1, candidate_artifact_id=2))
                    session.flush()
                    session.rollback()
                session.commit()
            finished = read_records(output)[-1]
            self.assertEqual(finished["page_items"], 0)
            self.assertEqual(finished["scored_rows"], [])
            engine.dispose()

    def test_dbworker_scoring_evidence_waits_for_commit_after_handler_returns(self):
        with tempfile.TemporaryDirectory() as directory:
            database, output = Path(directory, "application.db"), Path(directory, "operations.jsonl")
            engine = create_engine(f"sqlite:///{database}")
            Base.metadata.create_all(engine)
            with engine.begin() as connection:
                connection.exec_driver_sql("CREATE TABLE comparison_request(id INTEGER, workspace_id INTEGER, candidates_scored_count INTEGER)")
                connection.exec_driver_sql("INSERT INTO comparison_request VALUES(1, 1, 0)")
            environment = {"IMAGE_OBSERVATION_DATABASE": str(database), "IMAGE_OBSERVATION_FILE": str(output),
                           "IMAGE_OBSERVATION_BACKEND": "dbworker"}
            with patch.dict(os.environ, environment), Session(engine) as session:
                with observe_handler("comparison", 1, session):
                    session.add(ScoredCandidate(request_id=1, candidate_artifact_id=2))
                    session.flush()
                    session.execute(text("UPDATE comparison_request SET candidates_scored_count=1 WHERE id=1"))
                self.assertEqual([row["event"] for row in read_records(output)], ["started"])
                session.commit()
            finished = read_records(output)[-1]
            self.assertEqual(finished["page_items"], 1)
            self.assertEqual(finished["scored_rows"], [[1, 2]])
            engine.dispose()

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
