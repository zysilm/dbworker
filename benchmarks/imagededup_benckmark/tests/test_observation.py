import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from imagededup_benckmark.observation import observe_handler, read_records
from imagededup_benckmark import observation
from imagededup_benckmark.evidence import verify

try:
    from sqlalchemy import Integer, String, create_engine, text, update
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

    class FeatureArtifact(Base):
        __tablename__ = "feature_artifact"
        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        workspace_id: Mapped[int] = mapped_column(Integer)
        hash_value: Mapped[str | None] = mapped_column(String, nullable=True)
        revision: Mapped[int] = mapped_column(Integer, default=0)
        execution_status: Mapped[str | None] = mapped_column(String, nullable=True)


@unittest.skipIf(Session is None, "Run commit observer checks in an application environment with SQLAlchemy")
class ObservationTest(unittest.TestCase):
    def build_fixture(self, directory):
        database, output = Path(directory, "application.db"), Path(directory, "operations.jsonl")
        engine = create_engine(f"sqlite:///{database}")
        Base.metadata.create_all(engine)
        with Session(engine) as session:
            session.add(FeatureArtifact(id=1, workspace_id=1, hash_value=None, revision=0))
            session.commit()
        environment = {"IMAGE_OBSERVATION_DATABASE": str(database), "IMAGE_OBSERVATION_FILE": str(output),
                       "IMAGE_OBSERVATION_BACKEND": "celery"}
        observation.install_transaction_observers()
        return engine, output, environment

    def test_stale_delivery_does_not_claim_other_tasks_committed_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, output, environment = self.build_fixture(directory)
            with patch.dict(os.environ, environment):
                stale = observation.begin("build", 1, task_id="stale-delivery")
                winner = observation.begin("build", 1, task_id="winning-delivery")
                token = observation._current_attempt.set(winner)
                try:
                    with Session(engine) as session:
                        result = session.execute(update(FeatureArtifact).where(
                            FeatureArtifact.id == 1, FeatureArtifact.revision == 0,
                        ).values(hash_value="abc", revision=1))
                        self.assertEqual(result.rowcount, 1)
                        session.commit()
                    observation.end(winner, "SUCCESS")
                finally:
                    observation._current_attempt.reset(token)
                # The stale task's original revision guard returns before hashing.
                observation.end(stale, "SUCCESS")
            records = read_records(output)
            stale_finish = next(row for row in records if row["event"] == "finished" and row["task_id"] == "stale-delivery")
            self.assertEqual((stale_finish["items"], stale_finish["items_after"]), (0, 1))
            self.assertEqual(stale_finish["built_artifact_ids"], [])
            self.assertEqual(stale_finish["page_items"], 0)
            checked = verify(records, workspace=1, artifact_ids=[1], request_ids=[],
                             new_builds=True, page_size=250, record_offset=0)
            self.assertEqual(checked["completed_builds"], 1)
            self.assertEqual(checked["duplicate_build_deliveries"], 1)
            engine.dispose()

    def test_zero_match_hash_update_preserves_result_and_remains_diagnostic(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, output, environment = self.build_fixture(directory)
            with patch.dict(os.environ, environment), Session(engine) as session:
                record = observation.begin("build", 1, task_id="stale-cas")
                token = observation._current_attempt.set(record)
                try:
                    result = session.execute(update(FeatureArtifact).where(
                        FeatureArtifact.id == 1, FeatureArtifact.revision == 99,
                    ).values(hash_value="abc", revision=100))
                    self.assertEqual(result.rowcount, 0)
                    session.commit()
                    observation.end(record, "SUCCESS")
                finally:
                    observation._current_attempt.reset(token)
            finished = read_records(output)[-1]
            self.assertEqual(finished["page_items"], 0)
            self.assertEqual(finished["built_artifact_ids"], [])
            self.assertEqual(finished["hash_write_observations"], [{"artifact_id": 1, "matched_rows": 0, "kind": "bulk_update"}])
            engine.dispose()

    def test_bulk_hash_update_rollback_and_status_only_commit_do_not_count_builds(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, output, environment = self.build_fixture(directory)
            environment["IMAGE_OBSERVATION_BACKEND"] = "dbworker"
            with patch.dict(os.environ, environment), Session(engine) as session:
                with observe_handler("build", 1, session):
                    result = session.execute(update(FeatureArtifact).where(FeatureArtifact.id == 1).values(hash_value="abc"))
                    self.assertEqual(result.rowcount, 1)
                    session.rollback()
                    session.execute(update(FeatureArtifact).where(FeatureArtifact.id == 1).values(execution_status="working"))
                session.commit()
            finished = read_records(output)[-1]
            self.assertEqual(finished["page_items"], 0)
            self.assertEqual(finished["built_artifact_ids"], [])
            self.assertEqual(finished["hash_write_observations"], [])
            engine.dispose()

    def test_orm_dirty_hash_is_counted_only_after_outer_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, output, environment = self.build_fixture(directory)
            environment["IMAGE_OBSERVATION_BACKEND"] = "dbworker"
            with patch.dict(os.environ, environment), Session(engine) as session:
                with observe_handler("build", 1, session):
                    session.get(FeatureArtifact, 1).hash_value = "abc"
                    session.flush()
                self.assertEqual([row["event"] for row in read_records(output)], ["started"])
                session.commit()
            finished = read_records(output)[-1]
            self.assertEqual(finished["built_artifact_ids"], [1])
            self.assertEqual(finished["page_items"], 1)
            self.assertEqual(finished["hash_write_observations"][0]["kind"], "orm_flush")
            engine.dispose()

    def test_bulk_update_of_loaded_artifact_is_not_double_counted_as_orm_dirty(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, output, environment = self.build_fixture(directory)
            environment["IMAGE_OBSERVATION_BACKEND"] = "dbworker"
            with patch.dict(os.environ, environment), Session(engine) as session:
                with observe_handler("build", 1, session):
                    artifact = session.get(FeatureArtifact, 1)
                    session.execute(update(FeatureArtifact).where(FeatureArtifact.id == 1).values(hash_value="abc"))
                    session.flush()
                    self.assertEqual(artifact.hash_value, "abc")
                session.commit()
            finished = read_records(output)[-1]
            self.assertEqual(finished["built_artifact_ids"], [1])
            self.assertEqual(len(finished["hash_write_observations"]), 1)
            engine.dispose()

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
            Base.metadata.create_all(engine)
            with engine.begin() as connection:
                connection.exec_driver_sql("INSERT INTO feature_artifact(id,workspace_id,hash_value,revision) VALUES(1, 1, NULL, 0)")
            environment = {"IMAGE_OBSERVATION_DATABASE": str(database), "IMAGE_OBSERVATION_FILE": str(output),
                           "IMAGE_OBSERVATION_BACKEND": "dbworker"}
            with patch.dict(os.environ, environment), Session(engine) as session:
                with observe_handler("build", 1, session):
                    session.execute(update(FeatureArtifact).where(FeatureArtifact.id == 1).values(hash_value="abc"))
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
