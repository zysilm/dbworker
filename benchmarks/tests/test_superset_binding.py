"""Offline SQL Lab argument, output and committed-completion regressions."""
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine, insert, select, text
from sqlalchemy.orm import Session, sessionmaker

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from benchmarks.common.native_observer import argument_digest
from benchmarks.upstream.superset_backend import expected_rows, sql_for_operation
from dbworker import Claim, ExecutionStatus, Finished, LostClaim, _Worker, _execute_claim
from examples.superset_dbworker.executor import Base, SQLLabJob, completion_after_commit, original_arguments


class SupersetBindingTests(unittest.TestCase):
    def test_original_producer_argument_shape_is_preserved(self):
        kwargs = {"username": "benchmark", "return_results": False, "store_results": True,
                  "start_time": 123.25, "expand_data": False, "log_params": None}
        persisted = {"query_id": 17, "rendered_query": sql_for_operation("query-4"), **kwargs}
        positional, keywords = original_arguments(persisted)
        self.assertEqual(positional, [17, sql_for_operation("query-4")])
        self.assertEqual(keywords, kwargs)
        self.assertEqual(argument_digest(positional, keywords),
                         argument_digest((17, sql_for_operation("query-4")), kwargs))
        self.assertNotEqual(argument_digest(positional, keywords),
                            argument_digest((17, sql_for_operation("query-5")), kwargs))
        self.assertIn("query_id", persisted)

    def test_every_operation_has_distinct_independently_verified_full_table_output(self):
        with sqlite3.connect(":memory:") as db:
            db.execute("CREATE TABLE facts (category INTEGER, value INTEGER)")
            db.executemany("INSERT INTO facts VALUES (?,?)", ((i % 10, i) for i in range(10000)))
            for identity in ("warmup:0", "warmup:1", "query-0", "query-1", "query-99"):
                observed = [{"category": category, "total": total}
                            for category, total in db.execute(sql_for_operation(identity))]
                self.assertEqual(observed, expected_rows(identity))
            self.assertNotEqual(expected_rows("query-0"), expected_rows("query-1"))
            db.execute("UPDATE facts SET value=value+1 WHERE category=0")
            self.assertNotEqual(list(db.execute(sql_for_operation("query-0")))[0][1],
                                expected_rows("query-0")[0]["total"])

    def test_success_observation_waits_for_actual_commit(self):
        engine = create_engine("sqlite://")
        with Session(engine) as session, patch("examples.superset_dbworker.executor.record") as record:
            session.execute(text("CREATE TABLE ledger (state TEXT)"))
            session.commit()
            session.execute(text("INSERT INTO ledger VALUES ('finished')"))
            completion_after_commit(session, "query-0", "sql_lab:17")
            record.assert_not_called()
            session.commit()
            record.assert_called_once_with("sql_lab", "query-0", "sql_lab:17", "succeeded", backend="dbworker")
        engine.dispose()

    def test_rolled_back_completion_never_records_success(self):
        engine = create_engine("sqlite://")
        with Session(engine) as session, patch("examples.superset_dbworker.executor.record") as record:
            session.execute(text("SELECT 1"))
            completion_after_commit(session, "query-0", "sql_lab:17")
            session.rollback()
            record.assert_not_called()
        engine.dispose()

    def test_real_coordinator_commit_precedes_success_and_lost_claim_has_no_success(self):
        for claim_token in ("owner", "stale"):
            with self.subTest(claim_token=claim_token):
                engine = create_engine("sqlite://")
                sessions = sessionmaker(engine)

                def handler(job, session):
                    completion_after_commit(session, job.operation_id, "sql_lab:17")
                    return Finished()

                worker = _Worker(name="sql_lab", source=SQLLabJob, handler=handler)
                Base.metadata.create_all(engine)
                with sessions.begin() as session:
                    session.add(SQLLabJob(id=17, operation_id="query-0", arguments={}))
                    session.execute(insert(worker.table).values(source_id=17,
                        execution_status=ExecutionStatus.WORKING, claim_token="owner"))
                observed = []

                def observe(*args, **kwargs):
                    with sessions() as session:
                        observed.append(session.execute(select(worker.table.c.execution_status)).scalar_one())

                with patch("examples.superset_dbworker.executor.record", side_effect=observe):
                    if claim_token == "stale":
                        with self.assertRaises(LostClaim):
                            _execute_claim(worker, Claim(17, claim_token), sessions)
                        self.assertEqual(observed, [])
                    else:
                        _execute_claim(worker, Claim(17, claim_token), sessions)
                        self.assertEqual(observed, [ExecutionStatus.FINISHED])
                engine.dispose()


if __name__ == "__main__":
    unittest.main()
