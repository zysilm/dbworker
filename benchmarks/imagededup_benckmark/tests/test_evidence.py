import unittest

from imagededup_benckmark.evidence import verify


class EvidenceTest(unittest.TestCase):
    def records(self, *, widths=(2, 1), build=True):
        rows = []
        operations = [("comparison", 9, width) for width in widths]
        if build:
            operations += [("build", key, 1) for key in (1, 2, 3, 4)]
        for index, (stage, source_id, width) in enumerate(operations):
            record = {"attempt_id": str(index), "workspace_id": 1, "stage": stage, "source_id": source_id}
            details = {}
            if stage == "comparison":
                offset = sum(item[2] for item in operations[:index] if item[0] == "comparison")
                details = {"scored_rows": [[source_id, 2 + (offset + n) % 3] for n in range(width)],
                           "page_accounting": "transaction_committed_orm_inserts"}
            if stage == "build":
                details = {"built_artifact_ids": [source_id] if width else [],
                           "build_accounting": "transaction_committed_hash_writes"}
            rows += [{**record, "event": "started"},
                     {**record, "event": "finished", "state": "SUCCESS", "page_items": width, **details}]
        return rows

    def check(self, rows, *, build=True):
        return verify(rows, workspace=1, artifact_ids=[1, 2, 3, 4], request_ids=[9],
                      new_builds=build, page_size=2, record_offset=0)

    def test_checks_individual_builds_and_page_work(self):
        evidence = self.check(self.records())
        self.assertEqual(evidence["completed_builds"], 4)
        self.assertEqual(evidence["scored_pairs"], 3)
        self.assertEqual(evidence["comparisons"][0]["page_sizes"], [2, 1])

    def test_rejects_missing_build_and_oversized_page(self):
        with self.assertRaisesRegex(AssertionError, "build work"):
            self.check(self.records(build=False))
        with self.assertRaisesRegex(AssertionError, "oversized"):
            self.check(self.records(widths=(3,)))

    def test_rejects_missing_duplicate_pair_work_and_failure(self):
        for widths in ((2,), (2, 2)):
            with self.assertRaisesRegex(AssertionError, "candidate pairs"):
                self.check(self.records(widths=widths))
        rows = self.records()
        rows[1]["state"] = "FAILURE"
        with self.assertRaisesRegex(AssertionError, "failure or retry"):
            self.check(rows)

    def test_control_empty_waits_are_not_business_work(self):
        evidence = self.check(self.records(widths=(0, 2, 1, 0), build=False), build=False)
        self.assertEqual(evidence["empty_comparison_attempts"], 2)
        self.assertEqual(evidence["scored_pairs"], 3)

    def test_rejects_duplicate_candidate_identity_even_when_total_pair_count_matches(self):
        rows = self.records()
        rows[3]["scored_rows"] = [[9, 2]]
        with self.assertRaisesRegex(AssertionError, "duplicated candidate identities"):
            self.check(rows)

    def test_rejects_counter_only_evidence_and_unknown_candidate_identity(self):
        rows = self.records()
        del rows[1]["scored_rows"]
        with self.assertRaisesRegex(AssertionError, "transaction-attributed"):
            self.check(rows)
        rows = self.records()
        rows[1]["scored_rows"][0][1] = 99
        with self.assertRaisesRegex(AssertionError, "unknown input"):
            self.check(rows)

    def test_rejects_query_itself_replacing_a_candidate_even_when_counts_match(self):
        rows = self.records()
        rows[3]["scored_rows"] = [[9, 1]]
        with self.assertRaisesRegex(AssertionError, "exact submitted candidate identities"):
            self.check(rows)

    def test_zero_write_duplicate_build_delivery_is_kept_as_diagnostic(self):
        rows = self.records()
        record = {"attempt_id": "stale-build", "workspace_id": 1, "stage": "build", "source_id": 1}
        rows += [{**record, "event": "started"},
                 {**record, "event": "finished", "state": "SUCCESS", "page_items": 0,
                  "build_accounting": "transaction_committed_hash_writes", "built_artifact_ids": [],
                  "hash_write_observations": [{"artifact_id": 1, "matched_rows": 0, "kind": "bulk_update"}]}]
        checked = self.check(rows)
        self.assertEqual(checked["completed_builds"], 4)
        self.assertEqual(checked["duplicate_build_deliveries"], 1)
        self.assertEqual(checked["zero_match_hash_update_attempts"], 1)

    def test_actual_duplicate_positive_build_write_is_rejected(self):
        rows = self.records()
        record = {"attempt_id": "duplicate-positive-build", "workspace_id": 1, "stage": "build", "source_id": 1}
        rows += [{**record, "event": "started"},
                 {**record, "event": "finished", "state": "SUCCESS", "page_items": 1,
                  "build_accounting": "transaction_committed_hash_writes", "built_artifact_ids": [1]}]
        with self.assertRaisesRegex(AssertionError, "build work"):
            self.check(rows)

    def test_counter_only_or_forged_hash_identity_cannot_certify_build_work(self):
        for built in (None, [99]):
            with self.subTest(built=built):
                rows = self.records()
                rows[5]["built_artifact_ids"] = built
                with self.assertRaisesRegex(AssertionError, "transaction-attributed hash"):
                    self.check(rows)
