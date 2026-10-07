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
            rows += [{**record, "event": "started"},
                     {**record, "event": "finished", "state": "SUCCESS", "page_items": width}]
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
