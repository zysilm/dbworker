"""Boundary replay rejects omitted work and independently shortened durations."""
import copy
import unittest
from unittest.mock import patch

from benchmarks.common.timing_evidence import (
    begin_window, elapsed_seconds, end_window, validate_timing_window,
)
from benchmarks.common.workflow_graph import WorkflowMismatch


def fixture():
    window = {"schema_version": 1, "clock_domain": "unix_time_ns",
              "start": {"timestamp_ns": 1_000_000_000, "monotonic_ns": 10_000, "uncertainty_ns": 0},
              "end": {"timestamp_ns": 3_000_000_000, "monotonic_ns": 2_000_010_000, "uncertainty_ns": 0}}
    events = [{"operation_id": "op", "event": phase, "timestamp_ns": timestamp}
              for phase, timestamp in (("submitted", 1_100_000_000), ("started", 1_200_000_000),
                                       ("succeeded", 2_900_000_000))]
    return window, events


class TimingEvidenceTests(unittest.TestCase):
    def test_capture_brackets_monotonic_clock_and_uses_exact_duration(self):
        with patch('benchmarks.common.timing_evidence.time.time_ns', side_effect=[100, 110, 200, 210]), \
                patch('benchmarks.common.timing_evidence.time.monotonic_ns', side_effect=[1000, 1100]):
            window = end_window(begin_window())
        self.assertEqual(window['start'], {'timestamp_ns': 105, 'monotonic_ns': 1000, 'uncertainty_ns': 5})
        self.assertEqual(elapsed_seconds(window), 1e-7)

    def test_business_coverage_without_global_file_order_requirement(self):
        window, events = fixture()
        evidence = validate_timing_window(window, 2, list(reversed(events)), ['op'])
        self.assertEqual(evidence['duration_ns'], 2_000_000_000)

    def test_missing_legacy_window_is_not_newly_admitted(self):
        for window in (None, {}, {'schema_version': True}):
            with self.subTest(window=window), self.assertRaises(WorkflowMismatch):
                validate_timing_window(window, 2, fixture()[1], ['op'])

    def test_duration_cannot_be_shortened_or_nonfinite(self):
        window, events = fixture()
        for duration in (1, True, '2', float('nan'), float('inf'), -2):
            with self.subTest(duration=duration), self.assertRaises(WorkflowMismatch):
                validate_timing_window(window, duration, events, ['op'])

    def test_submission_and_terminal_work_cannot_escape_interval(self):
        window, events = fixture()
        for index, timestamp in ((0, 999_999_999), (2, 3_000_000_001)):
            altered = copy.deepcopy(events)
            altered[index]['timestamp_ns'] = timestamp
            with self.subTest(index=index), self.assertRaises(WorkflowMismatch):
                validate_timing_window(window, 2, altered, ['op'])

    def test_bad_boundary_and_event_values_fail_closed(self):
        window, events = fixture()
        for value in (None, True, 0, -1, '1', 1.5):
            altered = copy.deepcopy(window)
            altered['start']['timestamp_ns'] = value
            with self.subTest(value=value), self.assertRaises(WorkflowMismatch):
                validate_timing_window(altered, 2, events, ['op'])
            altered_events = copy.deepcopy(events)
            altered_events[0]['timestamp_ns'] = value
            with self.subTest(event_value=value), self.assertRaises(WorkflowMismatch):
                validate_timing_window(window, 2, altered_events, ['op'])

    def test_clock_jump_and_excessive_uncertainty_rejected(self):
        window, events = fixture()
        for update in ({'timestamp_ns': 3_100_000_000}, {'uncertainty_ns': 2_000_000},
                       {'monotonic_ns': 10_000}):
            altered = copy.deepcopy(window)
            altered['end'].update(update)
            with self.subTest(update=update), self.assertRaises(WorkflowMismatch):
                validate_timing_window(altered, 2, events, ['op'])

    def test_only_captured_sampling_uncertainty_allows_coverage_slack(self):
        window, events = fixture()
        window['start']['uncertainty_ns'] = 10
        events[0]['timestamp_ns'] = 999_999_990
        validate_timing_window(window, 2, events, ['op'])
        events[0]['timestamp_ns'] -= 1
        with self.assertRaises(WorkflowMismatch):
            validate_timing_window(window, 2, events, ['op'])

    def test_each_measured_operation_requires_submission_and_completion(self):
        window, events = fixture()
        for selected in (events[:-1], events[1:], events):
            operations = ['op', 'missing'] if selected is events else ['op']
            with self.subTest(selected=selected), self.assertRaises(WorkflowMismatch):
                validate_timing_window(window, 2, selected, operations)
        warmup = {'operation_id': 'warmup:0', 'event': 'succeeded', 'timestamp_ns': 1}
        validate_timing_window(window, 2, events + [warmup], ['op'])


if __name__ == '__main__':
    unittest.main()
