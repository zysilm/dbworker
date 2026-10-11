"""Validate and summarize sampled handoff traces; never declare a benchmark winner."""
from __future__ import annotations

import argparse
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

PHASES = ('submit_begin', 'submit_end', 'execution_enter', 'handler_enter',
          'handler_exit', 'execution_exit', 'future_done')
CLOCK_TOLERANCE_NS = 10_000_000


def _read(path: Path, errors: list[str]) -> list[dict]:
    rows = []
    try:
        with path.open() as stream:
            for number, line in enumerate(stream, 1):
                try:
                    row = json.loads(line)
                    if not isinstance(row, dict):
                        raise ValueError('Expected an object')
                    rows.append(row)
                except (ValueError, TypeError) as error:
                    errors.append(f'{path.name}:{number}: invalid JSON object: {error}')
    except OSError as error:
        errors.append(f'Cannot read {path.name}: {error}')
    return rows


def _identity(row: dict) -> tuple[int, str]:
    source, token = row.get('source_id'), row.get('claim_token_sha256')
    if type(source) is not int:
        raise ValueError('Invalid integer source_id')
    if not isinstance(token, str) or not re.fullmatch('[0-9a-f]{64}', token):
        raise ValueError('Invalid claim_token_sha256')
    return source, token


def _clock(row: dict) -> None:
    if not isinstance(row, dict):
        raise ValueError('Invalid clock object')
    for name in ('timestamp_ns', 'monotonic_ns'):
        if type(row.get(name)) is not int or row[name] < 0:
            raise ValueError(f'Invalid {name}')


def _summary(values: list[int]) -> dict:
    ordered = sorted(value / 1_000_000 for value in values)
    if not ordered:
        return {'count': 0, 'median_ms': None, 'mean_ms': None, 'p95_ms': None}
    position = (len(ordered) - 1) * .95
    lower = int(position)
    p95 = ordered[lower] + (ordered[min(lower + 1, len(ordered) - 1)] - ordered[lower]) * (position - lower)
    return {'count': len(ordered), 'median_ms': statistics.median(ordered),
            'mean_ms': statistics.mean(ordered), 'p95_ms': p95}


def analyze(trace: Path | str, workflow: Path | str | None = None) -> dict:
    """Keep valid sample statistics while explicitly invalidating incomplete evidence."""
    trace = Path(trace)
    errors: list[str] = []
    markers = sorted(str(path) for path in trace.parent.glob(trace.name + '.invalid-*'))
    if markers:
        errors.append('Diagnostic invalid markers exist')
    log = trace.parent / 'worker.log'
    if log.exists():
        try:
            with log.open(errors='replace') as stream:
                if any('DBWORKER_HANDOFF_DIAGNOSTIC_INVALID' in line for line in stream):
                    errors.append('Worker log contains the diagnostic invalid sentinel')
        except OSError:
            errors.append('Cannot inspect worker log for diagnostic failures')
    rows = _read(trace, errors)
    groups: dict[tuple[int, str], list[dict]] = defaultdict(list)
    batches: dict[tuple[int, int, str], dict] = {}
    memberships: dict[tuple[int, str], list[dict]] = defaultdict(list)
    for number, row in enumerate(rows, 1):
        try:
            _clock(row)
            if type(row.get('pid')) is not int or row['pid'] <= 0:
                raise ValueError('Invalid process ID')
            if not isinstance(row.get('worker'), str) or not row['worker']:
                raise ValueError('Invalid worker identity')
            if row.get('event') == 'claim_batch':
                for name in ('start', 'end'):
                    _clock(row[name])
                duration = row['end']['monotonic_ns'] - row['start']['monotonic_ns']
                if duration < 0 or type(row.get('duration_ns')) is not int or duration != row['duration_ns']:
                    raise ValueError('Invalid claim batch duration')
                if type(row.get('limit')) is not int or row['limit'] < 1:
                    raise ValueError('Invalid claim batch limit')
                if type(row.get('batch_sequence')) is not int or row['batch_sequence'] < 1:
                    raise ValueError('Invalid batch sequence')
                if not isinstance(row.get('claims'), list) or any(not isinstance(claim, dict) for claim in row['claims']):
                    raise ValueError('Invalid batch claim container')
                identities = [_identity(claim) for claim in row['claims']]
                if len(identities) > row['limit'] or len(set(identities)) != len(identities):
                    raise ValueError('Invalid or duplicate batch claims')
                key = row['pid'], row['batch_sequence'], row['worker']
                if key in batches:
                    raise ValueError('Duplicate claim batch')
                batches[key] = row
                for identity in identities:
                    memberships[identity].append(row)
            elif row.get('event') in PHASES:
                identity = _identity(row)
                if identity[0] % 32:
                    raise ValueError('Source ID violates the fixed sampling rule')
                groups[identity].append(row)
            else:
                raise ValueError('Unknown or failed diagnostic event')
        except (KeyError, ValueError, TypeError) as error:
            errors.append(f'Trace row {number}: {error}')

    workflow_rows = _read(Path(workflow), errors) if workflow else []
    measured_workflow = []
    for number, row in enumerate(workflow_rows, 1):
        try:
            operation = row['operation_id']
            if isinstance(operation, str) and operation.startswith('warmup:'):
                continue
            if not isinstance(operation, str) or not operation:
                raise ValueError('Invalid operation_id')
            if (type(row.get('timestamp_ns')) is not int or row['timestamp_ns'] < 0
                    or type(row.get('process_id')) is not int or row['process_id'] <= 0
                    or not isinstance(row.get('event'), str)):
                raise ValueError('Invalid workflow clock or process ID')
            if not isinstance(row.get('stage'), str) or row.get('backend') != 'dbworker':
                raise ValueError('Workflow must contain dbworker stage identities')
            measured_workflow.append(row)
        except (KeyError, ValueError, TypeError) as error:
            errors.append(f'Workflow row {number}: {error}')
    metrics: dict[str, list[int]] = defaultdict(list)
    valid_samples, invalid_samples, excluded = [], [], 0
    used_batches = set()
    expected = set(groups) | {identity for identity in memberships if identity[0] % 32 == 0}
    for identity in sorted(expected):
        events = groups.get(identity, [])
        sample_errors = []
        by_phase: dict[str, list[dict]] = defaultdict(list)
        for row in events:
            by_phase[row['event']].append(row)
        enters = by_phase['handler_enter']
        if len(enters) == 1 and str(enters[0].get('operation_id', '')).startswith('warmup:'):
            excluded += 1
            continue
        values = {}
        unavailable = []
        try:
            if any(len(by_phase[name]) != 1 for name in PHASES):
                raise ValueError('Missing or duplicate required phases')
            event = {name: by_phase[name][0] for name in PHASES}
            parent = {event[name]['pid'] for name in ('submit_begin', 'submit_end', 'future_done')}
            child = {event[name]['pid'] for name in ('execution_enter', 'handler_enter', 'handler_exit', 'execution_exit')}
            if len(parent) != 1 or len(child) != 1 or parent == child:
                raise ValueError('Inconsistent parent/child process identities')
            if len({row['worker'] for row in events}) != 1:
                raise ValueError('Inconsistent worker identity')
            offsets = [row['timestamp_ns'] - row['monotonic_ns'] for row in events]
            if max(offsets) - min(offsets) > CLOCK_TOLERANCE_NS:
                raise ValueError('Cross-process clock offsets exceed tolerance')
            if any(event[name].get('status') != 'returned' for name in ('handler_exit', 'execution_exit')):
                raise ValueError('Business execution failed or lacks returned status')
            if event['future_done'].get('cancelled') is not False:
                raise ValueError('Future cancelled or lacks cancellation evidence')
            order = [('submit_begin', 'submit_end'), ('submit_begin', 'execution_enter'),
                     ('execution_enter', 'handler_enter'), ('handler_enter', 'handler_exit'),
                     ('handler_exit', 'execution_exit'), ('execution_exit', 'future_done'),
                     ('submit_end', 'future_done')]
            if any(event[a]['monotonic_ns'] > event[b]['monotonic_ns'] for a, b in order):
                raise ValueError('Invalid monotonic phase ordering')
            member = memberships[identity]
            if len(member) != 1:
                raise ValueError('Missing or ambiguous claim batch membership')
            batch = member[0]
            if batch['pid'] not in parent or batch['worker'] != event['submit_begin']['worker']:
                raise ValueError('Claim batch parent/worker differs from submission')
            if batch['end']['monotonic_ns'] > event['submit_begin']['monotonic_ns']:
                raise ValueError('Submission precedes ownership commit')
            values = {name: event[end]['monotonic_ns'] - event[start]['monotonic_ns'] for name, start, end in (
                ('submit_call', 'submit_begin', 'submit_end'),
                ('submit_to_execution_entry', 'submit_begin', 'execution_enter'),
                ('execution_entry_to_handler_entry', 'execution_enter', 'handler_enter'),
                ('handler_exit_to_execution_exit', 'handler_exit', 'execution_exit'),
                ('execution_exit_to_future_done', 'execution_exit', 'future_done'))}
            operation, stage = event['handler_enter'].get('operation_id'), event['handler_enter'].get('stage')
            if any(value is not None and not isinstance(value, str) for value in (operation, stage)):
                raise ValueError('Invalid handler operation/stage type')
            if (operation, stage) != (event['handler_exit'].get('operation_id'), event['handler_exit'].get('stage')):
                raise ValueError('Handler operation/stage changes')
            spans = event['execution_exit'].get('observer_spans')
            if not isinstance(spans, list) or any(not isinstance(span, dict) for span in spans):
                raise ValueError('Missing observer span container')
            observed = {}
            for phase in ('started', 'succeeded'):
                matches = [span for span in spans if span.get('phase') == phase and span.get('stage') == stage]
                if len(matches) > 1 or (workflow and len(matches) != 1):
                    raise ValueError(f'Missing or ambiguous {phase} observer span')
                if not matches:
                    unavailable.append(f'observer_{phase}_write')
                    continue
                span = matches[0]
                _clock(span['start']); _clock(span['end'])
                duration = span['end']['monotonic_ns'] - span['start']['monotonic_ns']
                if duration < 0 or type(span.get('duration_ns')) is not int or span['duration_ns'] != duration:
                    raise ValueError('Invalid observer duration')
                if not (event['handler_enter']['monotonic_ns'] <= span['start']['monotonic_ns']
                        <= span['end']['monotonic_ns'] <= event['execution_exit']['monotonic_ns']):
                    raise ValueError('Observer span lies outside execution')
                if phase == 'started' and span['end']['monotonic_ns'] > event['handler_exit']['monotonic_ns']:
                    raise ValueError('Started observer finishes after handler exit')
                if phase == 'succeeded' and span['start']['monotonic_ns'] < event['handler_exit']['monotonic_ns']:
                    raise ValueError('Succeeded observer precedes handler exit')
                for clock in (span['start'], span['end']):
                    offset = clock['timestamp_ns'] - clock['monotonic_ns']
                    if min(offsets) - CLOCK_TOLERANCE_NS > offset or offset > max(offsets) + CLOCK_TOLERANCE_NS:
                        raise ValueError('Observer clock offset differs from execution')
                observed[phase] = span
                values[f'observer_{phase}_write'] = duration
            if 'started' in observed:
                values['handler_prestart'] = observed['started']['start']['monotonic_ns'] - event['handler_enter']['monotonic_ns']
            else:
                unavailable.append('handler_prestart')
            if workflow:
                if not operation or not stage:
                    raise ValueError('Missing native workflow identity')
                matched = [row for row in measured_workflow if row['operation_id'] == operation
                           and row['stage'] == stage and row['process_id'] in child]
                for phase in ('started', 'succeeded'):
                    records = [row for row in matched if row['event'] == phase]
                    if len(records) != 1:
                        raise ValueError(f'Missing or ambiguous workflow {phase}')
                    span = observed[phase]
                    if not span['start']['timestamp_ns'] <= records[0]['timestamp_ns'] <= span['end']['timestamp_ns']:
                        raise ValueError('Workflow timestamp outside matching observer span')
                previous = [row['timestamp_ns'] for row in measured_workflow if row['event'] == 'succeeded'
                            and row['process_id'] in child and row['timestamp_ns'] < event['execution_enter']['timestamp_ns']]
                if previous:
                    values['previous_success_to_execution_entry'] = event['execution_enter']['timestamp_ns'] - max(previous)
                else:
                    unavailable.append('previous_success_to_execution_entry')
            used_batches.add((batch['pid'], batch['batch_sequence'], batch['worker']))
            valid_samples.append({'source_id': identity[0], 'claim_token_sha256': identity[1],
                                  'parent_pid': next(iter(parent)), 'child_pid': next(iter(child)),
                                  'operation_id': operation, 'stage': stage,
                                  'unavailable_phases': unavailable})
        except (ValueError, KeyError, TypeError) as error:
            sample_errors.append(str(error))
        if sample_errors:
            invalid_samples.append({'source_id': identity[0], 'claim_token_sha256': identity[1], 'errors': sample_errors})
        else:
            for name, duration in values.items():
                metrics[name].append(duration)
    for key in used_batches:
        metrics['claim_batch'].append(batches[key]['duration_ns'])
    if not groups:
        errors.append('No sampled claim phases')
    stages: dict[str, int] = defaultdict(int)
    for sample in valid_samples:
        stages[sample['stage'] or 'unknown'] += 1
    return {'schema_version': 1, 'status': 'invalid' if errors or invalid_samples or not valid_samples else 'valid',
            'scope': 'Sampled diagnostic phase durations only; no performance goal or winner claim.',
            'clock_offset_tolerance_ns': CLOCK_TOLERANCE_NS, 'invalid_markers': markers,
            'errors': errors, 'sample_count': len(valid_samples), 'invalid_sample_count': len(invalid_samples),
            'excluded_warmup_samples': excluded, 'parsed_claim_batch_count': len(batches),
            'matched_claim_batch_count': len(used_batches),
            'parent_pids': sorted({sample['parent_pid'] for sample in valid_samples}),
            'child_pids': sorted({sample['child_pid'] for sample in valid_samples}),
            'stage_mix': dict(stages), 'phases': {name: _summary(values) for name, values in sorted(metrics.items())},
            'samples': valid_samples, 'invalid_samples': invalid_samples,
            'limitations': ['Submission-to-entry includes queueing, startup, IPC and scheduling.',
                            'Entry-to-handler includes source fetch and session setup.',
                            'Handler-exit-to-execution-exit includes completion transaction and observer.',
                            'Execution-exit-to-future-done includes serialization, IPC and result-thread scheduling.',
                            'Previous success on the same PID is a descriptive gap, not parent-refill causality.',
                            'Sampling and diagnostic writes can affect elapsed time; entry/exit timestamps precede probe append writes.',
                            'Claim-batch statistics count distinct batches linked to valid sampled tasks, not every recorded periodic batch.']}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--trace', type=Path, required=True)
    parser.add_argument('--workflow', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = analyze(args.trace, args.workflow)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + '\n')
    return 0 if result['status'] == 'valid' else 2


if __name__ == '__main__':
    raise SystemExit(main())
