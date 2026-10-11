"""Optional sampled coordinator handoff diagnostics, without additional SQL.

Installation is limited to a benchmark runtime instance. Spawn children dispatch
through an importable module function; the production execution function and
transaction boundary remain unchanged. O_APPEND writes are bounded by sampled
batch metadata and do not acquire application locks.
"""
from __future__ import annotations

import functools
import hashlib
import json
import os
import threading
import sys
import time
from pathlib import Path

ENVIRONMENT = 'DBWORKER_HANDOFF_TRACE_PATH'
SAMPLE_PERIOD = 32


def sampled(claim):
    return type(claim.source_id) is int and claim.source_id % SAMPLE_PERIOD == 0


def _clock():
    return {'timestamp_ns': time.time_ns(), 'monotonic_ns': time.monotonic_ns()}


def _write(path, event, **fields):
    row = {'event': event, 'pid': os.getpid(), 'thread_id': threading.get_ident(),
           **_clock(), **fields}
    payload = (json.dumps(row, sort_keys=True) + '\n').encode()
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        if os.write(descriptor, payload) != len(payload):
            raise OSError('Incomplete handoff diagnostic append')
    finally:
        os.close(descriptor)


def _emit(path, event, **fields):
    """Diagnostic failures invalidate evidence, never business execution."""
    try:
        _write(path, event, **fields)
        return True
    except Exception as error:
        try:
            marker = f'{path}.invalid-{os.getpid()}'
            descriptor = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                payload = (type(error).__name__ + '\n').encode()
                if os.write(descriptor, payload) != len(payload):
                    raise OSError('Incomplete diagnostic invalid marker')
            finally:
                os.close(descriptor)
        except Exception:
            print('DBWORKER_HANDOFF_DIAGNOSTIC_INVALID', file=sys.stderr, flush=True)
        return False


def _identity(claim):
    return {'source_id': claim.source_id, 'claim_token_sha256': hashlib.sha256(str(claim.token).encode()).hexdigest()}


def execute_in_process(claim):
    """Pickleable spawn entrypoint delegating to the original execution body."""
    import dbworker

    path = os.environ.get(ENVIRONMENT)
    if not path or not sampled(claim):
        return dbworker._execute_in_process(claim)
    worker = dbworker._process_worker
    if worker is None:
        return dbworker._execute_in_process(claim)
    identity = {**_identity(claim), 'worker': worker.name}
    original_handler = worker.handler
    record_globals = (getattr(original_handler, '__globals__', {})
                      if getattr(original_handler, '__module__', '') == 'examples.posthog_dbworker.runtime' else {})
    original_record = record_globals.get('record')
    observer_spans = []
    if original_record is not None:
        def observed_record(*args, **kwargs):
            start = _clock()
            try:
                return original_record(*args, **kwargs)
            finally:
                finish = _clock()
                observer_spans.append({'stage': args[0] if args else None,
                    'phase': args[3] if len(args) > 3 else None,
                    'start': start, 'end': finish,
                    'duration_ns': finish['monotonic_ns'] - start['monotonic_ns']})
    else:
        observed_record = None

    @functools.wraps(original_handler)
    def observed_handler(job, session):
        fields = {**identity, 'operation_id': getattr(job, 'operation_id', None),
                  'stage': getattr(job, 'stage', None)}
        _emit(path, 'handler_enter', **fields)
        try:
            outcome = original_handler(job, session)
        except BaseException as business_error:
            try:
                _emit(path, 'handler_exit', status='error', **fields)
            except BaseException:
                business_error.add_note('Handoff diagnostic write failed; diagnostic trace invalid')
            raise
        else:
            _emit(path, 'handler_exit', status='returned', **fields)
            return outcome

    _emit(path, 'execution_enter', **identity)
    worker.handler = observed_handler
    if observed_record is not None:
        record_globals['record'] = observed_record
    try:
        outcome = dbworker._execute_in_process(claim)
    except BaseException as business_error:
        try:
            _emit(path, 'execution_exit', status='error', observer_spans=observer_spans, **identity)
        except BaseException:
            business_error.add_note('Handoff diagnostic write failed; diagnostic trace invalid')
        raise
    else:
        _emit(path, 'execution_exit', status='returned', observer_spans=observer_spans, **identity)
        return outcome
    finally:
        worker.handler = original_handler
        if observed_record is not None:
            record_globals['record'] = original_record


class _ObservedExecutor:
    def __init__(self, executor, path, worker, failures):
        self.executor, self.path, self.worker, self.failures = executor, path, worker, failures

    def __getattr__(self, name):
        return getattr(self.executor, name)

    def submit(self, function, claim):
        import dbworker

        if function is not dbworker._execute_in_process:
            raise RuntimeError('Unexpected coordinator process dispatch function')
        observe = sampled(claim)
        identity = {**_identity(claim), 'worker': self.worker.name}
        if observe:
            _emit(self.path, 'submit_begin', **identity)
        future = self.executor.submit(execute_in_process, claim)
        if observe:
            _emit(self.path, 'submit_end', **identity)

            def completed(done):
                try:
                    _emit(self.path, 'future_done', cancelled=done.cancelled(), **identity)
                except BaseException as error:
                    self.failures.append(error)
            future.add_done_callback(completed)
        return future


def install(runtime):
    """Return False with no environment setting; otherwise install once."""
    path = os.environ.get(ENVIRONMENT)
    if not path:
        return False
    if getattr(runtime, '_benchmark_handoff_installed', False):
        return True
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        _emit(path, 'diagnostic_initialization_failed')
    original_claim, original_run = runtime._claim_many, runtime._run
    batches = 0
    failures = []

    @functools.wraps(original_claim)
    def claim_many(worker, limit):
        nonlocal batches
        batches += 1
        start = _clock()
        claims = original_claim(worker, limit)
        finish = _clock()
        if batches % SAMPLE_PERIOD == 0 or any(sampled(claim) for claim in claims):
            _emit(path, 'claim_batch', worker=worker.name, batch_sequence=batches,
                   limit=limit, claims=[_identity(claim) for claim in claims],
                   start=start, end=finish,
                   duration_ns=finish['monotonic_ns'] - start['monotonic_ns'])
        return claims

    @functools.wraps(original_run)
    def run(worker, handlers, wakeup):
        result = original_run(worker, _ObservedExecutor(handlers, path, worker, failures), wakeup)
        if failures:
            raise RuntimeError('Handoff diagnostic callback failed') from failures[0]
        return result

    runtime._claim_many, runtime._run = claim_many, run
    runtime._benchmark_handoff_installed = True
    return True
