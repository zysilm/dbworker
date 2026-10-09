"""Replay actual SQL Lab REST results against the independent warehouse fixture.

This establishes returned aggregate semantics, not physical scan telemetry.
The original worker and authenticated result retrieval remain inside timing.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from benchmarks.common.business_evidence import expected_operations
from benchmarks.common.timing_evidence import elapsed_seconds

SQL = 'SELECT category, SUM(value) + {marker} AS total FROM facts GROUP BY category ORDER BY category'


def digest(value):
    """Match the original benchmark's JSON encoding, including spaces."""
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def fixture_digest():
    return digest([[i % 10, i] for i in range(10000)])


def _require(condition, message):
    if not condition:
        raise ValueError('SQL result evidence: ' + message)


def validate_superset_output(row, directory, expected_requests, *, events=None):
    """Reconstruct measured per-query and aggregate fingerprints from observations."""
    evidence = row.get('sql_result_evidence')
    _require(isinstance(evidence, dict) and set(evidence) == {'path', 'sha256', 'schema_version'}
             and type(evidence['schema_version']) is int and evidence['schema_version'] == 1,
             'missing versioned receipt')
    _require(isinstance(evidence['path'], str), 'invalid receipt location')
    base = Path(directory).resolve()
    path = (base / evidence['path']).resolve()
    _require(not Path(evidence['path']).is_absolute() and path.is_relative_to(base)
             and path.name == 'sql-results-evidence.json', 'unsafe receipt location')
    raw = path.read_bytes()
    _require(hashlib.sha256(raw).hexdigest() == evidence['sha256'], 'receipt checksum differs')
    receipt = json.loads(raw)
    _require(isinstance(receipt, dict) and set(receipt) == {'schema_version', 'fixture', 'results'}
             and type(receipt['schema_version']) is int and receipt['schema_version'] == 1,
             'invalid receipt schema')
    _require(isinstance(receipt['fixture'], dict) and type(receipt['fixture'].get('rows')) is int
             and receipt['fixture'] == {'rows': 10000, 'ordered_input_sha256': fixture_digest()},
             'actual warehouse input differs from trusted fixture')
    operations, warmups = expected_operations('superset', expected_requests)
    _require(row.get('warmup_operations') == warmups, 'warmup identity differs')
    bindings = row.get('business_bindings')
    _require(isinstance(bindings, list) and len(bindings) == expected_requests
             and all(isinstance(b, dict) for b in bindings), 'missing query bindings')
    indexed = {b.get('operation_id'): b for b in bindings}
    _require(len(indexed) == expected_requests and set(indexed) == set(operations), 'query identities differ')
    observed = receipt['results']
    _require(isinstance(observed, list) and len(observed) == expected_requests, 'result count differs')
    window = row.get('measurement_window')
    seconds = elapsed_seconds(window)
    _require(row.get('metrics', {}).get('wall_seconds') == seconds, 'measured duration differs')
    start, end = window['start'], window['end']
    values = []
    for operation_id, item in zip(operations, observed):
        _require(isinstance(item, dict) and set(item) == {
            'operation_id', 'query_id', 'node_id', 'results_key', 'sql_sha256',
            'argument_sha256', 'retrieved_timestamp_ns', 'http_status', 'result'}, 'invalid query receipt')
        _require(item['operation_id'] == operation_id, 'result ordering or identity differs')
        binding = indexed[operation_id]
        for key in ('query_id', 'node_id', 'results_key', 'sql_sha256', 'argument_sha256'):
            _require(item[key] == binding.get(key), 'query binding differs: ' + key)
        _require(type(item['query_id']) is int and item['query_id'] > 0, 'invalid query ID')
        sql = SQL.format(marker=int(operation_id.split('-')[1]))
        _require(item['sql_sha256'] == hashlib.sha256(sql.encode()).hexdigest(), 'SQL input differs')
        timestamp = item['retrieved_timestamp_ns']
        _require(type(timestamp) is int and start['timestamp_ns'] - start['uncertainty_ns'] <= timestamp
                 <= end['timestamp_ns'] + end['uncertainty_ns'], 'result retrieved outside measured interval')
        if events is not None:
            started = [event for event in events if event.get('operation_id') == operation_id
                       and event.get('event') == 'started' and event.get('node_id') == item['node_id']]
            _require(len(started) == 1 and type(started[0].get('timestamp_ns')) is int
                     and started[0]['timestamp_ns'] <= timestamp, 'retrieval precedes original query execution')
        _require(type(item['http_status']) is int and item['http_status'] == 200, 'result API failed')
        value = item['result']
        _require(isinstance(value, dict) and set(value) == {'data', 'columns', 'status'}, 'invalid result payload')
        expected = [{'category': k, 'total': sum(range(k, 10000, 10)) + int(operation_id.split('-')[1])}
                    for k in range(10)]
        data = value['data']
        _require(isinstance(data, list) and len(data) == 10
                 and all(isinstance(entry, dict) and set(entry) == {'category', 'total'}
                         and type(entry['category']) is int and type(entry['total']) is int for entry in data)
                 and data == expected and value['status'] == 'success', 'aggregate output differs from independent oracle')
        columns = value['columns']
        _require(isinstance(columns, list) and len(columns) == 2, 'column count differs')
        for name, column in zip(('category', 'total'), columns):
            _require(isinstance(column, dict) and column.get('name') == name
                     and column.get('column_name') == name and column.get('is_dttm') is False
                     and isinstance(column.get('type'), str) and column['type'].upper() in ('INTEGER', 'INT', 'BIGINT')
                     and type(column.get('type_generic')) is int and column['type_generic'] == 0,
                     'native integer column schema differs')
        _require(binding.get('output_sha256') == digest(value), 'per-query output digest differs')
        values.append(value)
    validation = row.get('validation', {})
    for key, count in (('queries', expected_requests), ('retrieved_results', expected_requests), ('rows_per_query', 10)):
        _require(type(validation.get(key)) is int and validation[key] == count, 'result counts differ')
    _require(validation.get('output_digest') == digest(values), 'aggregate output digest differs')
    return {'queries': expected_requests, 'rows_per_query': 10, 'output_digest': digest(values)}
