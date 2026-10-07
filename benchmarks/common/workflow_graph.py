"""Fail-closed admission of observed business jobs and their parent/child graph."""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path


class WorkflowMismatch(ValueError):
    pass


def read_trace(path):
    import fcntl
    with Path(path).open('r', encoding='utf-8') as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_SH)
        data = stream.read()
    if data and not data.endswith("\n"):
        raise WorkflowMismatch("Incomplete trace write")
    records = [json.loads(line) for line in data.splitlines() if line]
    if any(not isinstance(record, dict) for record in records):
        raise WorkflowMismatch('Workflow trace records must be objects')
    return records


def validate_graph(events, expected_operations, expected_stages, expected_edges=()):
    """Validate a successful-work contract using actual observed task identities.

    expected_stages maps each stage to its per-operation job count. Expected
    edges contains (parent_stage, child_stage) pairs, with repetitions allowed.
    Warmup operations must be excluded explicitly by operation identity. Events
    without operation identity are never silently ignored.
    """
    operations = list(expected_operations)
    if any(not isinstance(value, str) or not value or value == 'None' for value in operations):
        raise WorkflowMismatch('Operation identities must be nonempty strings')
    if len(set(operations)) != len(operations) or not operations:
        raise WorkflowMismatch("Expected operation identities must be nonempty and unique")
    if not expected_stages or any(not isinstance(v, int) or isinstance(v, bool) or v < 1 for v in expected_stages.values()):
        raise WorkflowMismatch("Expected stages require positive job counts")
    jobs = defaultdict(list)
    warmup = []
    backends = set()
    for row in events:
        if not isinstance(row, dict) or type(row.get("schema_version")) is not int or row.get("schema_version") != 1:
            raise WorkflowMismatch("Invalid trace schema")
        backend = row.get('backend')
        if backend not in ('celery', 'dbworker'):
            raise WorkflowMismatch('Missing or invalid trace backend')
        backends.add(backend)
        if len(backends) != 1:
            raise WorkflowMismatch('Mixed backends in one workflow trace')
        op = row.get("operation_id")
        if not isinstance(op, str) or not op or op == 'None':
            raise WorkflowMismatch("Uncorrelated task in native workflow trace")
        if op not in operations:
            if op.startswith("warmup:"):
                warmup.append(row)
                continue
            raise WorkflowMismatch(f"Unexpected operation: {op}")
        if row.get("stage") not in expected_stages:
            raise WorkflowMismatch(f"Unexpected business task stage: {row.get('stage')}")
        node = row.get("node_id")
        if not isinstance(node, str) or not node or node == "None":
            raise WorkflowMismatch("Missing task identity")
        parent = row.get('parent_id')
        if parent is not None and (not isinstance(parent, str) or not parent or parent == 'None'):
            raise WorkflowMismatch('Invalid parent identity')
        jobs[node].append(row)
    if warmup:
        warmup_ops = sorted({row['operation_id'] for row in warmup})
        validate_graph(warmup, warmup_ops, expected_stages, expected_edges)
        if set(jobs) & {row.get('node_id') for row in warmup}:
            raise WorkflowMismatch('Warmup and measured jobs reuse a task identity')
    normalized = {}
    counts = Counter()
    edges = Counter()
    per_operation = {}
    for node, rows in jobs.items():
        identities = {(r['operation_id'], r['stage'], r.get('parent_id')) for r in rows}
        if len(identities) != 1:
            raise WorkflowMismatch(f"Task identity changed: {node}")
        op, stage, parent = next(iter(identities))
        phases = Counter(r['event'] for r in rows)
        if phases != Counter({'submitted': 1, 'started': 1, 'succeeded': 1}):
            raise WorkflowMismatch(f"Missing, duplicate or failed task attempts: {node}: {dict(phases)}")
        normalized[node] = {'operation_id': op, 'stage': stage, 'parent_id': parent}
        counts[stage] += 1
    expected_edge_counts = Counter(tuple(edge) for edge in expected_edges)
    for op in operations:
        selected = {node: row for node, row in normalized.items() if row['operation_id'] == op}
        stages = Counter(row['stage'] for row in selected.values())
        if stages != Counter(expected_stages):
            raise WorkflowMismatch(f"Business job granularity differs for {op}: {dict(stages)}")
        operation_edges = Counter()
        for node, row in selected.items():
            parent = row['parent_id']
            if parent is None:
                continue
            if parent not in selected:
                raise WorkflowMismatch(f"Missing or cross-operation parent: {node}")
            operation_edges[(selected[parent]['stage'], row['stage'])] += 1
            visited = {node}
            cursor = parent
            while cursor is not None:
                if cursor in visited:
                    raise WorkflowMismatch("Cyclic business task graph")
                visited.add(cursor)
                cursor = selected[cursor]['parent_id']
        if operation_edges != expected_edge_counts:
            raise WorkflowMismatch(f"Business task edges differ for {op}: {dict(operation_edges)}")
        edges.update(operation_edges)
        per_operation[op] = {'stages': dict(sorted(stages.items())),
                             'edges': [[a, b, n] for (a, b), n in sorted(operation_edges.items())]}
    return {'schema_version': 1, 'passed': True, 'contract': 'successful_business_workflow',
            'operations': operations, 'nodes': len(normalized),
            'stage_counts': dict(sorted(counts.items())),
            'edge_counts': [[a, b, n] for (a, b), n in sorted(edges.items())],
            'per_operation': per_operation,
            'observed_jobs': [{'node_id': node, **row} for node, row in sorted(normalized.items())]}


def compare_graphs(left, right):
    for name in ('schema_version', 'contract', 'operations', 'nodes', 'stage_counts', 'edge_counts', 'per_operation'):
        if left.get(name) != right.get(name):
            raise WorkflowMismatch(f"Celery/DBWorker workflow mismatch: {name}")
    if left.get('passed') is not True or right.get('passed') is not True:
        raise WorkflowMismatch("Both observed workflows must pass admission")
    return {'passed': True, 'operations': len(left['operations']), 'nodes_per_backend': left['nodes'],
            'stage_counts': left['stage_counts'], 'edge_counts': left['edge_counts']}
