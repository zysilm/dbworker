"""Extract replayable business semantics from actual task arguments."""
from __future__ import annotations

import hashlib
from pathlib import Path


def _argument(args, kwargs, index, name):
    if index < len(args):
        if name in kwargs:
            raise ValueError('Duplicate task business argument: ' + name)
        return args[index]
    if name not in kwargs:
        raise ValueError('Missing task business argument: ' + name)
    return kwargs[name]


def business_input(task_name, args, kwargs):
    """Read publication/execution inputs, never a benchmark's expected values.

    Upload bytes are inspected before the original consumer moves or removes
    its scratch input. No file contents or SQL text are persisted in evidence.
    """
    if task_name == 'sql_lab.get_sql_results':
        query_id = _argument(args, kwargs, 0, 'query_id')
        sql = _argument(args, kwargs, 1, 'rendered_query')
        if type(query_id) is not int or query_id <= 0 or not isinstance(sql, str):
            raise ValueError('Invalid original SQL Lab business arguments')
        return {'query_id': query_id, 'sql_sha256': hashlib.sha256(sql.encode()).hexdigest()}
    if task_name == 'documents.tasks.consume_file':
        document = _argument(args, kwargs, 0, 'input_doc')
        # The real ConsumableDocument exposes the original scratch-file Path.
        # Reading that actual path binds bytes, independently of fixture labels.
        path = Path(document.original_file)
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(block)
        return {'input_sha256': digest.hexdigest()}
    return {}
