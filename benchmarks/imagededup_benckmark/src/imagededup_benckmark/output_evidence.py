"""Replay complete image hashes and public top-K output receipts offline."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

TOP_K = 10
MAX_DISTANCE = 10


def replay(manifest, trace, images, scenario):
    """Compute normalized outputs from actual values and independently check top-K."""
    if not isinstance(manifest, dict) or set(manifest) != {
            'schema_version', 'workspace', 'top_k', 'max_distance', 'artifacts', 'comparisons'}:
        raise ValueError('Invalid image output manifest')
    for name, expected in (('schema_version', 1), ('workspace', trace['workspace']),
                           ('top_k', TOP_K), ('max_distance', MAX_DISTANCE)):
        if type(manifest.get(name)) is not int or manifest[name] != expected:
            raise ValueError(f'Image output setting differs from trusted workload: {name}')
    artifacts = manifest['artifacts']
    if not isinstance(artifacts, list) or len(artifacts) != images:
        raise ValueError('Image output hash coverage differs')
    artifact_ids, hashes = [], []
    for item in artifacts:
        if not isinstance(item, dict) or set(item) != {'artifact_id', 'hash_value'}:
            raise ValueError('Invalid image hash receipt')
        identity, value = item['artifact_id'], item['hash_value']
        if type(identity) is not int or identity <= 0 or not isinstance(value, str) or not re.fullmatch('[0-9a-f]{16}', value):
            raise ValueError('Invalid image hash identity or value')
        artifact_ids.append(identity)
        hashes.append(value)
    if len(set(artifact_ids)) != images or artifact_ids != trace['artifact_ids']:
        raise ValueError('Image output hashes differ from submitted source order')
    comparisons = manifest['comparisons']
    expected_requests = 0 if scenario == 'build' else images
    if not isinstance(comparisons, list) or len(comparisons) != expected_requests:
        raise ValueError('Image output comparison coverage differs')
    request_ids, digests = [], []
    ordinal = {identity: index for index, identity in enumerate(artifact_ids)}
    for index, item in enumerate(comparisons):
        if not isinstance(item, dict) or set(item) != {'request_id', 'query_artifact_id', 'results'}:
            raise ValueError('Invalid image comparison output receipt')
        if (type(item['request_id']) is not int or item['request_id'] <= 0
                or type(item['query_artifact_id']) is not int
                or item['query_artifact_id'] != artifact_ids[index]):
            raise ValueError('Image query output association differs')
        request_ids.append(item['request_id'])
        results = item['results']
        if not isinstance(results, list) or len(results) > TOP_K:
            raise ValueError('Invalid image top-K result size')
        actual = []
        for result in results:
            if not isinstance(result, dict) or set(result) != {'candidate_artifact_id', 'distance'}:
                raise ValueError('Invalid image top-K row')
            if (type(result['candidate_artifact_id']) is not int or result['candidate_artifact_id'] not in ordinal
                    or type(result['distance']) is not int or not 0 <= result['distance'] <= 64):
                raise ValueError('Invalid image top-K identity or distance')
            actual.append((result['candidate_artifact_id'], result['distance']))
        query = int(hashes[index], 16)
        expected = sorted(((identity, (query ^ int(value, 16)).bit_count())
                           for identity, value in zip(artifact_ids, hashes, strict=True)
                           if identity != item['query_artifact_id']), key=lambda pair: (pair[1], pair[0]))
        expected = [pair for pair in expected if pair[1] <= MAX_DISTANCE][:TOP_K]
        if actual != expected:
            raise ValueError('Observed image top-K differs from complete Hamming oracle')
        normalized = [(ordinal[identity], distance) for identity, distance in actual]
        digests.append(hashlib.sha256(json.dumps(normalized).encode()).hexdigest())
    if len(set(request_ids)) != expected_requests or request_ids != trace['request_ids']:
        raise ValueError('Image output requests differ from original submission identities')
    return {'passed': True, 'hashes_digest': hashlib.sha256(json.dumps(hashes).encode()).hexdigest(),
            'top_k_digests': digests, 'artifacts': images, 'requests': expected_requests,
            'scored_pairs': expected_requests * (images - 1)}


def validate(row, directory, images):
    """Verify safe receipt integrity and every reported output validation field."""
    metadata = row.get('output_evidence')
    if not isinstance(metadata, dict) or set(metadata) != {'path', 'sha256', 'schema_version'}:
        raise ValueError('Missing image output evidence')
    if type(metadata['schema_version']) is not int or metadata['schema_version'] != 1:
        raise ValueError('Invalid image output evidence version')
    relative = metadata['path']
    if (not isinstance(relative, str) or not relative or Path(relative).is_absolute()
            or '..' in Path(relative).parts or Path(relative).name != 'output-evidence.json'):
        raise ValueError('Unsafe image output evidence path')
    root = Path(directory).resolve()
    path = root
    for part in Path(relative).parts:
        path /= part
        if path.is_symlink():
            raise ValueError('Symlink image output evidence is not permitted')
    if not path.resolve().is_relative_to(root) or not path.is_file():
        raise ValueError('Missing image output evidence')
    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != metadata['sha256']:
        raise ValueError('Image output evidence checksum mismatch')
    result = replay(json.loads(payload), row['operation_trace'], images, row['scenario'])
    wrapped = {**result, 'output_digest': hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()}
    if row.get('validation') != wrapped:
        raise ValueError('Reported image output differs from replayed actual values')
    return wrapped
