"""Canonical argument fingerprints without publishing task payloads."""
from __future__ import annotations

import base64
import dataclasses
import datetime
import enum
import hashlib
import json
import math
import uuid
from collections.abc import Mapping
from pathlib import PurePath


def _qualified(value):
    return type(value).__module__ + '.' + type(value).__qualname__


def _canonical(value, active):
    if value is None or type(value) in (bool, int, str):
        return value
    if _qualified(value) == 'django.utils.safestring.SafeString':
        # Original Django template rendering preserves this safety marker in
        # pickled email payloads. Keep its exact class distinct from plain text;
        # accepting arbitrary string subclasses would hide application values.
        from django.utils.safestring import SafeString
        if type(value) is SafeString:
            return {'type': _qualified(value), 'value': str.__str__(value)}
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError('Task argument contains a nonfinite number')
        return {'float': value.hex()}
    if isinstance(value, bytes):
        return {'bytes': base64.b64encode(value).decode('ascii')}
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return {'type': _qualified(value), 'iso': value.isoformat()}
    if isinstance(value, (PurePath, uuid.UUID)):
        return {'type': _qualified(value), 'value': str(value)}
    if isinstance(value, enum.Enum):
        return {'type': _qualified(value), 'value': _canonical(value.value, active)}
    identity = id(value)
    if identity in active:
        raise ValueError('Task argument contains a cyclic object graph')
    active.add(identity)
    try:
        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            return {'type': _qualified(value), 'fields': {
                field.name: _canonical(getattr(value, field.name), active)
                for field in dataclasses.fields(value)}}
        if isinstance(value, Mapping):
            entries = [[_canonical(key, active), _canonical(item, active)]
                       for key, item in value.items()]
            entries.sort(key=lambda item: json.dumps(item[0], sort_keys=True))
            return {'mapping': entries}
        if isinstance(value, tuple) and hasattr(value, '_fields'):
            return {'type': _qualified(value), 'fields': {
                name: _canonical(getattr(value, name), active) for name in value._fields}}
        if isinstance(value, (tuple, list)):
            # JSON transport turns tuples into lists; this is the intended
            # sequence equivalence, not a change to their business values.
            return [_canonical(item, active) for item in value]
        if isinstance(value, (set, frozenset)):
            items = [_canonical(item, active) for item in value]
            return {'set': sorted(items, key=lambda item: json.dumps(item, sort_keys=True))}
        if type(value).__module__.startswith('django.core.mail.') and hasattr(value, '__dict__'):
            return {'type': _qualified(value), 'state': _canonical(vars(value), active)}
        raise ValueError('Unsupported task argument type: ' + _qualified(value))
    finally:
        active.remove(identity)


def argument_digest(args, kwargs):
    """Bind original publication values to worker input, without storing them."""
    if not isinstance(args, (tuple, list)) or not isinstance(kwargs, Mapping):
        raise ValueError('Task arguments must be a sequence and keyword mapping')
    payload = _canonical([list(args), kwargs], set())
    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=True,
                         allow_nan=False, separators=(',', ':')).encode()
    return hashlib.sha256(encoded).hexdigest()
