"""Replay safe observed SMTP content without importing the historical application."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from datetime import datetime
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path

HEADERS = ("From", "To", "Reply-To", "Subject", "X-Benchmark", "Message-Id", "Date")


def recipients(operation_id):
    stem = str(operation_id).replace(":", "-")
    return [f"recipient-{stem}-a@benchmark.invalid", f"recipient-{stem}-b@benchmark.invalid"]


class FixtureHTML(HTMLParser):
    """Observe actual paragraph text and inline declarations, including nested text."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.paragraphs = []
        self.active = None
        self.outside_text = ""

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag in ("script", "iframe", "object", "style"):
            raise ValueError("Unexpected active HTML content")
        if tag == "p" and attrs.get("class") == "fixture":
            if self.active is not None:
                raise ValueError("Nested fixture paragraph")
            self.active = {"text": "", "style": attrs.get("style", "")}
            self.paragraphs.append(self.active)

    def handle_endtag(self, tag):
        if tag == "p":
            self.active = None

    def handle_data(self, data):
        if self.active is not None:
            self.active["text"] += data
        else:
            self.outside_text += data


def observe_messages(messages):
    """Retain decoded received content and header occurrences, never pickle or config."""
    deliveries = []
    for sender, destinations, raw in messages:
        message = BytesParser(policy=policy.default).parsebytes(raw)
        deliveries.append({"sender": sender, "destinations": list(destinations),
            "headers": {key: [str(value) for value in message.get_all(key, [])] for key in HEADERS},
            "parts": [{"content_type": part.get_content_type(),
                       "content": part.get_content().rstrip("\r\n")}
                      for part in message.walk() if part.get_content_maintype() == "text"]})
    return {"schema_version": 1, "deliveries": deliveries}


def replay_messages(manifest, operations):
    """Compare independently observed outputs with the deterministic native fixture."""
    if not isinstance(manifest, dict) or set(manifest) != {"schema_version", "deliveries"}:
        raise ValueError("Invalid SMTP evidence manifest")
    if type(manifest["schema_version"]) is not int or manifest["schema_version"] != 1:
        raise ValueError("Unknown SMTP evidence version")
    deliveries = manifest["deliveries"]
    if not isinstance(deliveries, list):
        raise ValueError("Invalid SMTP deliveries")
    operations = set(operations)
    normalized, observed, message_ids = [], set(), []
    for row in deliveries:
        if not isinstance(row, dict) or set(row) != {"sender", "destinations", "headers", "parts"}:
            raise ValueError("Invalid SMTP receipt")
        headers = row["headers"]
        if not isinstance(headers, dict) or set(headers) != set(HEADERS):
            raise ValueError("Invalid SMTP headers")
        for key in HEADERS:
            values = headers[key]
            if not isinstance(values, list) or len(values) != 1 or not isinstance(values[0], str):
                raise ValueError(f"SMTP header absent or malformed: {key}")
        fields = {key: value[0] for key, value in headers.items()}
        operation_id, to = fields["X-Benchmark"], fields["To"]
        identity = (operation_id, to)
        if operation_id not in operations or to not in recipients(operation_id) or identity in observed:
            raise ValueError("SMTP output has unknown or duplicate delivery identities")
        if row["sender"] != "sender@benchmark.invalid" or row["destinations"] != [to] or fields["From"] != row["sender"]:
            raise ValueError("SMTP envelope and visible address headers differ")
        if fields["Subject"] != f"Historical Sentry fixture {operation_id}":
            raise ValueError("Native subject normalization differs")
        other = next(recipient for recipient in recipients(operation_id) if recipient != to)
        if fields["Reply-To"] != other:
            raise ValueError("Native multi-recipient reply header differs")
        parts = row["parts"]
        if (not isinstance(parts, list) or len(parts) != 2 or
                any(not isinstance(part, dict) or set(part) != {"content_type", "content"}
                    or not isinstance(part["content"], str) for part in parts)):
            raise ValueError("SMTP text MIME parts differ")
        content = {part["content_type"]: part["content"] for part in parts}
        if set(content) != {"text/plain", "text/html"}:
            raise ValueError("SMTP text MIME types differ")
        if content["text/plain"] != f"Plain body {operation_id} with unicode: café":
            raise ValueError("Native text template output differs")
        document = FixtureHTML()
        document.feed(content["text/html"])
        document.close()
        if document.outside_text.strip() or len(document.paragraphs) != 1 or document.paragraphs[0]["text"] != f"HTML body {operation_id}: café":
            raise ValueError("Native HTML template output differs")
        declarations = {}
        for declaration in document.paragraphs[0]["style"].split(";"):
            if ":" in declaration:
                key, value = declaration.split(":", 1)
                declarations[key.strip().lower()] = value.strip().lower()
        if declarations.get("color") != "red":
            raise ValueError("Native CSS was not inlined")
        msgid = fields["Message-Id"]
        if not re.fullmatch(r"<\d{14}\.\d+\.\d{1,5}@benchmark\.invalid>", msgid):
            raise ValueError("Native generated Message-ID header absent or malformed")
        try:
            datetime.strptime(msgid[1:15], "%Y%m%d%H%M%S")
        except ValueError as error:
            raise ValueError("Native generated Message-ID timestamp malformed") from error
        try:
            date = parsedate_to_datetime(fields["Date"])
        except (ValueError, TypeError, OverflowError) as error:
            raise ValueError("Native Date header malformed") from error
        if date is None or not re.search(r"(?:[+-]\d{4}|GMT|UTC)$", fields["Date"]):
            raise ValueError("Native Date header missing timezone")
        message_ids.append(msgid)
        observed.add(identity)
        normalized.append({"operation_id": operation_id, "from": row["sender"], "to": [to],
            "subject": fields["Subject"], "reply_to": other, "text": content["text/plain"], "html": content["text/html"]})
    expected = {(operation_id, recipient) for operation_id in operations for recipient in recipients(operation_id)}
    if observed != expected:
        raise ValueError("SMTP delivery contains missing distinct recipient messages")
    counts = Counter(message_ids)
    ids = {"distinct": len(counts), "collisions": sum(count - 1 for count in counts.values()),
           "duplicate_values": sorted(key for key, count in counts.items() if count > 1)}
    return sorted(normalized, key=lambda row: (row["operation_id"], row["to"])), ids


def business_digest(normalized):
    return hashlib.sha256(json.dumps(normalized, sort_keys=True).encode()).hexdigest()


def validate_smtp_evidence(row, directory, *, expected_requests=100):
    """Replay a caller-supplied trusted workload, independent of reported counts."""
    if type(expected_requests) is not int or expected_requests <= 0:
        raise ValueError("Invalid trusted SMTP request count")
    evidence = row.get("smtp_evidence")
    if (not isinstance(evidence, dict) or set(evidence) != {"path", "sha256", "schema_version"}
            or type(evidence["schema_version"]) is not int or evidence["schema_version"] != 1):
        raise ValueError("Missing versioned SMTP evidence")
    if not isinstance(evidence["path"], str) or not isinstance(evidence["sha256"], str):
        raise ValueError("Malformed SMTP evidence location")
    base = Path(directory).resolve()
    path = (base / evidence["path"]).resolve()
    if Path(evidence["path"]).is_absolute() or not path.is_relative_to(base) or path.name != "smtp-evidence.json":
        raise ValueError("SMTP evidence path escapes its report directory")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != evidence["sha256"]:
        raise ValueError("SMTP evidence SHA-256 differs")
    normalized, ids = replay_messages(json.loads(raw), [str(index) for index in range(expected_requests)])
    validation = row.get("validation", {})
    if validation.get("output_digest") != business_digest(normalized):
        raise ValueError("SMTP business digest differs")
    reported_ids = validation.get("generated_message_ids", {})
    if (not isinstance(reported_ids, dict)
            or any(type(reported_ids.get(key)) is not int for key in ("distinct", "collisions"))
            or any(reported_ids.get(key) != value for key, value in ids.items())):
        raise ValueError("SMTP generated-header diagnostics differ")
    if type(validation.get("messages")) is not int or validation["messages"] != expected_requests * 2:
        raise ValueError("SMTP message count differs")
    return {"schema_version": 1, "messages": expected_requests * 2, "output_digest": business_digest(normalized), **ids}
