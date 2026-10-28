"""Internal page_change_event.v1 messages built from the captured upstream fixture.

The ingest service owns the real upstream-to-internal mapping. This helper mirrors only the fields the
stream processor reads (docs/contracts/events/page_change_event.v1.json) so these tests do not depend on
ingest code. editor_key here is an unkeyed SHA-256 prefix: it only needs to be stable per editor.
"""

import hashlib
import json
from pathlib import Path

import pytest

FIXTURE = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "page_change" / "events.jsonl"


def editor_type(performer: dict) -> str:
    if performer.get("is_bot"):
        return "bot"
    if performer.get("is_system"):
        return "system"
    if performer.get("is_temp"):
        return "temp"
    return "registered"


def to_internal(sse_id: str, event: dict) -> dict:
    performer = event.get("performer") or {}
    revision = event.get("revision") or {}
    revert = revision.get("revert")
    user = performer.get("user_id") or performer.get("user_text")
    return {
        "event_id": event["meta"]["id"],
        "event_dt": event["dt"],
        "ingested_at": event["meta"]["dt"],
        "upstream_schema": event["$schema"],
        "sse_position": sse_id,
        "wiki_id": event["wiki_id"],
        "change_kind": event["page_change_kind"],
        "page_id": event["page"]["page_id"],
        "page_title": event["page"]["page_title"],
        "namespace_id": event["page"]["namespace_id"],
        "is_redirect": event["page"]["is_redirect"],
        "rev_id": revision.get("rev_id"),
        "revert": {"method": revert["method"]} if revert else None,
        "editor_type": editor_type(performer),
        "editor_key": hashlib.sha256(f"{event['wiki_id']}:{user}".encode()).hexdigest()[:32]
        if user
        else None,
    }


@pytest.fixture(scope="session")
def internal_events() -> list[dict]:
    events = []
    with FIXTURE.open() as lines:
        for line in lines:
            record = json.loads(line)
            if record["event"]["meta"]["domain"] == "canary":
                continue
            events.append(to_internal(record["sse_id"], record["event"]))
    return events
