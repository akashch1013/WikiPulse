import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from wikipulse_ingest.process import Accepted, Canary, DeadLetter, EventProcessor
from wikipulse_ingest.schemas import UpstreamSchemas
from wikipulse_ingest.transform import editor_key, strip_namespace

CONTRACTS = Path(__file__).resolve().parents[3] / "docs" / "contracts" / "events"
EVENT_CONTRACT = Draft202012Validator(
    json.loads((CONTRACTS / "page_change_event.v1.json").read_text()), format_checker=FormatChecker()
)
DLQ_CONTRACT = Draft202012Validator(
    json.loads((CONTRACTS / "dlq_event.v1.json").read_text()), format_checker=FormatChecker()
)
RECEIVED = datetime(2026, 10, 28, 3, 30, tzinfo=UTC)


@pytest.fixture(scope="module")
def processor(hmac_key) -> EventProcessor:
    return EventProcessor(UpstreamSchemas(), hmac_key, max_payload_bytes=900_000)


@pytest.fixture(scope="module")
def outcomes(processor, fixture_lines):
    return [processor.process(json.dumps(line["event"]), line["sse_id"], RECEIVED) for line in fixture_lines]


def test_every_fixture_event_maps_to_the_contract(outcomes):
    accepted = [o for o in outcomes if isinstance(o, Accepted)]
    assert len(accepted) == 1998
    for outcome in accepted:
        errors = [e.message for e in EVENT_CONTRACT.iter_errors(outcome.value)]
        assert not errors, errors
        assert outcome.key == f"{outcome.value['wiki_id']}:{outcome.value['page_id']}"
        datetime.fromisoformat(outcome.value["event_dt"])


def test_canary_events_are_dropped(outcomes, fixture_lines):
    canaries = [line for line in fixture_lines if line["event"]["meta"]["domain"] == "canary"]
    assert len(canaries) == 2
    assert sum(isinstance(o, Canary) for o in outcomes) == 2


def test_no_identity_or_comments_survive(outcomes, fixture_lines):
    for line, outcome in zip(fixture_lines, outcomes, strict=True):
        if not isinstance(outcome, Accepted):
            continue
        performer = line["event"]["performer"]
        identity_values = {
            performer.get("user_text"),
            performer.get("user_id"),
            performer.get("user_central_id"),
        }
        non_title = {k: v for k, v in outcome.value.items() if k not in ("page_title", "prior_page_title")}
        scalars = {v for v in non_title.values() if not isinstance(v, dict)}
        assert not identity_values & scalars
        serialized = json.dumps(non_title)
        assert "fixture comment" not in serialized
        assert performer["user_text"] not in serialized


def test_editor_type_distribution(outcomes):
    types = Counter(o.value["editor_type"] for o in outcomes if isinstance(o, Accepted))
    assert types == {"registered": 1782, "temp": 127, "bot": 89}


def test_editor_key_is_per_wiki_and_stable(hmac_key):
    performer = {"user_id": 42, "user_text": "Example"}
    en = editor_key(hmac_key, "enwiki", performer)
    assert en == editor_key(hmac_key, "enwiki", {"user_id": 42, "user_text": "Renamed"})
    assert en != editor_key(hmac_key, "dewiki", performer)
    assert len(en) == 32
    assert editor_key(hmac_key, "enwiki", {"is_system": True, "user_text": "Maintenance script"}) is None


def test_namespace_prefix_is_removed_only_outside_main_namespace():
    assert strip_namespace("User:FixtureUser1/sandbox", 2) == "FixtureUser1/sandbox"
    assert strip_namespace("Star_Wars:_Episode_IV", 0) == "Star_Wars:_Episode_IV"


def test_moves_carry_prior_title(outcomes):
    moves = [o.value for o in outcomes if isinstance(o, Accepted) and o.value["change_kind"] == "move"]
    assert len(moves) == 26
    assert all(m["prior_page_title"] is not None and m["prior_namespace_id"] is not None for m in moves)
    assert all(
        (m["prior_page_title"], m["prior_namespace_id"]) != (m["page_title"], m["namespace_id"])
        for m in moves
    )


def _first_accepted_event(fixture_lines) -> dict:
    return next(
        line["event"]
        for line in fixture_lines
        if line["event"]["meta"]["domain"] != "canary" and line["event"]["page"]["namespace_id"] == 0
    )


def test_unknown_minor_validates_against_newest_known(processor, fixture_lines):
    event = dict(_first_accepted_event(fixture_lines), **{"$schema": "/mediawiki/page/change/1.13.0"})
    outcome = processor.process(json.dumps(event), "[]", RECEIVED)
    assert isinstance(outcome, Accepted)
    assert outcome.schema_drift
    assert outcome.value["upstream_schema"] == "/mediawiki/page/change/1.13.0"


def _dead(processor, payload: str) -> dict:
    outcome = processor.process(payload, '[{"topic":"t","partition":0,"timestamp":1}]', RECEIVED)
    assert isinstance(outcome, DeadLetter)
    errors = [e.message for e in DLQ_CONTRACT.iter_errors(outcome.value)]
    assert not errors, errors
    return outcome.value


def test_dead_letter_reasons(processor, fixture_lines):
    base = _first_accepted_event(fixture_lines)
    assert (
        _dead(processor, json.dumps(dict(base, **{"$schema": "/mediawiki/page/change/2.0.0"})))["reason_code"]
        == "unknown_schema_major"
    )
    assert _dead(processor, '{"meta": ')["reason_code"] == "json_decode_error"

    no_page_id = json.loads(json.dumps(base))
    del no_page_id["page"]["page_id"]
    assert _dead(processor, json.dumps(no_page_id))["reason_code"] == "missing_page_id"

    bad_namespace = json.loads(json.dumps(base))
    bad_namespace["page"]["namespace_id"] = "zero"
    dead = _dead(processor, json.dumps(bad_namespace))
    assert dead["reason_code"] == "schema_invalid"
    assert dead["reason_detail"].startswith("/page/namespace_id")
    assert "FixtureUser" not in dead["payload"]
    assert "fixture comment" not in dead["payload"]


def test_oversize_payload_is_dead_lettered_scrubbed(fixture_lines, hmac_key):
    small = EventProcessor(UpstreamSchemas(), hmac_key, max_payload_bytes=500)
    dead = _dead(small, json.dumps(_first_accepted_event(fixture_lines)))
    assert dead["reason_code"] == "oversize"
    assert "performer" not in json.loads(dead["payload"])
