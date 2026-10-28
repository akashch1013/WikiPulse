import hashlib
import json
import re
from pathlib import Path

from jsonschema import Draft7Validator

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "page_change"
UPSTREAM = ROOT / "services" / "ingest" / "src" / "wikipulse_ingest" / "schemas" / "upstream"

PLACEHOLDER_USER_TEXT = re.compile(r"^(FixtureUser\d+|~\d{4}-\d{5}-\d{2}|192\.0\.2\.\d{1,3})$")
PLACEHOLDER_COMMENT = re.compile(r"^fixture comment \d+$")


def _records():
    with (FIXTURES / "events.jsonl").open(encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def _validator(schema_uri: str) -> Draft7Validator:
    # "$schema" in events is "/mediawiki/page/change/1.12.0"; vendored files mirror that path.
    path = UPSTREAM / f"{schema_uri.lstrip('/')}.json"
    return Draft7Validator(json.loads(path.read_text(encoding="utf-8")))


def _revisions(event):
    for revision in (event.get("revision"), (event.get("prior_state") or {}).get("revision")):
        if isinstance(revision, dict):
            yield revision


def _identity_holders(event):
    yield event.get("performer")
    for revision in _revisions(event):
        yield revision.get("editor")


def test_fixtures_checksum_matches_source():
    source = json.loads((FIXTURES / "SOURCE.json").read_text(encoding="utf-8"))
    digest = hashlib.sha256((FIXTURES / "events.jsonl").read_bytes()).hexdigest()
    assert digest == source["sha256"]
    assert len(_records()) == source["event_count"]


def test_fixtures_validate_against_vendored_upstream_schema():
    records = _records()
    validators = {uri: _validator(uri) for uri in {r["event"]["$schema"] for r in records}}
    failures = []
    for i, record in enumerate(records):
        event = record["event"]
        errors = list(validators[event["$schema"]].iter_errors(event))
        if errors:
            failures.append((i, event["meta"]["id"], errors[0].message))
    assert not failures, failures[:5]


def test_fixtures_sse_ids_are_resume_positions():
    for record in _records():
        position = json.loads(record["sse_id"])
        assert isinstance(position, list) and position
        for entry in position:
            assert isinstance(entry["topic"], str) and isinstance(entry["partition"], int)
            assert ("timestamp" in entry) != ("offset" in entry)


def test_fixtures_identity_fields_are_placeholders():
    for record in _records():
        event = record["event"]
        for holder in _identity_holders(event):
            if isinstance(holder, dict) and "user_text" in holder:
                assert PLACEHOLDER_USER_TEXT.match(holder["user_text"]), holder["user_text"]
        comments = [event.get("comment")]
        comments += [revision.get("comment") for revision in _revisions(event)]
        for comment in comments:
            if comment is not None:
                assert PLACEHOLDER_COMMENT.match(comment), comment


def test_fixtures_user_namespace_titles_are_placeholders():
    user_name = re.compile(r"^[^:]+:(FixtureUser\d+|~\d{4}-\d{5}-\d{2}|192\.0\.2\.\d{1,3})(/.*)?$")
    checked = 0
    for record in _records():
        event = record["event"]
        page = event.get("page") or {}
        candidates = [page, page.get("redirect_page_link"), (event.get("prior_state") or {}).get("page")]
        candidates.append(event.get("created_redirect_page"))
        for obj in candidates:
            if isinstance(obj, dict) and obj.get("namespace_id") in (2, 3) and "page_title" in obj:
                assert user_name.match(obj["page_title"]), obj["page_title"]
                checked += 1
    assert checked > 0
