import gzip
import json
import os
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
import yaml
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012

from wikipulse_common.config import load_settings

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests" / "fixtures"
SPEC_URI = "urn:wikipulse:openapi"
SPEC = yaml.safe_load((ROOT / "docs" / "contracts" / "openapi.yaml").read_text())
REGISTRY = Registry().with_resource(SPEC_URI, Resource.from_contents(SPEC, default_specification=DRAFT202012))

# tests/fixtures/pageviews/SOURCE.json: the file named ...-120000 covers 11:00-12:00 UTC (end-of-hour naming).
PAGEVIEW_FIXTURE = FIXTURES / "pageviews" / "pageviews-20261026-120000.slice.gz"
PAGEVIEW_HOUR = datetime(2026, 10, 26, 11, tzinfo=UTC)
XML_FIXTURE = FIXTURES / "xml" / "dewiki-20261001-stub-meta-current1.head400.xml.gz"
SNAPSHOT_TIME = datetime(2026, 10, 1, tzinfo=UTC)


def _pointer(*parts: str) -> str:
    return "/".join(part.replace("~", "~0").replace("/", "~1") for part in parts)


def assert_matches_spec(path: str, status: int, response) -> None:
    content_type = "application/json" if status == 200 else "application/problem+json"
    assert response.headers["content-type"].startswith(content_type), response.headers["content-type"]
    responses = SPEC["paths"][path]["get"]["responses"]
    assert str(status) in responses, f"{status} is not a documented response of {path}"
    entry = responses[str(status)]
    if "$ref" in entry:
        schema_ref = f"{SPEC_URI}{entry['$ref']}/content/{_pointer(content_type)}/schema"
    else:
        location = _pointer("paths", path, "get", "responses", str(status), "content", content_type, "schema")
        schema_ref = f"{SPEC_URI}#/{location}"
    validator = Draft202012Validator({"$ref": schema_ref}, registry=REGISTRY)
    errors = [f"{list(e.absolute_path)}: {e.message}" for e in validator.iter_errors(response.json())]
    assert not errors, errors
    _assert_utc_timestamps(response.json())


def _assert_utc_timestamps(value) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if key in {"bucket_start", "hour_utc", "last_complete_hour"} and item is not None:
                assert item.endswith("Z"), f"{key}={item} is not UTC"
                datetime.fromisoformat(item)
            _assert_utc_timestamps(item)
    elif isinstance(value, list):
        for item in value:
            _assert_utc_timestamps(item)


def _editor_type(performer: dict) -> str:
    if performer.get("is_bot"):
        return "bot"
    if performer.get("is_system"):
        return "system"
    if performer.get("is_temp"):
        return "temp"
    if performer.get("user_id") is None:
        return "anonymous_ip"
    return "registered"


@dataclass
class Bucket:
    edits: int = 0
    creates: int = 0
    moves: int = 0
    deletes: int = 0
    reverts: int = 0
    pages: set = field(default_factory=set)
    editors: set = field(default_factory=set)

    def row(self) -> tuple:
        return (
            self.edits,
            self.creates,
            self.moves,
            self.deletes,
            self.reverts,
            len(self.pages),
            len(self.editors),
        )


def activity_from_page_change_fixture(grain: timedelta) -> dict[tuple, Bucket]:
    # Source: the 2,000 real events of tests/fixtures/page_change/events.jsonl.
    buckets: dict[tuple, Bucket] = defaultdict(Bucket)
    seen = set()
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    for line in (FIXTURES / "page_change" / "events.jsonl").open():
        event = json.loads(line)["event"]
        if event["meta"]["domain"] == "canary" or event["meta"]["id"] in seen:
            continue
        seen.add(event["meta"]["id"])
        moment = datetime.fromisoformat(event["dt"])
        start = moment - (moment - epoch) % grain
        performer = event["performer"]
        bucket = buckets[(event["wiki_id"], start, _editor_type(performer))]
        kind = event["page_change_kind"]
        bucket.edits += kind == "edit"
        bucket.creates += kind == "create"
        bucket.moves += kind == "move"
        bucket.deletes += kind == "delete"
        bucket.reverts += (event.get("revision") or {}).get("revert") is not None
        bucket.pages.add(event["page"]["page_id"])
        bucket.editors.add(performer.get("user_id") or performer["user_text"])
    return dict(buckets)


def dewiki_pages_from_xml_fixture() -> dict[str, tuple[int, int]]:
    """Non-redirect pages of the dewiki stub-meta-current fixture, keyed by underscored title."""
    pages = {}
    with gzip.open(XML_FIXTURE) as handle:
        for _, element in ET.iterparse(handle):
            if element.tag.endswith("}page"):
                ns = element.tag[: -len("page")]
                if element.find(f"{ns}redirect") is None:
                    title = element.find(f"{ns}title").text.replace(" ", "_")
                    pages[title] = (int(element.find(f"{ns}id").text), int(element.find(f"{ns}ns").text))
                element.clear()
    return pages


def dewiki_views_from_pageview_fixture(pages: dict[str, tuple[int, int]]) -> dict[int, dict[str, int]]:
    """de and de.m lines of the pageview fixture whose title is a page in the XML fixture."""
    views: dict[int, dict[str, int]] = defaultdict(lambda: {"desktop": 0, "mobile-web": 0})
    with gzip.open(PAGEVIEW_FIXTURE, "rt") as handle:
        for line in handle:
            code, title, count, _ = line.split(" ")
            if code in ("de", "de.m") and title in pages:
                views[pages[title][0]]["desktop" if code == "de" else "mobile-web"] += int(count)
    return dict(views)


@dataclass
class Seed:
    minute: dict[tuple, Bucket]
    hour: dict[tuple, Bucket]
    pages: dict[str, tuple[int, int]]
    views: dict[int, dict[str, int]]
    pageview_hour: datetime = PAGEVIEW_HOUR


def _owner_connection() -> psycopg.Connection:
    pg = load_settings().postgres
    return psycopg.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.database,
        user=pg.user,
        password=pg.password.get_secret_value(),
        autocommit=True,
    )


@pytest.fixture(scope="session")
def seed():
    if not os.environ.get("WIKIPULSE_POSTGRES__PASSWORD"):
        pytest.fail("set the compose PostgreSQL credentials: set -a; . ./.env; set +a")
    minute = activity_from_page_change_fixture(timedelta(minutes=1))
    hour = activity_from_page_change_fixture(timedelta(hours=1))
    pages = dewiki_pages_from_xml_fixture()
    views = dewiki_views_from_pageview_fixture(pages)
    page_meta = {page_id: (title, ns) for title, (page_id, ns) in pages.items()}
    emitted = datetime.now(UTC)

    with _owner_connection() as conn:
        days = sorted({key[1].date() for key in minute})
        conn.execute(
            "SELECT ops.ensure_day_partitions('stream.wiki_activity_minute', %s, %s)", (days[0], days[-1])
        )
        hours = [*sorted({key[1] for key in hour}), PAGEVIEW_HOUR]
        for parent in ("mart.wiki_activity_hourly", "mart.page_activity_hourly"):
            conn.execute(
                "SELECT ops.ensure_month_partitions(%s, %s, %s)",
                (parent, hours[0].date().replace(day=1), hours[-1].date().replace(day=1)),
            )

        clashes = conn.execute(
            """
            SELECT
              (SELECT count(*) FROM stream.wiki_activity_minute
               WHERE (wiki_id, window_start, editor_type)
                     IN (SELECT * FROM unnest(%s::text[], %s::timestamptz[], %s::text[])))
            + (SELECT count(*) FROM mart.wiki_activity_hourly
               WHERE (wiki_id, hour_utc, editor_type)
                     IN (SELECT * FROM unnest(%s::text[], %s::timestamptz[], %s::text[])))
            + (SELECT count(*) FROM mart.page_activity_hourly
               WHERE wiki_id = 'dewiki' AND hour_utc = %s AND page_id = ANY(%s))
            + (SELECT count(*) FROM mart.page_current WHERE wiki_id = 'dewiki' AND page_id = ANY(%s))
            """,
            (
                *map(list, zip(*minute, strict=True)),
                *map(list, zip(*hour, strict=True)),
                PAGEVIEW_HOUR,
                list(views),
                list(views),
            ),
        ).fetchone()[0]
        if clashes:
            pytest.fail(
                f"{clashes} rows already exist for the fixture-derived keys; tests only own rows they insert"
            )

        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO stream.wiki_activity_minute VALUES (" + ", ".join(["%s"] * 12) + ")",
                [(w, s, s + timedelta(minutes=1), e, *b.row(), emitted) for (w, s, e), b in minute.items()],
            )
            cur.executemany(
                "INSERT INTO mart.wiki_activity_hourly VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                [(w, s, e, *b.row()) for (w, s, e), b in hour.items()],
            )
            cur.executemany(
                "INSERT INTO mart.page_current (wiki_id, page_id, title, namespace_id, is_redirect,"
                " is_deleted, updated_at) VALUES ('dewiki', %s, %s, %s, false, false, %s)",
                [(pid, page_meta[pid][0], page_meta[pid][1], SNAPSHOT_TIME) for pid in views],
            )
            cur.executemany(
                "INSERT INTO mart.page_activity_hourly VALUES ('dewiki', %s, %s, %s, %s, %s, %s, 0, 0, 0, 0)",
                [
                    (
                        pid,
                        PAGEVIEW_HOUR,
                        page_meta[pid][1],
                        v["desktop"] + v["mobile-web"],
                        v["desktop"],
                        v["mobile-web"],
                    )
                    for pid, v in views.items()
                ],
            )
    try:
        yield Seed(minute=minute, hour=hour, pages=pages, views=views)
    finally:
        with _owner_connection() as conn:
            conn.execute(
                "DELETE FROM stream.wiki_activity_minute WHERE (wiki_id, window_start, editor_type)"
                " IN (SELECT * FROM unnest(%s::text[], %s::timestamptz[], %s::text[]))",
                tuple(map(list, zip(*minute, strict=True))),
            )
            conn.execute(
                "DELETE FROM mart.wiki_activity_hourly WHERE (wiki_id, hour_utc, editor_type)"
                " IN (SELECT * FROM unnest(%s::text[], %s::timestamptz[], %s::text[]))",
                tuple(map(list, zip(*hour, strict=True))),
            )
            conn.execute(
                "DELETE FROM mart.page_activity_hourly"
                " WHERE wiki_id = 'dewiki' AND hour_utc = %s AND page_id = ANY(%s)",
                (PAGEVIEW_HOUR, list(views)),
            )
            conn.execute(
                "DELETE FROM mart.page_current WHERE wiki_id = 'dewiki' AND page_id = ANY(%s)", (list(views),)
            )


@pytest.fixture(scope="session")
def client(seed):
    os.environ["WIKIPULSE_TELEMETRY__EXPORTER"] = "none"
    from wikipulse_api.app import create_app

    with TestClient(create_app(load_settings())) as test_client:
        test_client.assert_matches_spec = assert_matches_spec
        yield test_client
