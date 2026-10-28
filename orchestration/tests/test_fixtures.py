import bz2
import gzip
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from wikipulse_orchestration.history.columns import COLUMNS, USER_TEXT_COLUMNS

FIXTURES = Path(__file__).resolve().parents[2] / "tests" / "fixtures"
SOURCES = ["pageviews", "xml", "history", "pageview_complete"]
PLACEHOLDER_USER = re.compile(r"^(Fixture-User-\d+|192\.0\.2\.\d+)$")
PAGEVIEW_LINE = re.compile(r"^(\S+) (\S+) (\d+) (\d+)$")
HOURLY = re.compile(r"([A-X])(\d+)")


def source(name):
    return json.loads((FIXTURES / name / "SOURCE.json").read_text())


def fixture_path(name):
    files = source(name)["files"]
    assert len(files) == 1
    return FIXTURES / name / next(iter(files))


@pytest.mark.parametrize("name", SOURCES)
def test_fixture_checksums_match_source_record(name):
    record = source(name)
    for required in ("url", "captured_at", "selection", "transformation", "files"):
        assert record[required], required
    for filename, sha256 in record["files"].items():
        assert hashlib.sha256((FIXTURES / name / filename).read_bytes()).hexdigest() == sha256


def test_pageview_fixture_lines():
    per_code = {}
    with gzip.open(fixture_path("pageviews"), "rt", encoding="utf-8") as fh:
        for line in fh:
            match = PAGEVIEW_LINE.match(line.rstrip("\n"))
            assert match, line
            code, _title, views, response_size = match.groups()
            assert int(views) > 0
            assert response_size == "0"
            per_code[code] = per_code.get(code, 0) + 1
    assert per_code == {"en": 12500, "en.m": 12500, "de": 12500, "de.m": 12500}


def test_xml_fixture_pages():
    pages = 0
    with gzip.open(fixture_path("xml"), "rb") as fh:
        for _event, elem in ET.iterparse(fh):
            if not elem.tag.endswith("}page"):
                continue
            ns = elem.tag[: -len("page")]
            for child in ("title", "ns", "id"):
                assert elem.find(ns + child) is not None, child
            revision = elem.find(ns + "revision")
            assert revision.find(ns + "id") is not None
            assert revision.find(ns + "timestamp") is not None
            text = revision.find(ns + "text")
            assert "bytes" in text.attrib
            assert "sha1" in text.attrib
            contributor = revision.find(ns + "contributor")
            if contributor.get("deleted") != "deleted":
                identity = contributor.find(ns + "username")
                if identity is None:
                    identity = contributor.find(ns + "ip")
                assert PLACEHOLDER_USER.match(identity.text), identity.text
            pages += 1
            elem.clear()
    assert pages == 400


def test_history_fixture_rows():
    user_idx = [COLUMNS.index(c) for c in USER_TEXT_COLUMNS]
    rows = 0
    with bz2.open(fixture_path("history"), "rt", encoding="utf-8") as fh:
        for line in fh:
            fields = line.rstrip("\n").split("\t")
            assert len(fields) == len(COLUMNS)
            for i in user_idx:
                assert fields[i] == "" or PLACEHOLDER_USER.match(fields[i]), fields[i]
            rows += 1
    assert rows == 10000


def test_pageview_complete_fixture_lines():
    rows = 0
    with bz2.open(fixture_path("pageview_complete"), "rt", encoding="utf-8") as fh:
        for line in fh:
            fields = line.rstrip("\n").split(" ")
            assert len(fields) in (5, 6), line
            wiki_code, access, daily_total, hourly = fields[0], fields[-3], fields[-2], fields[-1]
            assert wiki_code in ("de.wikipedia", "en.wikipedia")
            assert access in ("desktop", "mobile-web", "mobile-app")
            hours = HOURLY.findall(hourly)
            assert "".join(f"{h}{n}" for h, n in hours) == hourly
            assert sum(int(n) for _h, n in hours) == int(daily_total)
            rows += 1
    assert rows == 5000
