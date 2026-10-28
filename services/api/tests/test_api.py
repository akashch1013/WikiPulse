from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from wikipulse_api.wikis import describe
from wikipulse_common.config import load_settings

ACTIVITY = "/v1/wikis/{wiki_id}/activity"
TOP = "/v1/pages/top-viewed"
COUNT_FIELDS = ("edits", "creates", "moves", "deletes", "reverts", "distinct_pages", "distinct_editors")


def get(client, spec_path, url, status=200, **params):
    response = client.get(url, params=params)
    assert response.status_code == status, response.text
    client.assert_matches_spec(spec_path, status, response)
    return response.json()


def walk(client, spec_path, url, **params):
    rows, cursors, page = [], set(), get(client, spec_path, url, **params)
    while True:
        rows.extend(page["data"])
        if page["next_cursor"] is None:
            return rows
        assert page["next_cursor"] not in cursors
        cursors.add(page["next_cursor"])
        page = get(client, spec_path, url, **params, cursor=page["next_cursor"])


def iso(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def fixture_window(seed, wiki_id):
    starts = [start for (wiki, start, _) in seed.minute if wiki == wiki_id]
    return min(starts), max(starts) + timedelta(minutes=1)


def test_health(client):
    assert get(client, "/healthz", "/healthz") == {"status": "ok"}


def test_ready(client):
    assert get(client, "/readyz", "/readyz") == {"status": "ready"}


def test_not_ready_when_database_unreachable(client, monkeypatch):
    monkeypatch.setenv("WIKIPULSE_TELEMETRY__EXPORTER", "none")
    monkeypatch.setenv("WIKIPULSE_POSTGRES__PORT", "1")
    from wikipulse_api.app import create_app

    with TestClient(create_app(load_settings())) as down:
        down.assert_matches_spec = client.assert_matches_spec
        assert get(down, "/healthz", "/healthz") == {"status": "ok"}
        problem = get(down, "/readyz", "/readyz", status=503)
        assert problem["status"] == 503
        get(down, "/v1/freshness", "/v1/freshness", status=503)


def test_freshness_lists_every_dataset(client, seed):
    data = get(client, "/v1/freshness", "/v1/freshness")["data"]
    assert [row["source"] for row in data] == [
        "stream",
        "pageviews_hourly",
        "page_snapshot",
        "mediawiki_history",
        "marts",
    ]
    stream = data[0]
    latest_seeded = max(start for (_, start, _) in seed.minute) + timedelta(minutes=1)
    assert datetime.fromisoformat(stream["last_complete_hour"]) >= latest_seeded
    assert stream["lag_seconds"] >= 0


def test_wikis_paginate_without_gaps(client, seed):
    rows = walk(client, "/v1/wikis", "/v1/wikis", limit=7)
    ids = [row["wiki_id"] for row in rows]
    assert ids == sorted(set(ids))
    assert {wiki for (wiki, _, _) in seed.minute} <= set(ids)
    by_id = {row["wiki_id"]: row for row in rows}
    assert by_id["enwiki"]["domain"] == "en.wikipedia.org" and by_id["enwiki"]["is_focus"]
    assert by_id["commonswiki"]["domain"] == "commons.wikimedia.org"
    assert by_id["wikidatawiki"]["domain"] == "www.wikidata.org"


def test_wikis_focus_filter(client):
    focus = get(client, "/v1/wikis", "/v1/wikis", is_focus="true")["data"]
    assert {row["wiki_id"] for row in focus} == set(load_settings().wikis.page_level)
    others = walk(client, "/v1/wikis", "/v1/wikis", is_focus="false", limit=500)
    assert not {row["wiki_id"] for row in others} & set(load_settings().wikis.page_level)


@pytest.mark.parametrize(
    ("wiki_id", "expected"),
    [
        ("dewiki", ("de.wikipedia.org", "wikipedia", "de")),
        ("enwiktionary", ("en.wiktionary.org", "wiktionary", "en")),
        ("zh_min_nanwiki", ("zh-min-nan.wikipedia.org", "wikipedia", "zh-min-nan")),
        ("be_x_oldwiki", ("be-tarask.wikipedia.org", "wikipedia", "be-tarask")),
        ("zhwikisource", ("zh.wikisource.org", "wikisource", "zh")),
        ("bdwikimedia", ("bd.wikimedia.org", "wikimedia", None)),
        ("mediawikiwiki", ("www.mediawiki.org", "mediawiki", None)),
    ],
)
def test_wiki_domains(wiki_id, expected):
    info = describe(wiki_id)
    assert (info.domain, info.project, info.language_code) == expected


def test_minute_activity_matches_fixture_and_paginates(client, seed):
    start, end = fixture_window(seed, "enwiki")
    rows = walk(
        client,
        ACTIVITY,
        ACTIVITY.format(wiki_id="enwiki"),
        grain="minute",
        limit=5,
        **{"from": iso(start)},
        to=iso(end),
    )
    keys = [(row["bucket_start"], row["editor_type"]) for row in rows]
    assert keys == sorted(set(keys))
    expected = {(iso(s), e): bucket.row() for (w, s, e), bucket in seed.minute.items() if w == "enwiki"}
    assert {key: tuple(row[f] for f in COUNT_FIELDS) for key, row in zip(keys, rows, strict=True)} == expected
    assert all(row["is_final"] for row in rows)


def test_hour_activity_and_editor_type_filter(client, seed):
    hour = min(start for (wiki, start, _) in seed.hour if wiki == "enwiki")
    params = {"from": iso(hour), "to": iso(hour + timedelta(hours=1))}
    rows = get(client, ACTIVITY, ACTIVITY.format(wiki_id="enwiki"), **params)["data"]
    expected = {e: b.row() for (w, s, e), b in seed.hour.items() if w == "enwiki" and s == hour}
    assert {row["editor_type"]: tuple(row[f] for f in COUNT_FIELDS) for row in rows} == expected

    bots = get(client, ACTIVITY, ACTIVITY.format(wiki_id="enwiki"), editor_type="bot", **params)["data"]
    assert [row["editor_type"] for row in bots] == (["bot"] if "bot" in expected else [])


def test_activity_freshness_source_follows_grain(client, seed):
    start, end = fixture_window(seed, "enwiki")
    minute = get(
        client,
        ACTIVITY,
        ACTIVITY.format(wiki_id="enwiki"),
        grain="minute",
        **{"from": iso(start)},
        to=iso(end),
    )
    hour = get(client, ACTIVITY, ACTIVITY.format(wiki_id="enwiki"), **{"from": iso(start)}, to=iso(end))
    assert minute["freshness"]["source"] == "stream"
    assert hour["freshness"]["source"] == "marts"


def test_activity_unknown_wiki(client):
    problem = get(client, ACTIVITY, ACTIVITY.format(wiki_id="nosuchwiki"), status=404)
    assert problem["status"] == 404


@pytest.mark.parametrize(
    ("params", "status"),
    [
        ({"from": "2026-10-28T04:00:00Z", "to": "2026-10-28T03:00:00Z"}, 422),
        ({"grain": "minute", "from": "2026-10-25T00:00:00Z", "to": "2026-10-28T00:00:00Z"}, 422),
        ({"from": "2026-09-01T00:00:00Z", "to": "2026-10-28T00:00:00Z"}, 422),
        ({"from": "2026-10-28T03:00:00"}, 400),
        ({"grain": "second"}, 400),
        ({"editor_type": "human"}, 400),
        ({"limit": 0}, 400),
        ({"limit": 501}, 400),
        ({"cursor": "not-a-cursor"}, 400),
    ],
)
def test_activity_rejects_bad_parameters(client, seed, params, status):
    problem = get(client, ACTIVITY, ACTIVITY.format(wiki_id="enwiki"), status=status, **params)
    assert problem["status"] == status


def test_cursor_is_bound_to_its_filters(client, seed):
    start, end = fixture_window(seed, "enwiki")
    params = {"grain": "minute", "from": iso(start), "to": iso(end), "limit": 1}
    first = get(client, ACTIVITY, ACTIVITY.format(wiki_id="enwiki"), **params)
    assert first["next_cursor"]
    get(
        client,
        ACTIVITY,
        ACTIVITY.format(wiki_id="enwiki"),
        status=400,
        **{**params, "editor_type": "bot"},
        cursor=first["next_cursor"],
    )


def test_top_viewed_matches_fixture_and_paginates(client, seed):
    rows = walk(client, TOP, TOP, wiki_id="dewiki", hour=iso(seed.pageview_hour), limit=10)
    expected = sorted(
        (
            (v["desktop"] + v["mobile-web"], pid)
            for pid, v in seed.views.items()
            if v["desktop"] + v["mobile-web"]
        ),
        key=lambda item: (-item[0], item[1]),
    )
    assert [(row["views"], row["page_id"]) for row in rows] == expected
    titles = {page_id: title for title, (page_id, _) in seed.pages.items()}
    assert all(row["title"] == titles[row["page_id"]] for row in rows)
    assert all(row["hour_utc"] == iso(seed.pageview_hour) for row in rows)


def test_top_viewed_access_method_and_namespace(client, seed):
    desktop = walk(
        client, TOP, TOP, wiki_id="dewiki", hour=iso(seed.pageview_hour), access_method="desktop", limit=25
    )
    expected = sorted(
        ((v["desktop"], pid) for pid, v in seed.views.items() if v["desktop"]),
        key=lambda item: (-item[0], item[1]),
    )
    assert [(row["desktop_views"], row["page_id"]) for row in desktop] == expected

    articles = walk(client, TOP, TOP, wiki_id="dewiki", hour=iso(seed.pageview_hour), namespace=0, limit=50)
    namespaces = {page_id: ns for (page_id, ns) in seed.pages.values()}
    assert articles and all(row["namespace"] == 0 for row in articles)
    assert len(articles) == sum(1 for pid in seed.views if namespaces[pid] == 0)


@pytest.mark.parametrize(
    ("params", "status"),
    [
        ({"wiki_id": "dewiki", "hour": "2026-10-26T11:30:00Z"}, 400),
        ({"wiki_id": "dewiki"}, 400),
        ({"wiki_id": "commonswiki", "hour": "2026-10-26T11:00:00Z"}, 404),
        ({"wiki_id": "dewiki", "hour": "2026-10-26T11:00:00Z", "access_method": "mobile-app"}, 400),
    ],
)
def test_top_viewed_rejects_bad_parameters(client, seed, params, status):
    get(client, TOP, TOP, status=status, **params)


def test_only_documented_routes_are_served(client):
    for path in ("/docs", "/redoc", "/openapi.json"):
        response = client.get(path)
        assert response.status_code == 404
        assert response.headers["content-type"].startswith("application/problem+json")
