from collections import Counter
from datetime import UTC, datetime

from wikipulse_stream.app import add_event, event_time_ms, init_window, window_rows


def test_event_time_comes_from_event_dt(internal_events):
    event = internal_events[0]
    expected = datetime.fromisoformat(event["event_dt"].replace("Z", "+00:00"))
    assert event_time_ms(event, None, 0, None) == int(expected.timestamp() * 1000)


def test_window_aggregate_matches_independent_counts(internal_events):
    events = [e for e in internal_events if e["wiki_id"] == "enwiki"]
    agg = init_window(events[0])
    for event in events[1:]:
        agg = add_event(agg, event)
    rows = {r["editor_type"]: r for r in window_rows({"start": 0, "end": 60_000, "value": agg})}

    for editor_type in {e["editor_type"] for e in events}:
        mine = [e for e in events if e["editor_type"] == editor_type]
        kinds = Counter(e["change_kind"] for e in mine)
        row = rows[editor_type]
        assert row["wiki_id"] == "enwiki"
        assert row["edits"] == kinds["edit"]
        assert row["creates"] == kinds["create"]
        assert row["moves"] == kinds["move"]
        assert row["deletes"] == kinds["delete"]
        assert row["reverts"] == sum(1 for e in mine if e["revert"])
        assert row["distinct_pages"] == len({e["page_id"] for e in mine})
        assert row["distinct_editors"] == len({e["editor_key"] for e in mine if e["editor_key"]})
        assert row["window_start"] == datetime.fromtimestamp(0, UTC)
