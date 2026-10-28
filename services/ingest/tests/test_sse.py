import json
import random

from wikipulse_ingest.sse import SseParser


def _encode(lines: list[dict], newline: str) -> bytes:
    parts = [": comment lines are ignored" + newline]
    for line in lines:
        data = json.dumps(line["event"])
        parts.append(f"event: message{newline}id: {line['sse_id']}{newline}data: {data}{newline}{newline}")
    # A live stream never ends on the bare CR of a blank line: the next keepalive completes it.
    parts.append(": keepalive" + newline + newline)
    return "".join(parts).encode()


def _feed_in_random_chunks(payload: bytes, seed: int):
    rng = random.Random(seed)
    parser = SseParser()
    events, position = [], 0
    while position < len(payload):
        size = rng.randint(1, 700)
        events.extend(parser.feed(payload[position : position + size]))
        position += size
    return events


def test_parser_recovers_every_event_across_arbitrary_chunk_boundaries(fixture_lines):
    lines = fixture_lines[:300]
    for newline in ("\n", "\r\n", "\r"):
        payload = _encode(lines, newline)
        for seed in range(5):
            events = _feed_in_random_chunks(payload, seed)
            assert [e.id for e in events] == [line["sse_id"] for line in lines]
            assert [json.loads(e.data) for e in events] == [line["event"] for line in lines]


def test_multiline_data_and_multibyte_split():
    parser = SseParser()
    payload = "id: 7\ndata: first\ndata: zweite Zeile ü\n\n".encode()
    split = payload.index("ü".encode()) + 1
    events = parser.feed(payload[:split]) + parser.feed(payload[split:])
    assert len(events) == 1
    assert events[0].data == "first\nzweite Zeile ü"
    assert events[0].id == "7"


def test_events_without_data_are_not_dispatched_but_keep_id():
    parser = SseParser()
    assert parser.feed(b"id: 9\n\n") == []
    assert parser.feed(b"data: x\n\n")[0].id == "9"
