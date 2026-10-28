import json
import os
import uuid
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from wikipulse_common.config import load_settings
from wikipulse_common.lake import catalog_dsn, connect_lake
from wikipulse_common.storage import MARKER_FILE
from wikipulse_stream.app import Topics, build_application, postgres_conninfo
from wikipulse_stream.sinks import BRONZE_TABLE, parse_utc

pytestmark = pytest.mark.skipif(
    not os.environ.get("WIKIPULSE_POSTGRES__PASSWORD"), reason="needs the compose Kafka and PostgreSQL"
)

DUPLICATES = 25


def ts_ms(event: dict) -> int:
    return int(parse_utc(event["event_dt"]).timestamp() * 1000)


def derived(event: dict, event_dt: datetime) -> dict:
    # A copy of a real event with a new identity and a shifted event time.
    copy = dict(event)
    copy["event_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL, event["event_id"] + event_dt.isoformat()))
    copy["event_dt"] = event_dt.isoformat().replace("+00:00", "Z")
    return copy


def expected_windows(stream: list[dict], duration_ms: int, grace_ms: int) -> tuple[dict, set[str]]:
    # Independent model of key-closed tumbling windows: an event is late when its window start is at or
    # before (latest event time seen for its wiki) - grace - duration.
    latest: dict[str, int] = {}
    late: set[str] = set()
    windows: dict[tuple[str, int], list[dict]] = defaultdict(list)
    for event in stream:
        wiki, ts = event["wiki_id"], ts_ms(event)
        latest[wiki] = max(ts, latest.get(wiki, 0))
        start = ts - ts % duration_ms
        if start <= latest[wiki] - grace_ms - duration_ms:
            late.add(event["event_id"])
            continue
        windows[(wiki, start)].append(event)

    rows = {}
    for (wiki, start), events in windows.items():
        if start > latest[wiki] - grace_ms - duration_ms:
            continue
        for editor_type in {e["editor_type"] for e in events}:
            mine = [e for e in events if e["editor_type"] == editor_type]
            kinds = Counter(e["change_kind"] for e in mine)
            rows[(wiki, datetime.fromtimestamp(start / 1000, UTC), editor_type)] = (
                kinds["edit"],
                kinds["create"],
                kinds["move"],
                kinds["delete"],
                sum(1 for e in mine if e["revert"]),
                len({e["page_id"] for e in mine}),
                len({e["editor_key"] for e in mine if e["editor_key"]}),
            )
    return rows, late


def consume_all(bootstrap: str, topic: str) -> list[dict]:
    consumer = Consumer(
        {"bootstrap.servers": bootstrap, "group.id": f"check-{uuid.uuid4()}", "auto.offset.reset": "earliest"}
    )
    consumer.subscribe([topic])
    records, idle = [], 0
    while idle < 5:
        message = consumer.poll(1.0)
        if message is None or message.error():
            idle += 1
            continue
        records.append(json.loads(message.value()))
    consumer.close()
    return records


@pytest.fixture
def isolated(monkeypatch, tmp_path):
    run = uuid.uuid4().hex[:10]
    (tmp_path / MARKER_FILE).touch()
    monkeypatch.setenv("WIKIPULSE_LAKE__DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("WIKIPULSE_LAKE__DUCKDB_TEMP_DIRECTORY", str(tmp_path / "spill"))
    monkeypatch.setenv("WIKIPULSE_LAKE__DUCKDB_MEMORY_LIMIT", "256MB")
    monkeypatch.setenv("WIKIPULSE_LAKE__CATALOG_SCHEMA", f"ducklake_test_{run}")
    monkeypatch.setenv("WIKIPULSE_TELEMETRY__EXPORTER", "none")
    settings = load_settings()
    topics = Topics(f"test.page_change.{run}", f"test.dlq.{run}", f"test.late.{run}")
    group = f"test-stream-{run}"
    window_schema = f"stream_test_{run}"

    admin = AdminClient({"bootstrap.servers": settings.kafka.bootstrap_servers})
    for future in admin.create_topics(
        [NewTopic(t, num_partitions=1, replication_factor=1) for t in vars(topics).values()]
    ).values():
        future.result()
    with psycopg.connect(postgres_conninfo(settings), autocommit=True) as pg:
        pg.execute(f"CREATE SCHEMA {window_schema}")
        pg.execute(
            f"CREATE TABLE {window_schema}.wiki_activity_minute "
            "(LIKE stream.wiki_activity_minute INCLUDING DEFAULTS INCLUDING CONSTRAINTS, "
            "PRIMARY KEY (wiki_id, window_start, editor_type)) PARTITION BY RANGE (window_start)"
        )
    yield settings, topics, group, f"{window_schema}.wiki_activity_minute", tmp_path

    leftovers = [t for t in admin.list_topics(timeout=10).topics if run in t]
    for future in admin.delete_topics(leftovers).values():
        future.result()
    with psycopg.connect(postgres_conninfo(settings), autocommit=True) as pg:
        pg.execute(f"DROP SCHEMA {window_schema} CASCADE")
    with psycopg.connect(catalog_dsn(settings), autocommit=True) as pg:
        pg.execute(f'DROP SCHEMA IF EXISTS "ducklake_test_{run}" CASCADE')


def test_dedup_bronze_windows_late_and_dlq(isolated, internal_events):
    settings, topics, group, window_table, tmp_path = isolated
    events = internal_events
    enwiki = [e for e in events if e["wiki_id"] == "enwiki"]
    future = derived(events[100], datetime.now(UTC) + timedelta(days=1))
    late = derived(enwiki[-1], parse_utc(enwiki[0]["event_dt"]))
    duplicates = events[: DUPLICATES * 10 : 10]

    sent = [*events[:500], future, *events[500:], *duplicates, late]
    producer = Producer({"bootstrap.servers": settings.kafka.bootstrap_servers})
    for event in sent:
        producer.produce(
            topics.source,
            key=f"{event['wiki_id']}:{event['page_id']}".encode(),
            value=json.dumps(event).encode(),
        )
    producer.flush(30)

    app, side = build_application(
        settings,
        topics,
        tmp_path / "state",
        tmp_path / "heartbeat",
        consumer_group=group,
        window_table=window_table,
    )
    app.run(timeout=20)
    side.flush()

    accepted = [*events, late]
    con = connect_lake(settings)
    bronze = con.execute(
        f"SELECT event_id::VARCHAR, kafka_partition, kafka_offset, source_file FROM {BRONZE_TABLE}"
    ).fetchall()
    con.close()
    assert sorted(r[0] for r in bronze) == sorted(e["event_id"] for e in accepted)
    assert len({(r[1], r[2]) for r in bronze}) == len(bronze)
    assert all(r[3].startswith(f"{topics.source}/0/") for r in bronze)
    assert (tmp_path / "heartbeat").exists()

    windows = settings.windows
    expected, expected_late = expected_windows(
        [*events, late], windows.wiki_window_seconds * 1000, windows.grace_seconds * 1000
    )
    with psycopg.connect(postgres_conninfo(settings)) as pg:
        actual = {
            (r[0], r[1], r[2]): tuple(r[3:])
            for r in pg.execute(
                "SELECT wiki_id, window_start, editor_type, edits, creates, moves, deletes, reverts, "
                f"distinct_pages, distinct_editors FROM {window_table}"
            )
        }
    assert expected
    assert actual == expected

    assert {
        r["event_id"] for r in consume_all(settings.kafka.bootstrap_servers, topics.late)
    } == expected_late
    assert late["event_id"] in expected_late
    dlq = consume_all(settings.kafka.bootstrap_servers, topics.dlq)
    assert [json.loads(r["payload"])["event_id"] for r in dlq] == [future["event_id"]]
    assert dlq[0]["reason_code"] == "future_timestamp"
    assert dlq[0]["stage"] == "stream_processor"
