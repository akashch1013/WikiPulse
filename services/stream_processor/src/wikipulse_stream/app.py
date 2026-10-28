import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from psycopg.conninfo import make_conninfo
from quixstreams import Application, State, message_context

from wikipulse_common.config import Settings
from wikipulse_common.kafka import producer_config
from wikipulse_common.telemetry import get_meter
from wikipulse_stream.sinks import BronzeSink, WindowSink, parse_utc

SERVICE = "stream-processor"
CONSUMER_GROUP = "wikipulse-stream-processor"
WINDOW_TABLE = "stream.wiki_activity_minute"
WINDOW_KIND = "wiki_minute"
COUNTED_KINDS = {"edit": "edits", "create": "creates", "move": "moves", "delete": "deletes"}

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Topics:
    source: str
    dlq: str
    late: str


def iso(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def event_time_ms(value: Any, headers: Any, timestamp: float, timestamp_type: Any) -> int:
    return int(parse_utc(value["event_dt"]).timestamp() * 1000)


class SideOutput:
    # Producer for the DLQ and late topics. The sinks flush it before their own writes, so a Kafka offset
    # is never committed while a DLQ or late record for that offset is still in the send buffer.
    def __init__(self, settings: Settings) -> None:
        self._producer = Producer(producer_config(settings.kafka, f"{SERVICE}-side"))

    def send(self, topic: str, key: str, value: dict) -> None:
        self._producer.produce(topic, key=key.encode(), value=json.dumps(value).encode())
        self._producer.poll(0)

    def flush(self) -> None:
        remaining = self._producer.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} side-output records not delivered")


def empty_counts() -> dict:
    return {"edits": 0, "creates": 0, "moves": 0, "deletes": 0, "reverts": 0, "pages": {}, "editors": {}}


def add_event(agg: dict, event: dict) -> dict:
    counts = agg["by_editor_type"].setdefault(event["editor_type"], empty_counts())
    column = COUNTED_KINDS.get(event["change_kind"])
    if column:
        counts[column] += 1
    if event.get("revert"):
        counts["reverts"] += 1
    # Dicts stand in for sets because window state is serialised as JSON.
    counts["pages"][str(event["page_id"])] = 1
    if event.get("editor_key"):
        counts["editors"][event["editor_key"]] = 1
    return agg


def init_window(event: dict) -> dict:
    return add_event({"wiki_id": event["wiki_id"], "by_editor_type": {}}, event)


def window_rows(window: dict) -> list[dict]:
    start = datetime.fromtimestamp(window["start"] / 1000, UTC)
    end = datetime.fromtimestamp(window["end"] / 1000, UTC)
    emitted_at = datetime.now(UTC)
    agg = window["value"]
    return [
        {
            "wiki_id": agg["wiki_id"],
            "window_start": start,
            "window_end": end,
            "editor_type": editor_type,
            "edits": counts["edits"],
            "creates": counts["creates"],
            "moves": counts["moves"],
            "deletes": counts["deletes"],
            "reverts": counts["reverts"],
            "distinct_pages": len(counts["pages"]),
            "distinct_editors": len(counts["editors"]),
            "emitted_at": emitted_at,
        }
        for editor_type, counts in agg["by_editor_type"].items()
    ]


def postgres_conninfo(settings: Settings) -> str:
    pg = settings.postgres
    return make_conninfo(
        host=pg.host, port=pg.port, dbname=pg.database, user=pg.user, password=pg.password.get_secret_value()
    )


def build_application(
    settings: Settings,
    topics: Topics,
    state_dir: Path,
    heartbeat_path: Path,
    consumer_group: str = CONSUMER_GROUP,
    window_table: str = WINDOW_TABLE,
) -> tuple[Application, SideOutput]:
    meter = get_meter(SERVICE)
    duplicates = meter.create_counter("duplicates_dropped_total", description="Events dropped as duplicates")
    late_events = meter.create_counter("late_events_total", description="Events after their window closed")
    dlq_events = meter.create_counter("dlq_total", description="Events routed to the dead-letter topic")

    side = SideOutput(settings)
    future_tolerance = timedelta(seconds=settings.stream.future_tolerance_seconds)
    dedup_ttl = timedelta(hours=settings.windows.dedup_ttl_hours)
    max_payload = settings.stream.max_payload_bytes

    app = Application(
        broker_address=settings.kafka.bootstrap_servers,
        consumer_group=consumer_group,
        auto_offset_reset="earliest",
        state_dir=state_dir,
        processing_guarantee="at-least-once",
        loglevel=settings.telemetry.log_level,
    )

    def not_future(event: dict) -> bool:
        now = datetime.now(UTC)
        if parse_utc(event["event_dt"]) <= now + future_tolerance:
            return True
        ctx = message_context()
        side.send(
            topics.dlq,
            event["event_id"],
            {
                "failed_at": iso(now),
                "stage": "stream_processor",
                "reason_code": "future_timestamp",
                "reason_detail": f"event_dt is over {future_tolerance} ahead of {iso(now)}",
                "upstream_schema": event.get("upstream_schema"),
                "source_position": f"{ctx.topic}/{ctx.partition}/{ctx.offset}",
                "payload": json.dumps(event)[:max_payload],
            },
        )
        dlq_events.add(1, {"reason_code": "future_timestamp"})
        return False

    def first_seen(event: dict, state: State) -> bool:
        # The message key is "<wiki_id>:<page_id>", so every copy of an event lands in the same partition
        # and the same keyed state.
        if state.get(event["event_id"]) is not None:
            duplicates.add(1, {"wiki_id": event["wiki_id"]})
            return False
        state.set(event["event_id"], 1, ttl=dedup_ttl)
        return True

    def on_late(value, key, timestamp_ms, late_by_ms, start, end, store_name, topic, partition, offset):
        side.send(
            topics.late,
            value["event_id"],
            {
                "event_id": value["event_id"],
                "wiki_id": value["wiki_id"],
                "page_id": value["page_id"],
                "event_dt": value["event_dt"],
                "observed_at": iso(datetime.now(UTC)),
                "window_kind": WINDOW_KIND,
                "window_start": iso(datetime.fromtimestamp(start / 1000, UTC)),
                "window_end": iso(datetime.fromtimestamp(end / 1000, UTC)),
                "lateness_seconds": late_by_ms / 1000,
            },
        )
        late_events.add(1, {"window_kind": WINDOW_KIND})
        return False

    source = app.topic(
        topics.source, key_deserializer="str", value_deserializer="json", timestamp_extractor=event_time_ms
    )
    sdf = app.dataframe(source)
    sdf = sdf.filter(not_future)
    sdf = sdf.filter(first_seen, stateful=True)
    sdf.sink(BronzeSink(settings, side, heartbeat_path))

    windows = (
        sdf.group_by(lambda event: event["wiki_id"], name="wiki_id")
        .tumbling_window(
            duration_ms=timedelta(seconds=settings.windows.wiki_window_seconds),
            grace_ms=timedelta(seconds=settings.windows.grace_seconds),
            name=WINDOW_KIND,
            on_late=on_late,
        )
        .reduce(reducer=add_event, initializer=init_window)
        # Key closing: a wiki's window closes once that wiki's own event time passes window end + grace.
        # Quix 3.28 partition closing compares late events against the last expired window end minus the
        # grace again, so an event arriving up to one grace period after expiry is neither counted nor
        # reported late. Quiet wikis therefore emit their minutes later; the hourly batch recompute
        # covers them either way.
        .final(closing_strategy="key")
    )
    windows = windows.apply(window_rows, expand=True)
    windows.sink(WindowSink(postgres_conninfo(settings), window_table, side, heartbeat_path))
    return app, side
