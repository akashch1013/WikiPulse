import json
from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg
from psycopg.types.json import Jsonb


@dataclass(frozen=True)
class Checkpoint:
    last_event_id: str
    last_event_dt: datetime
    events_acked: int


def position_timestamp(sse_id: str) -> datetime | None:
    """Newest Kafka timestamp in an EventStreams id array; entries carrying offsets are ignored."""
    stamps = [entry["timestamp"] for entry in json.loads(sse_id) if "timestamp" in entry]
    return datetime.fromtimestamp(max(stamps) / 1000, UTC) if stamps else None


class CheckpointStore:
    def __init__(self, connection: psycopg.Connection, consumer: str) -> None:
        self._conn = connection
        self.consumer = consumer

    def load(self) -> Checkpoint | None:
        row = self._conn.execute(
            "SELECT last_event_id, last_event_dt, events_acked"
            " FROM ops.stream_checkpoint WHERE consumer = %s",
            (self.consumer,),
        ).fetchone()
        if row is None:
            return None
        # The id array is stored as jsonb; it is sent back to EventStreams in compact form.
        return Checkpoint(json.dumps(row[0], separators=(",", ":")), row[1], row[2])

    def save(self, checkpoint: Checkpoint) -> None:
        self._conn.execute(
            """
            INSERT INTO ops.stream_checkpoint
                (consumer, last_event_id, last_event_dt, events_acked, updated_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (consumer) DO UPDATE SET
                last_event_id = EXCLUDED.last_event_id,
                last_event_dt = EXCLUDED.last_event_dt,
                events_acked = EXCLUDED.events_acked,
                updated_at = EXCLUDED.updated_at
            """,
            (
                self.consumer,
                Jsonb(json.loads(checkpoint.last_event_id)),
                checkpoint.last_event_dt,
                checkpoint.events_acked,
            ),
        )

    def record_gap(self, start: datetime, end: datetime, reason: str) -> None:
        self._conn.execute(
            "INSERT INTO ops.stream_gaps (consumer, gap_start, gap_end, reason) VALUES (%s, %s, %s, %s)",
            (self.consumer, start, end, reason),
        )
