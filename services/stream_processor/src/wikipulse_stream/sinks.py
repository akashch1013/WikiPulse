import json
import uuid
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import psycopg
from quixstreams.sinks import BatchingSink, SinkBatch

from wikipulse_common.config import Settings
from wikipulse_common.lake import connect_lake

if TYPE_CHECKING:
    from wikipulse_stream.app import SideOutput

BRONZE_TABLE = "bronze.page_change_events"
BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
    event_id UUID NOT NULL,
    event_dt TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL,
    wiki_id VARCHAR NOT NULL,
    page_id BIGINT NOT NULL,
    change_kind VARCHAR NOT NULL,
    payload JSON NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    ingest_date DATE NOT NULL,
    source_file VARCHAR NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL,
    run_id VARCHAR NOT NULL
)
"""

WINDOW_UPSERT = """
INSERT INTO {table} (
    wiki_id, window_start, window_end, editor_type, edits, creates, moves, deletes, reverts,
    distinct_pages, distinct_editors, emitted_at
) VALUES (
    %(wiki_id)s, %(window_start)s, %(window_end)s, %(editor_type)s, %(edits)s, %(creates)s, %(moves)s,
    %(deletes)s, %(reverts)s, %(distinct_pages)s, %(distinct_editors)s, %(emitted_at)s
)
ON CONFLICT (wiki_id, window_start, editor_type) DO UPDATE SET
    window_end = EXCLUDED.window_end,
    edits = EXCLUDED.edits,
    creates = EXCLUDED.creates,
    moves = EXCLUDED.moves,
    deletes = EXCLUDED.deletes,
    reverts = EXCLUDED.reverts,
    distinct_pages = EXCLUDED.distinct_pages,
    distinct_editors = EXCLUDED.distinct_editors,
    emitted_at = EXCLUDED.emitted_at
"""


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


class HeartbeatSink(BatchingSink):
    def __init__(self, side_output: "SideOutput", heartbeat_path: Path) -> None:
        super().__init__()
        self._side_output = side_output
        self._heartbeat_path = heartbeat_path

    def flush(self) -> None:
        self._side_output.flush()
        super().flush()
        self._heartbeat_path.touch()


class BronzeSink(HeartbeatSink):
    def __init__(self, settings: Settings, side_output: "SideOutput", heartbeat_path: Path) -> None:
        super().__init__(side_output, heartbeat_path)
        self._settings = settings
        self._con = None

    def setup(self) -> None:
        self._con = connect_lake(self._settings)
        self._con.execute(BRONZE_DDL)
        self._con.execute("CREATE TEMP TABLE bronze_batch AS SELECT * FROM " + BRONZE_TABLE + " WITH NO DATA")

    def write(self, batch: SinkBatch) -> None:
        loaded_at = datetime.now(UTC)
        offsets = [item.offset for item in batch]
        first, last = min(offsets), max(offsets)
        source = f"{batch.topic}/{batch.partition}/{first}-{last}"
        run_id = f"bronze-sink-{uuid.uuid4()}"
        rows = []
        for item in batch:
            event = item.value
            ingested_at = parse_utc(event["ingested_at"])
            rows.append(
                (
                    event["event_id"],
                    parse_utc(event["event_dt"]),
                    ingested_at,
                    event["wiki_id"],
                    event["page_id"],
                    event["change_kind"],
                    json.dumps(event, separators=(",", ":")),
                    batch.partition,
                    item.offset,
                    ingested_at.date(),
                    source,
                    loaded_at,
                    run_id,
                )
            )
        con = self._con
        con.execute("DELETE FROM bronze_batch")
        con.executemany("INSERT INTO bronze_batch VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        # Deleting the offset range first makes a replayed batch (after a crash between this commit and
        # the Kafka offset commit) overwrite its earlier copy instead of duplicating it.
        con.execute("BEGIN")
        try:
            con.execute(
                f"DELETE FROM {BRONZE_TABLE} WHERE kafka_partition = ? AND kafka_offset BETWEEN ? AND ?",
                [batch.partition, first, last],
            )
            con.execute(f"INSERT INTO {BRONZE_TABLE} SELECT * FROM bronze_batch")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise


class WindowSink(HeartbeatSink):
    def __init__(self, conninfo: str, table: str, side_output: "SideOutput", heartbeat_path: Path) -> None:
        super().__init__(side_output, heartbeat_path)
        self._conninfo = conninfo
        self._table = table
        self._upsert = WINDOW_UPSERT.format(table=table)
        self._conn: psycopg.Connection | None = None
        self._days_with_partitions: set[date] = set()

    def setup(self) -> None:
        self._conn = psycopg.connect(self._conninfo)

    def write(self, batch: SinkBatch) -> None:
        rows = [item.value for item in batch]
        new_days = sorted({row["window_start"].date() for row in rows} - self._days_with_partitions)
        with self._conn.transaction():
            for day in new_days:
                self._conn.execute(
                    "SELECT ops.ensure_day_partitions(%s::regclass, %s, %s)", (self._table, day, day)
                )
            with self._conn.cursor() as cur:
                cur.executemany(self._upsert, rows)
        self._days_with_partitions.update(new_days)
