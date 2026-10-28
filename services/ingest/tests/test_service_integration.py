import json
import os
import threading
import time
import uuid
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import psycopg
import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from wikipulse_common.config import load_settings
from wikipulse_common.kafka import producer_config
from wikipulse_common.telemetry import get_meter
from wikipulse_ingest.checkpoint import Checkpoint, CheckpointStore, position_timestamp
from wikipulse_ingest.process import Accepted, EventProcessor
from wikipulse_ingest.schemas import UpstreamSchemas
from wikipulse_ingest.service import Ingestor

pytestmark = pytest.mark.skipif(
    not os.environ.get("WIKIPULSE_POSTGRES__PASSWORD"), reason="needs the compose Kafka and PostgreSQL"
)

EVENT_COUNT = 400


class ReplayServer:
    """EventStreams stand-in: replays fixture events, resumes by timestamp, closes every `per_connection`."""

    def __init__(self, lines: list[dict], per_connection: int) -> None:
        self.lines = lines
        self.per_connection = per_connection
        self.requested_ids: list[str | None] = []
        server = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: object) -> None:
                return

            def do_GET(self) -> None:
                last_id = self.headers.get("Last-Event-ID")
                server.requested_ids.append(last_id)
                start = server.resume_index(last_id)
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                for line in server.lines[start : start + server.per_connection]:
                    body = f"id: {line['sse_id']}\ndata: {json.dumps(line['event'])}\n\n"
                    self.wfile.write(body.encode())
                self.wfile.flush()

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_port}/v2/stream/mediawiki.page_change.v1"
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def resume_index(self, last_id: str | None) -> int:
        # Like EventStreams with timestamp positions: start at the first event at or after the
        # requested time, which replays the checkpointed event itself.
        if last_id is None:
            return 0
        requested = position_timestamp(last_id)
        for index, line in enumerate(self.lines):
            if position_timestamp(line["sse_id"]) >= requested:
                return index
        return len(self.lines)

    def close(self) -> None:
        self.httpd.shutdown()


class CrashingProducer:
    def __init__(self, inner: Producer, crash_after: int) -> None:
        self._inner = inner
        self._remaining = crash_after

    def produce(self, topic: str, **kwargs: object) -> None:
        if self._remaining == 0:
            raise RuntimeError("simulated crash")
        self._remaining -= 1
        self._inner.produce(topic, **kwargs)

    def poll(self, timeout: float) -> int:
        return self._inner.poll(timeout)

    def flush(self, timeout: float) -> int:
        return self._inner.flush(timeout)


@pytest.fixture
def env(monkeypatch, fixture_lines, hmac_key):
    run = uuid.uuid4().hex[:10]
    server = ReplayServer(fixture_lines[:EVENT_COUNT], per_connection=90)
    topic, dlq = f"wikipulse.test.{run}", f"wikipulse.test.{run}.dlq"
    monkeypatch.setenv("WIKIPULSE_STREAM__URL", server.url)
    monkeypatch.setenv("WIKIPULSE_STREAM__TOPIC", topic)
    monkeypatch.setenv("WIKIPULSE_STREAM__DLQ_TOPIC", dlq)
    monkeypatch.setenv("WIKIPULSE_STREAM__CHECKPOINT_INTERVAL_EVENTS", "50")
    monkeypatch.setenv("WIKIPULSE_STREAM__RECONNECT_BACKOFF_MIN_SECONDS", "0.05")
    monkeypatch.setenv("WIKIPULSE_STREAM__RECONNECT_BACKOFF_MAX_SECONDS", "0.2")
    monkeypatch.setenv(
        "WIKIPULSE_HTTP__CONTACT", os.environ.get("WIKIPULSE_HTTP__CONTACT") or "test@example.org"
    )
    settings = load_settings()

    admin = AdminClient({"bootstrap.servers": settings.kafka.bootstrap_servers})
    for future in admin.create_topics([NewTopic(topic, 4, 1), NewTopic(dlq, 1, 1)]).values():
        future.result()
    pg = settings.postgres
    connection = psycopg.connect(
        host=pg.host,
        port=pg.port,
        dbname=pg.database,
        user=pg.user,
        password=pg.password.get_secret_value(),
        autocommit=True,
    )
    consumer_name = f"test:{run}"
    processor = EventProcessor(UpstreamSchemas(), hmac_key, settings.stream.max_payload_bytes)
    expected = {
        o.value["event_id"]
        for o in (
            processor.process(json.dumps(line["event"]), line["sse_id"], datetime.now(UTC))
            for line in server.lines
        )
        if isinstance(o, Accepted)
    }
    yield {
        "settings": settings,
        "server": server,
        "topic": topic,
        "store": CheckpointStore(connection, consumer_name),
        "processor": processor,
        "expected": expected,
    }
    server.close()
    for future in admin.delete_topics([topic, dlq]).values():
        future.result()
    connection.execute("DELETE FROM ops.stream_checkpoint WHERE consumer = %s", (consumer_name,))
    connection.execute("DELETE FROM ops.stream_gaps WHERE consumer = %s", (consumer_name,))
    connection.close()


def _ingestor(env, producer, tmp_path) -> Ingestor:
    return Ingestor(
        env["settings"],
        store=env["store"],
        producer=producer,
        processor=env["processor"],
        client=httpx.Client(timeout=10),
        meter=get_meter("ingest-test"),
        heartbeat=tmp_path / "heartbeat",
    )


def _producer(env) -> Producer:
    return Producer(producer_config(env["settings"].kafka, client_id="ingest-test"))


def _run_until_last_event_checkpointed(ingestor: Ingestor, env, timeout: float = 60) -> None:
    last = json.loads(env["server"].lines[-1]["sse_id"])
    stop = threading.Event()
    errors: list[BaseException] = []

    def target() -> None:
        try:
            ingestor.run(stop)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=target)
    thread.start()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not errors:
        saved = env["store"].load()
        if saved is not None and json.loads(saved.last_event_id) == last:
            break
        time.sleep(0.2)
    stop.set()
    thread.join(timeout=20)
    assert not errors, errors
    saved = env["store"].load()
    assert saved is not None and json.loads(saved.last_event_id) == last


def _consume_event_ids(env) -> list[str]:
    settings = env["settings"]
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka.bootstrap_servers,
            "group.id": f"verify-{uuid.uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    consumer.assign([TopicPartition(env["topic"], p, 0) for p in range(4)])
    ids, idle_since = [], time.monotonic()
    while time.monotonic() - idle_since < 3:
        message = consumer.poll(0.5)
        if message is None or message.error():
            continue
        ids.append(json.loads(message.value())["event_id"])
        idle_since = time.monotonic()
    consumer.close()
    return ids


def test_reconnects_lose_nothing_and_checkpoint_trails_acks(env, tmp_path):
    ingestor = _ingestor(env, _producer(env), tmp_path)
    saves: list[int] = []
    original_save = env["store"].save

    def recording_save(checkpoint: Checkpoint) -> None:
        saves.append(ingestor.unacknowledged)
        original_save(checkpoint)

    env["store"].save = recording_save
    _run_until_last_event_checkpointed(ingestor, env)

    consumed = _consume_event_ids(env)
    assert env["expected"] <= set(consumed)
    assert sum(1 for request in env["server"].requested_ids if request is not None) >= 4
    assert saves and all(pending == 0 for pending in saves)
    # Clean server closes are checkpointed first, so each reconnect replays at most the boundary event.
    assert len(consumed) - len(set(consumed)) <= len(env["server"].requested_ids)
    assert (tmp_path / "heartbeat").exists()


def test_restart_after_crash_resumes_from_checkpoint(env, tmp_path):
    crashing = _ingestor(env, CrashingProducer(_producer(env), crash_after=150), tmp_path)
    with pytest.raises(RuntimeError, match="simulated crash"):
        crashing.run(threading.Event())
    saved = env["store"].load()
    assert saved is not None

    _run_until_last_event_checkpointed(_ingestor(env, _producer(env), tmp_path), env)
    assert env["expected"] <= set(_consume_event_ids(env))


def test_resume_beyond_retention_records_gap(env, tmp_path):
    first = json.loads(env["server"].lines[0]["sse_id"])
    expired = [
        dict(entry, timestamp=entry["timestamp"] - 600_000) if "timestamp" in entry else entry
        for entry in first
    ]
    env["store"].save(Checkpoint(json.dumps(expired), datetime.fromtimestamp(0, UTC), events_acked=0))
    _run_until_last_event_checkpointed(_ingestor(env, _producer(env), tmp_path), env)

    gaps = (
        env["store"]
        ._conn.execute(
            "SELECT gap_start, gap_end, reason FROM ops.stream_gaps WHERE consumer = %s",
            (env["store"].consumer,),
        )
        .fetchall()
    )
    assert len(gaps) == 1
    start, end, reason = gaps[0]
    assert reason == "retention_exceeded"
    assert (end - start).total_seconds() == pytest.approx(600, abs=1)
