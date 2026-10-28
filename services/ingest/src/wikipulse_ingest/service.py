import json
import logging
import random
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import httpx
from confluent_kafka import KafkaError, Message
from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

from wikipulse_common.config import Settings
from wikipulse_common.http import user_agent
from wikipulse_ingest.checkpoint import Checkpoint, CheckpointStore, position_timestamp
from wikipulse_ingest.process import Accepted, Canary, DeadLetter, EventProcessor
from wikipulse_ingest.sse import ServerSentEvent, StreamHttpError, read_event_stream

log = logging.getLogger(__name__)

HEARTBEAT_FILE = Path("/tmp/wikipulse-ingest.heartbeat")
GAP_TOLERANCE = timedelta(seconds=60)
FLUSH_TIMEOUT_SECONDS = 30


class DeliveryFailed(RuntimeError):
    pass


class KafkaProducer(Protocol):
    def produce(self, topic: str, **kwargs: Any) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


class IngestMetrics:
    def __init__(self, meter: metrics.Meter, last_event_dt: Callable[[], datetime | None]) -> None:
        self.received = meter.create_counter(
            "events_received_total", description="Events read from EventStreams"
        )
        self.produced = meter.create_counter(
            "events_produced_total", description="Events acknowledged by Kafka"
        )
        self.dlq = meter.create_counter("dlq_total", description="Events sent to the dead-letter topic")
        self.drift = meter.create_counter(
            "schema_drift_total", description="Events with an unvendored minor schema"
        )
        self.canary = meter.create_counter(
            "canary_dropped_total", description="Wikimedia canary events dropped"
        )
        self.reconnects = meter.create_counter(
            "sse_reconnects_total", description="EventStreams reconnections"
        )

        def observe_age(_: CallbackOptions) -> list[Observation]:
            last = last_event_dt()
            return [Observation((datetime.now(UTC) - last).total_seconds())] if last else []

        meter.create_observable_gauge(
            "ingest_last_event_age_seconds",
            callbacks=[observe_age],
            unit="s",
            description="Age of the newest event",
        )


class Ingestor:
    def __init__(
        self,
        settings: Settings,
        *,
        store: CheckpointStore,
        producer: KafkaProducer,
        processor: EventProcessor,
        client: httpx.Client,
        meter: metrics.Meter,
        heartbeat: Path = HEARTBEAT_FILE,
    ) -> None:
        self._stream = settings.stream
        self._user_agent = user_agent(settings.http)
        self._store = store
        self._producer = producer
        self._processor = processor
        self._client = client
        self._heartbeat = heartbeat
        self.metrics = IngestMetrics(meter, lambda: self._last_event_dt)

        self._last_sse_id: str | None = None
        self._last_event_dt: datetime | None = None
        self._saved_sse_id: str | None = None
        self._produced = 0
        self._acked = 0
        self._acked_total = 0
        self._acked_at_save = 0
        self._delivery_error: KafkaError | None = None
        self._since_checkpoint = 0
        self._last_checkpoint_at = time.monotonic()

    @property
    def unacknowledged(self) -> int:
        return self._produced - self._acked

    def run(self, stop: threading.Event) -> None:
        checkpoint = self._store.load()
        if checkpoint is not None:
            self._last_sse_id = self._saved_sse_id = checkpoint.last_event_id
            self._last_event_dt = checkpoint.last_event_dt
            self._acked_total = checkpoint.events_acked
            log.info("resuming", extra={"last_event_id": checkpoint.last_event_id})

        delay = self._stream.reconnect_backoff_min_seconds
        while not stop.is_set():
            requested = self._saved_sse_id
            try:
                self._read(requested, stop)
                delay = self._stream.reconnect_backoff_min_seconds
            except StreamHttpError as exc:
                log.warning("stream http error", extra={"status": exc.status})
                delay = max(exc.retry_after or 0.0, self._next_delay(delay))
            except httpx.HTTPError as exc:
                log.warning("stream connection error", extra={"error": repr(exc)})
                delay = self._next_delay(delay)
            # Clean server closes happen every few minutes; checkpointing before reconnecting means the
            # resume position is the last event read, so a normal reconnect replays nothing.
            self.checkpoint(force=True)
            if stop.is_set():
                break
            self.metrics.reconnects.add(1)
            stop.wait(random.uniform(0, delay))
        self.checkpoint(force=True)

    def _next_delay(self, delay: float) -> float:
        return min(
            max(delay * 2, self._stream.reconnect_backoff_min_seconds),
            self._stream.reconnect_backoff_max_seconds,
        )

    def _read(self, requested: str | None, stop: threading.Event) -> None:
        check_gap = requested is not None
        for sse in read_event_stream(self._client, self._stream.url, self._user_agent, requested):
            if check_gap and sse.id is not None:
                self._detect_gap(requested, sse.id)
                check_gap = False
            self._handle(sse)
            if self._checkpoint_due():
                self.checkpoint()
            if stop.is_set():
                return

    def _detect_gap(self, requested: str, delivered: str) -> None:
        requested_at = position_timestamp(requested)
        delivered_at = position_timestamp(delivered)
        if requested_at is None or delivered_at is None:
            return
        if delivered_at - requested_at > GAP_TOLERANCE:
            # EventStreams resumes from the earliest retained offset when the requested position has
            # expired, so a large jump means the events in between can no longer be fetched.
            self._store.record_gap(requested_at, delivered_at, "retention_exceeded")
            log.warning(
                "stream gap",
                extra={"gap_start": requested_at.isoformat(), "gap_end": delivered_at.isoformat()},
            )

    def _handle(self, sse: ServerSentEvent) -> None:
        if sse.id is None:
            return
        received_at = datetime.now(UTC)
        outcome = self._processor.process(sse.data, sse.id, received_at)
        match outcome:
            case Canary():
                self.metrics.canary.add(1)
            case DeadLetter(value=value):
                self.metrics.dlq.add(1, {"reason_code": value["reason_code"]})
                self._produce(self._stream.dlq_topic, None, value, None, {})
            case Accepted(value=value, key=key, timestamp_ms=timestamp_ms, schema_drift=drift):
                self.metrics.received.add(1, {"wiki_id": value["wiki_id"]})
                if drift:
                    self.metrics.drift.add(1)
                    log.warning("schema drift", extra={"upstream_schema": value["upstream_schema"]})
                headers = {"upstream_schema": value["upstream_schema"], "sse_position": sse.id}
                self._produce(self._stream.topic, key, value, timestamp_ms, headers)
                self._last_event_dt = datetime.fromtimestamp(timestamp_ms / 1000, UTC)
        self._last_sse_id = sse.id
        self._since_checkpoint += 1
        if self._delivery_error is not None:
            raise DeliveryFailed(str(self._delivery_error))

    def _produce(
        self,
        topic: str,
        key: str | None,
        value: dict[str, Any],
        timestamp_ms: int | None,
        headers: dict[str, str],
    ) -> None:
        kwargs: dict[str, Any] = {
            "key": key.encode() if key else None,
            "value": json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(),
            "headers": [(name, text.encode()) for name, text in headers.items()],
            "on_delivery": self._on_delivery,
        }
        if timestamp_ms is not None:
            kwargs["timestamp"] = timestamp_ms
        while True:
            try:
                self._producer.produce(topic, **kwargs)
                break
            except BufferError:
                self._producer.poll(1.0)
        self._produced += 1
        self._producer.poll(0)

    def _on_delivery(self, error: KafkaError | None, message: Message) -> None:
        if error is not None:
            self._delivery_error = error
            log.error("kafka delivery failed", extra={"error": str(error), "topic": message.topic()})
            return
        self._acked += 1
        if message.topic() == self._stream.topic:
            self.metrics.produced.add(1)

    def _checkpoint_due(self) -> bool:
        return (
            self._since_checkpoint >= self._stream.checkpoint_interval_events
            or time.monotonic() - self._last_checkpoint_at >= self._stream.checkpoint_interval_seconds
        )

    def checkpoint(self, force: bool = False) -> None:
        if self._last_sse_id is None or self._last_sse_id == self._saved_sse_id:
            return
        if not force and not self._checkpoint_due():
            return
        remaining = self._producer.flush(FLUSH_TIMEOUT_SECONDS)
        if remaining or self._delivery_error is not None or self._acked != self._produced:
            raise DeliveryFailed(
                f"{remaining} messages unacknowledged, delivery error: {self._delivery_error}"
            )
        self._acked_total += self._acked - self._acked_at_save
        self._acked_at_save = self._acked
        self._store.save(
            Checkpoint(
                last_event_id=self._last_sse_id,
                last_event_dt=self._last_event_dt or datetime.now(UTC),
                events_acked=self._acked_total,
            )
        )
        self._saved_sse_id = self._last_sse_id
        self._since_checkpoint = 0
        self._last_checkpoint_at = time.monotonic()
        self._heartbeat.touch()
