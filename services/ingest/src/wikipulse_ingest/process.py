import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from jsonschema import ValidationError

from wikipulse_ingest.schemas import SchemaMismatch, UnknownSchemaMajor, UpstreamSchemas
from wikipulse_ingest.transform import format_utc, parse_utc, scrub, to_page_change_event

DLQ_PAYLOAD_LIMIT = 900_000


@dataclass(frozen=True)
class Accepted:
    value: dict[str, Any]
    key: str
    timestamp_ms: int
    schema_drift: bool


@dataclass(frozen=True)
class DeadLetter:
    value: dict[str, Any]


@dataclass(frozen=True)
class Canary:
    pass


Outcome = Accepted | DeadLetter | Canary


def _truncate(text: str, limit: int = DLQ_PAYLOAD_LIMIT) -> str:
    encoded = text.encode()
    if len(encoded) <= limit:
        return text
    return encoded[:limit].decode(errors="ignore")


class EventProcessor:
    def __init__(self, schemas: UpstreamSchemas, hmac_key: bytes, max_payload_bytes: int) -> None:
        self._schemas = schemas
        self._hmac_key = hmac_key
        self._max_payload_bytes = max_payload_bytes

    def process(self, data: str, sse_position: str, received_at: datetime) -> Outcome:
        try:
            event = json.loads(data)
        except json.JSONDecodeError as exc:
            return self._dead(received_at, "json_decode_error", str(exc), None, sse_position, data)
        if not isinstance(event, dict):
            return self._dead(
                received_at, "json_decode_error", "event is not a JSON object", None, sse_position, data
            )

        declared = event.get("$schema")
        if len(data.encode()) > self._max_payload_bytes:
            return self._dead_event(
                received_at, "oversize", f"{len(data.encode())} bytes", event, sse_position
            )
        if (event.get("meta") or {}).get("domain") == "canary":
            return Canary()
        if not (event.get("page") or {}).get("page_id"):
            return self._dead_event(
                received_at, "missing_page_id", "page.page_id absent or 0", event, sse_position
            )

        try:
            validator, drift = self._schemas.resolve(str(declared))
        except UnknownSchemaMajor as exc:
            return self._dead_event(received_at, "unknown_schema_major", str(exc), event, sse_position)
        except SchemaMismatch as exc:
            return self._dead_event(received_at, "schema_invalid", str(exc), event, sse_position)
        error = next(iter(validator.iter_errors(event)), None)
        if error is not None:
            return self._dead_event(received_at, "schema_invalid", _describe(error), event, sse_position)

        value = to_page_change_event(
            event, sse_position=sse_position, ingested_at=received_at, hmac_key=self._hmac_key
        )
        return Accepted(
            value=value,
            key=f"{value['wiki_id']}:{value['page_id']}",
            timestamp_ms=int(parse_utc(event["dt"]).timestamp() * 1000),
            schema_drift=drift,
        )

    def _dead_event(
        self, received_at: datetime, reason: str, detail: str, event: dict[str, Any], position: str
    ) -> DeadLetter:
        payload = json.dumps(scrub(event), ensure_ascii=False, separators=(",", ":"))
        return self._dead(received_at, reason, detail, event.get("$schema"), position, payload)

    @staticmethod
    def _dead(
        received_at: datetime, reason: str, detail: str, schema: Any, position: str, payload: str
    ) -> DeadLetter:
        return DeadLetter(
            {
                "failed_at": format_utc(received_at),
                "stage": "ingest",
                "reason_code": reason,
                "reason_detail": detail[:4000],
                "upstream_schema": schema if isinstance(schema, str) else None,
                "source_position": position,
                "payload": _truncate(payload),
            }
        )


def _describe(error: ValidationError) -> str:
    pointer = "/" + "/".join(str(part) for part in error.absolute_path)
    return f"{pointer}: {error.message}"
