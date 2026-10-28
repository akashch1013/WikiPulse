import base64
import hashlib
import json
from datetime import datetime
from typing import Any


class InvalidCursor(ValueError):
    pass


def _filters_digest(filters: dict[str, Any]) -> str:
    canonical = json.dumps(filters, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def encode(key: list[Any], filters: dict[str, Any]) -> str:
    payload = {
        "k": [v.isoformat() if isinstance(v, datetime) else v for v in key],
        "f": _filters_digest(filters),
    }
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def decode(cursor: str, filters: dict[str, Any], types: tuple[type, ...]) -> list[Any]:
    # The filter digest pins a cursor to the query that produced it; reusing it with other filters would
    # silently skip or repeat rows.
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded.encode()))
        values = payload["k"]
        digest = payload["f"]
    except (ValueError, KeyError, TypeError) as exc:
        raise InvalidCursor("cursor is not a value returned by this API") from exc
    if digest != _filters_digest(filters):
        raise InvalidCursor("cursor was produced with different filters")
    if not isinstance(values, list) or len(values) != len(types):
        raise InvalidCursor("cursor is not a value returned by this API")
    decoded = []
    for value, kind in zip(values, types, strict=True):
        try:
            decoded.append(datetime.fromisoformat(value) if kind is datetime else kind(value))
        except (ValueError, TypeError) as exc:
            raise InvalidCursor("cursor is not a value returned by this API") from exc
    return decoded
