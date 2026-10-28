import copy
import hashlib
import hmac
import ipaddress
from datetime import UTC, datetime
from typing import Any

REVERT_FIELDS = ("method", "is_exact", "rev_original_id", "rev_reverted_oldest_id", "rev_reverted_newest_id")


def parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)


def format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _is_ip(text: str | None) -> bool:
    if not text:
        return False
    try:
        ipaddress.ip_address(text)
    except ValueError:
        return False
    return True


def editor_type(performer: dict[str, Any]) -> str:
    if performer.get("is_system"):
        return "system"
    if performer.get("is_bot"):
        return "bot"
    if performer.get("is_temp"):
        return "temp"
    if not performer.get("user_id") and _is_ip(performer.get("user_text")):
        return "anonymous_ip"
    return "registered"


def editor_key(hmac_key: bytes, wiki_id: str, performer: dict[str, Any]) -> str | None:
    if performer.get("is_system"):
        return None
    user_id = performer.get("user_id")
    identity = str(user_id) if user_id else performer.get("user_text")
    if not identity:
        return None
    digest = hmac.new(hmac_key, f"{wiki_id}:{identity}".encode(), hashlib.sha256).digest()
    return digest[:16].hex()


def strip_namespace(title: str, namespace_id: int) -> str:
    # Upstream titles carry the localised namespace prefix ("User:Example/sandbox"); main-namespace
    # titles may legitimately contain a colon, so only non-zero namespaces are split.
    if namespace_id == 0:
        return title
    _, sep, rest = title.partition(":")
    return rest if sep else title


def to_page_change_event(
    event: dict[str, Any], *, sse_position: str, ingested_at: datetime, hmac_key: bytes
) -> dict[str, Any]:
    page = event["page"]
    revision = event.get("revision") or {}
    performer = event.get("performer") or {}
    wiki_id = event["wiki_id"]
    change_kind = event["page_change_kind"]

    prior_title = prior_namespace = None
    prior_page = (event.get("prior_state") or {}).get("page")
    if change_kind == "move" and prior_page:
        # prior_state lists only the fields the move changed; an absent namespace means it was kept.
        prior_namespace = prior_page.get("namespace_id", page["namespace_id"])
        prior_title = strip_namespace(prior_page.get("page_title", page["page_title"]), prior_namespace)

    revert = revision.get("revert")
    rev_dt = revision.get("rev_dt")
    return {
        "event_id": event["meta"]["id"],
        "event_dt": format_utc(parse_utc(event["dt"])),
        "ingested_at": format_utc(ingested_at),
        "upstream_schema": event["$schema"],
        "sse_position": sse_position,
        "wiki_id": wiki_id,
        "change_kind": change_kind,
        "page_id": page["page_id"],
        "page_title": strip_namespace(page["page_title"], page["namespace_id"]),
        "namespace_id": page["namespace_id"],
        "is_redirect": page["is_redirect"],
        "wikibase_item_id": page.get("wikibase_item_id"),
        "prior_page_title": prior_title,
        "prior_namespace_id": prior_namespace,
        "rev_id": revision.get("rev_id"),
        "rev_parent_id": revision.get("rev_parent_id"),
        "rev_dt": format_utc(parse_utc(rev_dt)) if rev_dt else None,
        "rev_sha1": revision.get("rev_sha1"),
        "rev_size": revision.get("rev_size"),
        "is_minor_edit": revision.get("is_minor_edit"),
        "revert": {key: revert.get(key) for key in REVERT_FIELDS} if revert else None,
        "editor_type": editor_type(performer),
        "editor_key": editor_key(hmac_key, wiki_id, performer),
    }


def scrub(event: dict[str, Any]) -> dict[str, Any]:
    """Remove user identity and comments from an upstream event before it is written to the DLQ."""
    cleaned = copy.deepcopy(event)
    cleaned.pop("performer", None)
    cleaned.pop("comment", None)
    revisions = [cleaned.get("revision"), (cleaned.get("prior_state") or {}).get("revision")]
    for revision in revisions:
        if isinstance(revision, dict):
            for field in ("editor", "comment"):
                revision.pop(field, None)
    return cleaned
