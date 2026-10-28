from datetime import datetime
from typing import Literal

from pydantic import BaseModel

EditorType = Literal["bot", "temp", "registered", "anonymous_ip", "system"]
FreshnessSource = Literal["stream", "pageviews_hourly", "page_snapshot", "mediawiki_history", "marts"]


class Health(BaseModel):
    status: Literal["ok"] = "ok"


class Readiness(BaseModel):
    status: Literal["ready"] = "ready"


class Freshness(BaseModel):
    source: FreshnessSource
    last_complete_hour: datetime | None
    lag_seconds: int | None


class FreshnessList(BaseModel):
    data: list[Freshness]


class Wiki(BaseModel):
    wiki_id: str
    domain: str
    project: str
    language_code: str | None
    is_focus: bool


class ActivityBucket(BaseModel):
    bucket_start: datetime
    editor_type: EditorType
    edits: int
    creates: int
    moves: int
    deletes: int
    reverts: int
    distinct_pages: int
    distinct_editors: int
    is_final: bool


class TopViewedRow(BaseModel):
    wiki_id: str
    page_id: int
    title: str
    namespace: int
    hour_utc: datetime
    views: int
    desktop_views: int
    mobile_web_views: int


class WikiPage(BaseModel):
    data: list[Wiki]
    next_cursor: str | None
    freshness: Freshness


class ActivityPage(BaseModel):
    data: list[ActivityBucket]
    next_cursor: str | None
    freshness: Freshness


class TopViewedPage(BaseModel):
    data: list[TopViewedRow]
    next_cursor: str | None
    freshness: Freshness
