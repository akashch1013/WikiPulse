import logging
import time
from collections.abc import Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from typing import Annotated, Any, Literal

import psycopg
from fastapi import FastAPI, Path, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout
from pydantic import AwareDatetime
from starlette.exceptions import HTTPException as StarletteHTTPException

from wikipulse_api import cursor, wikis
from wikipulse_api.models import (
    ActivityBucket,
    ActivityPage,
    EditorType,
    Freshness,
    FreshnessList,
    Health,
    Readiness,
    TopViewedPage,
    TopViewedRow,
    Wiki,
    WikiPage,
)
from wikipulse_common.config import Settings, load_settings
from wikipulse_common.logging import configure_logging
from wikipulse_common.telemetry import configure_metrics, get_meter

SERVICE = "wikipulse-api"
FRESHNESS_ORDER = ("stream", "pageviews_hourly", "page_snapshot", "mediawiki_history", "marts")
WIKI_ID_PATTERN = r"^[a-z0-9_]+$"
ACCESS_METRIC = {"all": "views", "desktop": "desktop_views", "mobile-web": "mobile_web_views"}

log = logging.getLogger("wikipulse_api")


class Problem(Exception):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


def problem_response(status: int, detail: str, instance: str) -> JSONResponse:
    body = {
        "type": "about:blank",
        "title": HTTPStatus(status).phrase,
        "status": status,
        "detail": detail,
        "instance": instance,
    }
    return JSONResponse(body, status_code=status, media_type="application/problem+json")


def reader_conninfo(settings: Settings) -> str:
    pg = settings.postgres
    return make_conninfo(
        host=pg.host,
        port=pg.port,
        dbname=pg.database,
        user=pg.reader_user,
        password=pg.reader_password.get_secret_value(),
        application_name=SERVICE,
        connect_timeout=5,
        options="-c default_transaction_read_only=on -c statement_timeout=10000 -c TimeZone=UTC",
    )


def _utc(value: datetime | None) -> datetime | None:
    return value.astimezone(UTC) if value else None


def _floor(moment: datetime, grain: timedelta) -> datetime:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    return moment - (moment - epoch) % grain


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    configure_logging(SERVICE, settings.telemetry.log_level)
    configure_metrics(settings.telemetry, SERVICE)
    meter = get_meter(SERVICE)
    requests_total = meter.create_counter("http_server_requests_total", description="API responses")
    request_duration = meter.create_histogram("http_server_duration_seconds", unit="s")
    pool_wait = meter.create_histogram(
        "db_pool_wait_seconds", unit="s", description="Time to obtain a database connection"
    )

    api = settings.api
    focus_wikis = list(settings.wikis.page_level)
    grace = timedelta(seconds=settings.windows.grace_seconds)
    pool = ConnectionPool(
        reader_conninfo(settings),
        min_size=settings.postgres.pool_min_size,
        max_size=settings.postgres.pool_max_size,
        open=False,
        timeout=5,
        kwargs={"row_factory": dict_row, "autocommit": True},
        name=SERVICE,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # wait=False lets the process start and answer /healthz while the database is still unreachable;
        # /readyz reports the difference.
        pool.open(wait=False)
        yield
        pool.close()

    app = FastAPI(
        title="WikiPulse API",
        version="0.1.0",
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @contextmanager
    def connection(timeout: float = 5) -> Iterator[psycopg.Connection]:
        started = time.perf_counter()
        try:
            with pool.connection(timeout=timeout) as conn:
                pool_wait.record(time.perf_counter() - started)
                yield conn
        except (PoolTimeout, psycopg.OperationalError) as exc:
            log.warning("database unavailable", extra={"error": str(exc)})
            raise Problem(503, "The serving database is unavailable.") from exc

    def freshness(conn: psycopg.Connection, dataset: str) -> Freshness:
        row = conn.execute(
            "SELECT last_complete_period, lag_seconds FROM ops.pipeline_freshness WHERE dataset = %s",
            (dataset,),
        ).fetchone()
        return Freshness(
            source=dataset, last_complete_hour=row["last_complete_period"], lag_seconds=row["lag_seconds"]
        )

    def wiki_exists(conn: psycopg.Connection, wiki_id: str) -> bool:
        return conn.execute(
            "SELECT EXISTS (SELECT 1 FROM stream.wiki_activity_minute WHERE wiki_id = %(w)s)"
            " OR EXISTS (SELECT 1 FROM mart.wiki_activity_hourly WHERE wiki_id = %(w)s) AS found",
            {"w": wiki_id},
        ).fetchone()["found"]

    def decode_cursor(
        value: str | None, filters: dict[str, Any], types: tuple[type, ...]
    ) -> list[Any] | None:
        if value is None:
            return None
        try:
            return cursor.decode(value, filters, types)
        except cursor.InvalidCursor as exc:
            raise Problem(400, str(exc)) from exc

    @app.middleware("http")
    async def observe(request: Request, call_next):
        started = time.perf_counter()
        response = await call_next(request)
        elapsed = time.perf_counter() - started
        route = getattr(request.scope.get("route"), "path", "unmatched")
        attributes = {"route": route, "method": request.method, "status": str(response.status_code)}
        requests_total.add(1, attributes)
        request_duration.record(elapsed, {"route": route, "method": request.method})
        log.info(
            "request",
            extra={
                "method": request.method,
                "route": route,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": round(elapsed * 1000, 1),
            },
        )
        return response

    @app.exception_handler(Problem)
    async def handle_problem(request: Request, exc: Problem) -> JSONResponse:
        return problem_response(exc.status, exc.detail, request.url.path)

    @app.exception_handler(RequestValidationError)
    async def handle_validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        details = "; ".join(
            f"{'.'.join(str(part) for part in error['loc'][1:]) or error['loc'][0]}: {error['msg']}"
            for error in exc.errors()
        )
        return problem_response(400, details, request.url.path)

    @app.exception_handler(StarletteHTTPException)
    async def handle_http(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return problem_response(exc.status_code, str(exc.detail), request.url.path)

    Limit = Annotated[int, Query(ge=1, le=api.max_page_size)]
    Cursor = Annotated[str | None, Query(alias="cursor", max_length=512)]
    WikiIdPath = Annotated[str, Path(pattern=WIKI_ID_PATTERN)]

    @app.get("/healthz", response_model=Health)
    def get_health() -> Health:
        return Health()

    @app.get("/readyz", response_model=Readiness)
    def get_readiness() -> Readiness:
        with connection(timeout=2) as conn:
            conn.execute("SELECT 1")
        return Readiness()

    @app.get("/v1/freshness", response_model=FreshnessList)
    def get_freshness() -> FreshnessList:
        with connection() as conn:
            return FreshnessList(data=[freshness(conn, dataset) for dataset in FRESHNESS_ORDER])

    @app.get("/v1/wikis", response_model=WikiPage)
    def list_wikis(
        limit: Limit = api.default_page_size,
        cursor_value: Cursor = None,
        is_focus: bool | None = None,
    ) -> WikiPage:
        filters = {"is_focus": is_focus}
        after = decode_cursor(cursor_value, filters, (str,))
        conditions = [sql.SQL("TRUE")]
        params: dict[str, Any] = {"limit": limit + 1, "focus": focus_wikis}
        if is_focus is not None:
            conditions.append(sql.SQL("(wiki_id = ANY(%(focus)s)) = %(is_focus)s"))
            params["is_focus"] = is_focus
        if after:
            conditions.append(sql.SQL('wiki_id COLLATE "C" > %(after)s'))
            params["after"] = after[0]
        # Distinct wiki ids via a loose index scan: one index probe per wiki instead of reading every
        # minute row of the last week. Paging uses byte order (COLLATE "C") so cursors do not depend on the
        # database locale, which sorts "zh_yuewiki" and "zhwiki" differently.
        query = sql.SQL(
            """
            WITH RECURSIVE stream_ids AS (
                (SELECT wiki_id FROM stream.wiki_activity_minute ORDER BY wiki_id LIMIT 1)
                UNION ALL
                SELECT (SELECT m.wiki_id FROM stream.wiki_activity_minute m
                        WHERE m.wiki_id > s.wiki_id ORDER BY m.wiki_id LIMIT 1)
                FROM stream_ids s WHERE s.wiki_id IS NOT NULL
            ), mart_ids AS (
                (SELECT wiki_id FROM mart.wiki_activity_hourly ORDER BY wiki_id LIMIT 1)
                UNION ALL
                SELECT (SELECT h.wiki_id FROM mart.wiki_activity_hourly h
                        WHERE h.wiki_id > s.wiki_id ORDER BY h.wiki_id LIMIT 1)
                FROM mart_ids s WHERE s.wiki_id IS NOT NULL
            ), ids AS (
                SELECT wiki_id FROM stream_ids WHERE wiki_id IS NOT NULL
                UNION
                SELECT wiki_id FROM mart_ids WHERE wiki_id IS NOT NULL
            )
            SELECT wiki_id FROM ids WHERE {conditions} ORDER BY wiki_id COLLATE "C" LIMIT %(limit)s
            """
        ).format(conditions=sql.SQL(" AND ").join(conditions))
        with connection() as conn:
            rows = conn.execute(query, params).fetchall()
            fresh = freshness(conn, "stream")
        page = rows[:limit]
        data = []
        for row in page:
            info = wikis.describe(row["wiki_id"])
            data.append(
                Wiki(
                    wiki_id=info.wiki_id,
                    domain=info.domain,
                    project=info.project,
                    language_code=info.language_code,
                    is_focus=info.wiki_id in focus_wikis,
                )
            )
        next_cursor = cursor.encode([page[-1]["wiki_id"]], filters) if len(rows) > limit else None
        return WikiPage(data=data, next_cursor=next_cursor, freshness=fresh)

    @app.get("/v1/wikis/{wiki_id}/activity", response_model=ActivityPage)
    def get_wiki_activity(
        wiki_id: WikiIdPath,
        from_: Annotated[AwareDatetime | None, Query(alias="from")] = None,
        to: AwareDatetime | None = None,
        grain: Literal["minute", "hour"] = "hour",
        editor_type: EditorType | None = None,
        limit: Limit = api.default_page_size,
        cursor_value: Cursor = None,
    ) -> ActivityPage:
        if grain == "minute":
            table, time_column, step = (
                ("stream", "wiki_activity_minute"),
                "window_start",
                timedelta(minutes=1),
            )
            dataset, max_hours = "stream", api.max_range_hours_minute
        else:
            table, time_column, step = ("mart", "wiki_activity_hourly"), "hour_utc", timedelta(hours=1)
            dataset, max_hours = "marts", api.max_range_hours_hourly
        filters = {
            "wiki_id": wiki_id,
            "from": _utc(from_),
            "to": _utc(to),
            "grain": grain,
            "editor_type": editor_type,
        }
        # The cursor carries the resolved range so that later pages keep the window of the first page even
        # when a defaulted `to` would have moved forward with newly arrived data.
        position = decode_cursor(cursor_value, filters, (datetime, datetime, datetime, str))

        with connection() as conn:
            if not wiki_exists(conn, wiki_id):
                raise Problem(404, f"No activity has been recorded for wiki '{wiki_id}'.")
            fresh = freshness(conn, dataset)
            if position:
                range_from, range_to = position[0], position[1]
            else:
                range_to = _utc(to) or fresh.last_complete_hour or _floor(datetime.now(UTC), step)
                range_from = _utc(from_) or range_to - timedelta(hours=24)
                if range_from >= range_to:
                    raise Problem(422, "`from` must be earlier than `to`.")
                if range_to - range_from > timedelta(hours=max_hours):
                    raise Problem(422, f"The time range for {grain} grain is limited to {max_hours} hours.")

            column = sql.Identifier(time_column)
            conditions = [
                sql.SQL("wiki_id = %(wiki_id)s"),
                sql.SQL("{c} >= %(range_from)s").format(c=column),
                sql.SQL("{c} < %(range_to)s").format(c=column),
            ]
            params: dict[str, Any] = {
                "wiki_id": wiki_id,
                "range_from": range_from,
                "range_to": range_to,
                "limit": limit + 1,
            }
            if editor_type:
                conditions.append(sql.SQL("editor_type = %(editor_type)s"))
                params["editor_type"] = editor_type
            if position:
                conditions.append(
                    sql.SQL("({c}, editor_type) > (%(after_start)s, %(after_type)s)").format(c=column)
                )
                params["after_start"], params["after_type"] = position[2], position[3]
            query = sql.SQL(
                "SELECT {c} AS bucket_start, editor_type, edits, creates, moves, deletes, reverts,"
                " distinct_pages, distinct_editors FROM {table} WHERE {where}"
                " ORDER BY {c}, editor_type LIMIT %(limit)s"
            ).format(c=column, table=sql.Identifier(*table), where=sql.SQL(" AND ").join(conditions))
            rows = conn.execute(query, params).fetchall()

        now = datetime.now(UTC)
        page = rows[:limit]
        data = [ActivityBucket(**row, is_final=row["bucket_start"] + step + grace <= now) for row in page]
        next_cursor = None
        if len(rows) > limit:
            last = page[-1]
            next_cursor = cursor.encode(
                [range_from, range_to, last["bucket_start"], last["editor_type"]], filters
            )
        return ActivityPage(data=data, next_cursor=next_cursor, freshness=fresh)

    @app.get("/v1/pages/top-viewed", response_model=TopViewedPage)
    def list_top_viewed_pages(
        wiki_id: Annotated[str, Query(pattern=WIKI_ID_PATTERN)],
        hour: AwareDatetime,
        namespace: int | None = None,
        access_method: Literal["all", "desktop", "mobile-web"] = "all",
        limit: Limit = api.default_page_size,
        cursor_value: Cursor = None,
    ) -> TopViewedPage:
        hour = hour.astimezone(UTC)
        if hour != _floor(hour, timedelta(hours=1)):
            raise Problem(400, "`hour` must be the start of an hour, e.g. 2026-10-27T12:00:00Z.")
        if wiki_id not in focus_wikis:
            raise Problem(404, f"Page-level data is available for {', '.join(focus_wikis)} only.")
        filters = {"wiki_id": wiki_id, "hour": hour, "namespace": namespace, "access_method": access_method}
        position = decode_cursor(cursor_value, filters, (int, int))

        metric = sql.Identifier("p", ACCESS_METRIC[access_method])
        conditions = [
            sql.SQL("p.wiki_id = %(wiki_id)s"),
            sql.SQL("p.hour_utc = %(hour)s"),
            sql.SQL("{m} > 0").format(m=metric),
        ]
        params: dict[str, Any] = {"wiki_id": wiki_id, "hour": hour, "limit": limit + 1}
        if namespace is not None:
            conditions.append(sql.SQL("p.namespace_id = %(namespace)s"))
            params["namespace"] = namespace
        if position:
            conditions.append(
                sql.SQL(
                    "({m} < %(after_metric)s OR ({m} = %(after_metric)s AND p.page_id > %(after_page)s))"
                ).format(m=metric)
            )
            params["after_metric"], params["after_page"] = position
        # Titles come from the version valid at the hour when history is published, otherwise the current one.
        query = sql.SQL(
            """
            SELECT p.page_id, COALESCE(th.title, pc.title) AS title, p.namespace_id, p.hour_utc,
                   p.views, p.desktop_views, p.mobile_web_views, {m} AS metric
            FROM mart.page_activity_hourly p
            JOIN mart.page_current pc ON pc.wiki_id = p.wiki_id AND pc.page_id = p.page_id
            LEFT JOIN mart.page_title_history th
              ON th.wiki_id = p.wiki_id AND th.page_id = p.page_id
             AND th.valid_from <= p.hour_utc AND p.hour_utc < th.valid_to
            WHERE {where}
            ORDER BY {m} DESC, p.page_id
            LIMIT %(limit)s
            """
        ).format(m=metric, where=sql.SQL(" AND ").join(conditions))
        with connection() as conn:
            rows = conn.execute(query, params).fetchall()
            fresh = freshness(conn, "marts")
        page = rows[:limit]
        data = [
            TopViewedRow(
                wiki_id=wiki_id,
                page_id=row["page_id"],
                title=row["title"],
                namespace=row["namespace_id"],
                hour_utc=row["hour_utc"],
                views=row["views"],
                desktop_views=row["desktop_views"],
                mobile_web_views=row["mobile_web_views"],
            )
            for row in page
        ]
        next_cursor = None
        if len(rows) > limit:
            next_cursor = cursor.encode([page[-1]["metric"], page[-1]["page_id"]], filters)
        return TopViewedPage(data=data, next_cursor=next_cursor, freshness=fresh)

    return app


app = create_app()
