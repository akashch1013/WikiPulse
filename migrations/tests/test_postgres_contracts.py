import os
from pathlib import Path

import psycopg
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CONTRACTS = [
    yaml.safe_load(p.read_text()) for p in sorted((ROOT / "docs" / "contracts" / "tables").glob("*.yaml"))
]
POSTGRES_CONTRACTS = [c for c in CONTRACTS if c["store"].startswith("postgres:")]

# Contract type names that format_type() spells differently.
TYPE_ALIASES = {
    "varchar": "character varying",
    "timestamptz": "timestamp with time zone",
    "bigserial": "bigint",
}


def connect(user: str, password_env: str) -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ.get("WIKIPULSE_POSTGRES__HOST", "127.0.0.1"),
        port=int(os.environ.get("WIKIPULSE_POSTGRES__PORT", "55432")),
        dbname="wikipulse",
        user=user,
        password=os.environ[password_env],
    )


@pytest.fixture(scope="module")
def conn():
    with connect("wikipulse", "WIKIPULSE_POSTGRES__PASSWORD") as c:
        yield c


def relation_columns(conn, name):
    rows = conn.execute(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, c.relkind
        FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
        WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (name,),
    ).fetchall()
    return rows


@pytest.mark.parametrize("contract", POSTGRES_CONTRACTS, ids=lambda c: c["name"])
def test_columns_match_contract(conn, contract):
    rows = relation_columns(conn, contract["name"])
    is_view = contract.get("kind") == "view"
    assert (rows[0][3] == "v") == is_view, "contract kind and relation kind differ"
    actual = {name: (type_, not_null) for name, type_, not_null, _ in rows}
    expected = {
        c["name"]: (TYPE_ALIASES.get(c["type"], c["type"]), not c["nullable"]) for c in contract["columns"]
    }
    if is_view:
        # Views carry no NOT NULL constraints; only names and types are comparable.
        actual = {k: v[0] for k, v in actual.items()}
        expected = {k: v[0] for k, v in expected.items()}
    assert actual == expected


@pytest.mark.parametrize(
    "contract",
    [c for c in POSTGRES_CONTRACTS if c.get("kind") != "view"],
    ids=lambda c: c["name"],
)
def test_primary_key_matches_contract(conn, contract):
    rows = conn.execute(
        """
        SELECT a.attname
        FROM pg_index i
        JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, position) ON true
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
        WHERE i.indrelid = %s::regclass AND i.indisprimary
        ORDER BY k.position
        """,
        (contract["name"],),
    ).fetchall()
    assert [r[0] for r in rows] == contract["primary_key"]


def test_stream_insert_outside_partitions_fails_until_day_partition_created(conn):
    row = (
        "enwiki",
        "2001-01-15 10:00+00",
        "2001-01-15 10:01+00",
        "registered",
        1,
        0,
        0,
        0,
        0,
        1,
        1,
        "2001-01-15 10:06+00",
    )
    insert = "INSERT INTO stream.wiki_activity_minute VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    ensure = "SELECT ops.ensure_day_partitions('stream.wiki_activity_minute', '2001-01-15', '2001-01-15')"
    with conn.transaction(force_rollback=True):
        with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
            conn.execute(insert, row)
        assert conn.execute(ensure).fetchone()[0] == 1
        conn.execute(insert, row)
        assert conn.execute(ensure).fetchone()[0] == 0
        assert conn.execute("SELECT to_regclass('stream.wiki_activity_minute_p20010115')").fetchone()[0]


def test_stream_tables_have_day_partitions_around_today(conn):
    for parent in ("stream.wiki_activity_minute", "stream.page_activity_window"):
        bounds = conn.execute(
            """
            SELECT min(pg_get_expr(c.relpartbound, c.oid)), count(*)
            FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
            WHERE i.inhparent = %s::regclass
            """,
            (parent,),
        ).fetchone()
        assert bounds[1] >= 11, parent


def test_drop_day_partitions_before_cutoff(conn):
    with conn.transaction(force_rollback=True):
        conn.execute(
            "SELECT ops.ensure_day_partitions('stream.page_activity_window', '2001-01-01', '2001-01-03')"
        )
        dropped = conn.execute(
            "SELECT ops.drop_day_partitions_before('stream.page_activity_window', '2001-01-03')"
        ).fetchone()[0]
        assert dropped == 2
        assert (
            conn.execute("SELECT to_regclass('stream.page_activity_window_p20010102')").fetchone()[0] is None
        )
        assert conn.execute("SELECT to_regclass('stream.page_activity_window_p20010103')").fetchone()[0]


def test_mart_insert_outside_partitions_fails_until_month_partition_created(conn):
    row = ("enwiki", "2001-01-15 10:00+00", "registered", 1, 0, 0, 0, 0, 1, 1)
    insert = "INSERT INTO mart.wiki_activity_hourly VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    ensure = "SELECT ops.ensure_month_partitions('mart.wiki_activity_hourly', '2001-01-01', '2001-01-31')"
    with conn.transaction(force_rollback=True):
        with pytest.raises(psycopg.errors.CheckViolation), conn.transaction():
            conn.execute(insert, row)
        assert conn.execute(ensure).fetchone()[0] == 1
        conn.execute(insert, row)
        assert conn.execute(ensure).fetchone()[0] == 0


def test_drop_month_partitions_before_cutoff(conn):
    with conn.transaction(force_rollback=True):
        conn.execute("SELECT ops.ensure_month_partitions('mart.trending_hourly', '2001-01-01', '2001-02-01')")
        dropped = conn.execute(
            "SELECT ops.drop_partitions_before('mart.trending_hourly', '2001-02-01 00:00+00')"
        ).fetchone()[0]
        assert dropped == 1
        assert conn.execute("SELECT to_regclass('mart.trending_hourly_p200101')").fetchone()[0] is None
        assert conn.execute("SELECT to_regclass('mart.trending_hourly_p200102')").fetchone()[0]


def test_reader_can_select_serving_schemas_but_not_write():
    with connect("wikipulse_reader", "WIKIPULSE_POSTGRES__READER_PASSWORD") as reader:
        for relation in (
            "mart.page_current",
            "mart.trending_hourly",
            "stream.wiki_activity_minute",
            "ops.pipeline_freshness",
        ):
            reader.execute(f"SELECT count(*) FROM {relation}").fetchone()
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute(
                "INSERT INTO mart.evaluation_summary VALUES "
                "('m', 'l', 'enwiki', '2026-01-01', '2026-01-28', 28, 0.5, 0.5, now())"
            )
        reader.rollback()
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            reader.execute("SELECT count(*) FROM enrich.page_info_cache")


def test_freshness_view_reports_every_dataset(conn):
    datasets = {r[0] for r in conn.execute("SELECT dataset FROM ops.pipeline_freshness").fetchall()}
    assert datasets == {"stream", "pageviews_hourly", "page_snapshot", "mediawiki_history", "marts"}


def test_freshness_view_parses_manifest_partition_keys(conn):
    with conn.transaction(force_rollback=True):
        # Rolled back afterwards; clearing first keeps the test independent of real loads in the database.
        conn.execute("DELETE FROM ops.batch_manifest")
        conn.execute(
            """
            INSERT INTO ops.batch_manifest (source, url, partition_key, status) VALUES
              ('pageviews_hourly', 'u1', '2026-10-27-12:00', 'loaded'),
              ('pageviews_hourly', 'u2', '2026-10-27-13:00', 'failed'),
              ('page_snapshot_xml', 'u3', 'enwiki/20261001', 'loaded'),
              ('page_snapshot_xml', 'u4', 'dewiki/20260901', 'loaded'),
              ('mediawiki_history', 'u5', 'enwiki/2026-09', 'loaded')
            """
        )
        rows = dict(
            conn.execute(
                "SELECT dataset, last_complete_period AT TIME ZONE 'UTC' FROM ops.pipeline_freshness"
            ).fetchall()
        )
    assert str(rows["pageviews_hourly"]) == "2026-10-27 13:00:00"
    assert str(rows["page_snapshot"]) == "2026-09-01 00:00:00"
    assert str(rows["mediawiki_history"]) == "2026-10-01 00:00:00"
