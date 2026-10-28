import os

import psycopg
import pytest

from wikipulse_common.config import load_settings
from wikipulse_common.lake import catalog_dsn, connect_lake, ducklake_attach_sql
from wikipulse_common.storage import MARKER_FILE

# Read before the autouse fixture clears WIKIPULSE_* variables.
PG_PASSWORD = os.environ.get("WIKIPULSE_POSTGRES__PASSWORD")
needs_postgres = pytest.mark.skipif(not PG_PASSWORD, reason="needs the compose PostgreSQL credentials")


def test_catalog_dsn_quotes_values(monkeypatch):
    monkeypatch.setenv("WIKIPULSE_POSTGRES__PASSWORD", "it's a \\ secret")
    dsn = catalog_dsn(load_settings())
    assert "password='it\\'s a \\\\ secret'" in dsn
    assert "dbname='lakehouse_catalog'" in dsn


def test_attach_sql_escapes_for_duckdb(monkeypatch, tmp_path):
    monkeypatch.setenv("WIKIPULSE_POSTGRES__PASSWORD", "it's")
    monkeypatch.setenv("WIKIPULSE_LAKE__DATA_ROOT", str(tmp_path))
    sql = ducklake_attach_sql(load_settings())
    assert "it\\''s" in sql
    assert f"DATA_PATH '{tmp_path}/lake/'" in sql


@needs_postgres
def test_lake_round_trip(monkeypatch, tmp_path):
    (tmp_path / MARKER_FILE).touch()
    monkeypatch.setenv("WIKIPULSE_POSTGRES__PASSWORD", PG_PASSWORD)
    monkeypatch.setenv("WIKIPULSE_LAKE__DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("WIKIPULSE_LAKE__DUCKDB_TEMP_DIRECTORY", str(tmp_path / "spill"))
    schema = f"ducklake_test_{os.getpid()}"
    monkeypatch.setenv("WIKIPULSE_LAKE__CATALOG_SCHEMA", schema)
    settings = load_settings()
    table = "bronze.round_trip"
    con = connect_lake(settings)
    try:
        con.execute(f"CREATE TABLE {table} (event_id VARCHAR, n INTEGER)")
        # Large enough to exceed DuckLake's data inlining, so rows land in Parquet rather than the catalog.
        con.execute(f"INSERT INTO {table} SELECT 'e' || i, 1 FROM range(10000) t(i)")
        con.close()
        con = connect_lake(settings)
        assert con.execute(f"SELECT sum(n) FROM {table}").fetchone()[0] == 10000
        assert list((tmp_path / "lake").rglob("*.parquet"))
    finally:
        con.close()
        with psycopg.connect(catalog_dsn(settings)) as pg:
            pg.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
