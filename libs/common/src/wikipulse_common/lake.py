from pathlib import Path

import duckdb

from wikipulse_common.config import Settings
from wikipulse_common.storage import require_data_root

LAKE_ALIAS = "lake"
LAYERS = ("bronze", "silver", "gold")


def _libpq_value(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def catalog_dsn(settings: Settings) -> str:
    pg = settings.postgres
    parts = {
        "host": pg.host,
        "port": str(pg.port),
        "dbname": pg.catalog_database,
        "user": pg.user,
        "password": pg.password.get_secret_value(),
    }
    return " ".join(f"{key}={_libpq_value(value)}" for key, value in parts.items())


def lake_data_path(settings: Settings) -> Path:
    return settings.lake.data_root / "lake"


def ducklake_attach_sql(settings: Settings) -> str:
    target = "ducklake:postgres:" + catalog_dsn(settings)
    data_path = str(lake_data_path(settings)) + "/"
    return (
        f"ATTACH {_sql_literal(target)} AS {LAKE_ALIAS} "
        f"(DATA_PATH {_sql_literal(data_path)}, METADATA_SCHEMA {_sql_literal(settings.lake.catalog_schema)})"
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def connect_lake(settings: Settings) -> duckdb.DuckDBPyConnection:
    require_data_root(settings.lake.data_root)
    lake = settings.lake
    lake.duckdb_temp_directory.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit = {_sql_literal(lake.duckdb_memory_limit)}")
    con.execute(f"SET threads = {int(lake.duckdb_threads)}")
    con.execute(f"SET temp_directory = {_sql_literal(str(lake.duckdb_temp_directory))}")
    con.execute(f"SET max_temp_directory_size = {_sql_literal(lake.duckdb_max_temp_directory_size)}")
    con.execute("SET TimeZone = 'UTC'")
    con.execute(ducklake_attach_sql(settings))
    for layer in LAYERS:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKE_ALIAS}.{layer}")
    con.execute(f"USE {LAKE_ALIAS}")
    return con
