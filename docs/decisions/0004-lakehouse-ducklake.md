# 0004. Lakehouse table format: DuckLake on DuckDB 2.0

Status: Accepted (2026-10-28)

## Context
The lake has three layers (bronze, silver, gold) on local disk and on ADLS Gen2. It holds about 2.3M pageview rows
per hour for en+de and about 1.5M stream events per day.

The format must support:
- writes from Python services and from dbt
- atomic replacement of a partition, so batch reruns are idempotent
- concurrent writers (the stream sink and batch jobs)
- reads from DuckDB

Postgres already runs for serving.

## Decision
Use DuckLake 1.x through DuckDB 2.0.0 LTS.
- The catalog is in a separate database (`lakehouse_catalog`) on the existing Postgres server.
- Data is Parquet under `${WIKIPULSE_DATA_ROOT}/lake` locally and `abfss://lake@<account>.dfs.core.windows.net`
  on Azure, through the DuckDB `azure` extension.
- Every batch write deletes and inserts a partition in one DuckLake transaction.
- The stream sink commits each batch together with its Kafka offset range.
- A maintenance asset compacts small files and expires old snapshots.

## Alternatives considered
- **Delta Lake via delta-rs (`deltalake` 1.6.6) plus the DuckDB `delta` extension.** The DuckDB extension can only
  CREATE and INSERT, with no MERGE, UPDATE or DELETE. dbt models would therefore have to go through Python
  writers, which splits the transformation logic.
- **Apache Iceberg (pyiceberg 0.12.0).** DuckDB writes require a REST catalog, which is one more service to run
  locally and on Azure.
- **Plain Hive-partitioned Parquet with a manifest.** Partition replacement would not be atomic, and concurrent
  writers could interleave files.

## Consequences
- DuckLake reached 1.0 in April 2026 and is younger than Delta or Iceberg. Fewer reviewers will know it, and
  fewer engines read it (DuckDB only, in practice). The ADR accepts this in exchange for one engine, no extra
  catalog service, and transactional DELETE+INSERT from both Python and dbt.
- The catalog's availability is tied to Postgres. A Postgres outage blocks lake writes, but no data is lost
  because Kafka and the source files remain.
- The DuckDB `azure` extension does not refresh access tokens. Long Azure jobs must re-create the secret, or
  stay under the token lifetime.
- DuckDB 2.0 database files cannot be opened by older versions. The DuckDB version is pinned across all images.
