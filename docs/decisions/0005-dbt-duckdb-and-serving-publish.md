# 0005. Transform with dbt-duckdb; publish serving marts to Postgres

Status: Accepted (2026-10-28)

## Context
Silver→gold modelling needs tested, documented SQL with lineage. Gold data volumes differ widely:
- the page-hour pageview fact is about 2.3M rows/hour, or about 1.9B rows over 35 days for en+de
- the API needs only serving-sized aggregates: trending candidates, bursts, page activity, wiki activity

The serving database is Azure Postgres Flexible B1ms (1 vCore, 2 GiB, 32 GB storage).

## Decision
- Use dbt-core 1.12.x with dbt-duckdb 1.11.x, running against the DuckLake catalog.
  - All silver and gold models live in the lake.
  - dbt tests cover not-null, unique, relationships, accepted values, freshness and reconciliation.
- A separate `publish` asset copies the gold partitions the API needs into Postgres schema `mart`. It uses
  DELETE+INSERT per partition in one Postgres transaction, through the DuckDB `postgres` extension.
- Contract-enforced dbt model configs mirror `docs/contracts/tables/`. A CI check compares them.

## Alternatives considered
- **dbt-postgres 1.11.0 for all models.** Loading about 1.9B pageview rows into B1ms would exceed its storage and
  memory, and scans would be slow.
- **dbt-duckdb writing directly into attached Postgres.** Incremental strategies against an attached database are
  less mature, and coupling dbt runs to Postgres availability blocks lake modelling during Postgres maintenance.
- **dbt v2 (Fusion, 2.0.0rc9).** DuckDB support is beta and CLI-only, it is under a separate licence, and Postgres
  is not supported.

## Consequences
- The publish step is an extra hop with its own freshness. `ops` views expose the lag between gold and mart, and
  the API's `/v1/freshness` reports both.
- Analysts who need page-level history beyond the marts query the lake with DuckDB. The API does not serve it.
- dbt-core 1.12 is the pinned line. A later move to Fusion is a separate decision once DuckDB support leaves beta.
