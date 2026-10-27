# 0012. Postgres migrations with dbmate

Status: Accepted (2026-10-28)

## Context
Postgres holds tables not owned by dbt:
- `ops` (manifest, checkpoints, gaps)
- `stream` (window aggregates)
- `enrich` (enrichment cache)
- `mart` schemas and grants
- roles
- the partitioned parent tables

Schema changes must be versioned, reviewable and applied the same way locally, in CI and on Azure.

## Decision
- Plain-SQL migrations in `migrations/` are applied with dbmate, a single static binary run as a one-shot
  container in compose and as a Container Apps Job on Azure.
- Each migration has `-- migrate:up` and `-- migrate:down` sections.
- dbt owns only the table contents it publishes into `mart`. Table definitions for `mart` are created by
  migrations so the API's read contract is stable.

## Alternatives considered
- **Alembic.** It adds SQLAlchemy as a dependency to a codebase that uses psycopg directly, and its autogenerate
  feature is irrelevant without ORM models.
- **Flyway or Liquibase.** These are JVM tools, heavier to run as a job.
- **Letting dbt create `mart` tables.** The API contract would change whenever a model changes, and dbt cannot
  manage roles, grants or partitioned parents cleanly.

## Consequences
- `mart` tables are defined twice, as a migration and as a dbt contract. The CI contract check compares both
  against `docs/contracts/tables/`.
- Down migrations are written but only exercised in CI (up, down, up) and never run against Azure data.
