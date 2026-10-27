# 0006. Orchestration with Dagster; Container Apps Jobs in the cloud

Status: Accepted (2026-10-28)

## Context
Batch work includes:
- hourly pageview partitions, with backfill over 840 hours and a republication sweep
- monthly XML and History TSV partitions
- enrichment
- dbt
- publish

It needs partition-aware backfills, a record of which partitions ran, and lineage to dbt models. Locally,
orchestration must fit in about 1.6 GB, including run workers. On Azure an always-on scheduler costs about
$15/month per 0.25 vCPU.

## Decision
Use Dagster 1.13.x with software-defined assets, hourly and monthly partitions, and `dagster-dbt` for model
lineage. Run storage is Postgres.
- **Locally:** dagster-webserver (384 MB limit) and dagster-daemon (1.25 GB limit, including run workers and
  DuckDB at a 1 GB memory_limit).
- **Azure:** no daemon or webserver. Container Apps scheduled Jobs run `dagster asset materialize --select ...
  --partition ...` from the same image and definitions.
- **Our manifest is the authority:** `ops.batch_manifest` decides which source files are processed, whether
  Dagster or a cloud Job triggers the run.

## Alternatives considered
- **Airflow 3.3.2.** Its Docker guide asks for at least 4 GB (ideally 8) for Airflow alone, which exceeds this
  machine's budget. Its data-interval model is also less direct than asset partitions for backfills.
- **Prefect 3.9.0.** It fits in memory, but it has no first-class asset partitions or dbt asset lineage, so the
  manifest logic would be the same and the UI would show less.
- **Cron plus scripts.** There would be no backfill tooling, lineage or run history.

## Consequences
- In the cloud there is no Dagster UI or sensor loop. Schedules are Container Apps cron expressions, and run
  history comes from `ops.batch_manifest` and Log Analytics. Running the UI in Azure is optional item O4.
- Dagster memory figures are not published officially. The build measures them, and the compose limits are
  adjusted from those measurements.
- Because the manifest lives outside Dagster, partition status can differ between Dagster's view and the
  manifest. The manifest wins, and a DQ check reports divergence.
