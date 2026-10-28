# 0007. Dashboard with Grafana OSS

Status: Accepted (2026-10-28)

## Context
The project needs two dashboards:
- an analyst dashboard: data-dense tables and time series with filters for wiki, namespace, time range and editor
  type, plus visible data freshness
- an ops dashboard: consumer lag, freshness, DLQ and late rates, manifest status

The local memory budget leaves about 384 MB for a dashboard (Grafana 13.2.5 measured 244 MB idle). On Azure it should scale to zero.

## Decision
Use Grafana OSS 13.3.x with provisioned dashboards stored as JSON in `dashboards/grafana/` and a Postgres
datasource using a read-only role.
- Dashboard variables provide the filters.
- Every panel query reads from `mart`, `stream` or `ops`.
- Locally, Grafana also reads Prometheus for service metrics.
- On Azure it runs as a Container App with min replicas 0. It is stateless, because all configuration is
  provisioned from files.

## Alternatives considered
- **Apache Superset 6.1.0.** Its documented Docker footprint is about 6 GB.
- **Metabase v0.64.3.** It needs about 2 GB of JVM heap in practice.
- **Streamlit 1.66.1.** It is light, but every chart, filter, refresh and freshness indicator is custom code to
  maintain, and it does not cover the ops dashboard.
- **Evidence 40.1.x.** It is a static-site build with few releases in 2026, and it is not suited to live
  freshness.

## Consequences
- One tool serves both analytics and ops, so there is one fewer service.
- Grafana queries Postgres directly rather than the REST API. The API is tested separately through its OpenAPI
  conformance tests and the Newman run.
- Grafana is a monitoring-oriented tool. Analyst tables are built with table panels and SQL. Interactions beyond
  filtering and drill-down links are out of scope.
