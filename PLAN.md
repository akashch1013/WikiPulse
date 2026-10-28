# WikiPulse — planning session plan

## Context
Empty repo (`master`, no commits) at /home/akasdh/Desktop/Projects/PartsHub. This session produces the design and
execution plan for WikiPulse; no application code. After approval I write the planning documents and empty
directory layout below, then send the user a short message (open decisions, volumes, coverage, cost, commit
command) and stop.

Machine: 4 cores, 7 GB RAM (~3 GB free at idle with the user's other workloads). SSD: 23 GB free. External HDD
(user instruction: bulk data goes here, not the SSD): `/media/akasdh/AKASH..`, Toshiba MQ01ABD100 931 GB
spinning disk, NTFS via ntfs-3g (`fuseblk`, allow_other), 194 GB free.

Storage split (ADR 0013 "Local storage placement"):
- HDD, under `${WIKIPULSE_DATA_ROOT}` = `/media/akasdh/AKASH../wikipulse`: the DuckLake data path (bronze/silver/gold
  Parquet), raw source files (pageview .gz, XML parts, History TSV .bz2), and evaluation label snapshots.
  Sequential Parquet/gz I/O suits an HDD (~100 MB/s).
- SSD, Docker named volumes: Postgres data (it requires POSIX ownership and 0700 permissions, which NTFS-FUSE
  cannot provide), Kafka log dirs and Quix RocksDB state (fsync-heavy random I/O), and the DuckDB
  `temp_directory` spill (capped at 4 GB). Together these total under 6 GB.
- `WIKIPULSE_DATA_ROOT` and the UA contact live in the gitignored repo-root `.env`, with `.env.example`
  committed. These are the two values only the user provides. The committed default is `./data` so CI and other
  machines work.
- Every service that touches the data root checks at startup and in `/readyz` that the path is mounted and
  writable. If the drive is unplugged, the stack fails loudly instead of writing to an empty mountpoint on the
  SSD.

User decisions already made:
- User-Agent contact = user's email, stored once in `config/wikipulse.toml` `[http].contact` (env override
  `WIKIPULSE_HTTP__CONTACT`). UA string: `WikiPulse/<version> (<contact>) python-httpx/<v>`.
- Revision history = MediaWiki History TSV dumps (last 3 months); XML = `stub-meta-current` for the page dimension.
- Page-level coverage = enwiki + dewiki. Stream covers all wikis for per-wiki activity.

## Measured volumes (research spikes, 2026-10-27/28; spike output discarded, numbers kept)
| Source | Measurement |
|---|---|
| EventStreams `mediawiki.page_change.v1` (schema 1.12.0) | 17.25 ev/s (90 s), avg 2,775 B raw JSON, ~48 KB/s, ~1.49M ev/day, ~4.1 GB/day raw; 40.8% bot, 3.1% temp accounts, 0 IP editors; 0 duplicate meta.id in 735 |
| EventStreams `recentchange` (not ingested; reference) | 34.9 ev/s, 1,358 B avg, 49% `categorize`; no page_id; p99 lag 23.3 s, max lag ~14.9 years |
| Pageviews hourly (2026-09) | median 61.0 MB gz (44.6–80.4), day 1.34–1.65 GB, month 43.1 GB; one hour = 185 MB raw, 5.94M lines, 1,756 codes, 16.1M views; parse+download 72 s (download-bound) |
| en+en.m+de+de.m share of an hour | 2.29M lines (38.6%), 7.82M views (48%) |
| Publication delay of hourly files | median 2.20 h, p95 2.77 h, max 20.1 h; files are re-published (June 2026 hours rewritten 2026-10-27); no checksums published |
| pageview_complete daily (user) | 0.72–0.86 GB bz2/day, has page_id |
| XML stub-meta-current 20261001 | enwiki 7.85 GB gz (27 parts), dewiki 1.09 GB; md5/sha1 sums published; export-0.11; parse ~7.5 MB/s (download-bound) |
| XML history (rejected) | stub-meta-history en 124 GB / de 23.8 GB (sunset Jan 2027, blackouts Nov 15–30, Dec 6–31); content_history en 1.94 TB, de 496 GB |
| MediaWiki History TSV (2026-09 snapshot) | enwiki 520–585 MB bz2/month; dewiki ~1 GB/year (~85 MB/month) |
| Action API / REST limits (2026 policy) | 10 req/min unidentified, 200 req/min with compliant UA; 429/503 with Retry-After; 50 ids per request; empty UA → 403 |
| dumps.wikimedia.org | max 3 connections per IP |

## Architecture (what docs/architecture.md will state)
Two services always on, batch as jobs.
- **ingest** (Python, httpx SSE): consumes `mediawiki.page_change.v1` for all wikis. It drops canary events,
  validates against the vendored upstream schema (same-major unknown minor → validate against the highest known
  minor, count `schema_drift`; unknown major or invalid → DLQ with reason), and pseudonymises the event
  (HMAC-SHA256 of user_id and user_text with a Key Vault key; drops comments, IPs, edit counts and registration
  dates; derives editor_type ∈ {bot, temp, registered, anonymous_ip, system}). It then produces the internal
  contract event to Kafka topic `wikipulse.page_change.v1` (4 partitions, key `wiki_id:page_id` for per-page
  ordering), idempotent producer, acks=all. The SSE `Last-Event-ID` array is checkpointed to Postgres
  `ops.stream_checkpoint` only after `producer.flush()`; restart and the 15-min server disconnect resume from it.
  Replays create duplicates, which are removed downstream by meta.id. Gaps longer than upstream retention
  (7–31 days) are recorded in `ops.stream_gaps`.
- **stream-processor** (Quix Streams 3.28.x): per-key dedup on meta.id (RocksDB state, 24 h TTL) → (a) bronze sink
  into the lakehouse; (b) event-time windows on `dt`: 1-min tumbling per wiki × editor_type, and 60-min hopping
  (5-min step) per page. Grace is 5 min. Events with event time > 10 min ahead of processing time are clamped to
  the DLQ as `future_timestamp`. Windows closed past grace route via `on_late` to topic
  `wikipulse.page_change.late.v1`, and the count is a metric. Aggregates are upserted to Postgres `stream.*`
  keyed by (wiki, window_start[, page_id]). Streaming aggregates are provisional; the hourly batch recompute from
  silver is authoritative and the reconciliation test compares them.
- Delivery: at-least-once end to end; offsets committed after sink flush (Quix checkpoint); idempotent writes
  keyed by meta.id / window key. Exactly-once is not claimed because Event Hubs Standard has no Kafka transactions.
- **Lakehouse**: DuckLake (DuckDB 2.0 LTS) with its catalog in the existing Postgres (separate database) and
  Parquet data in `./data/lake` locally or `abfss://lake@…` on ADLS Gen2. Layers: `bronze` (as-landed, plus
  source file/offset lineage), `silver` (typed, deduped, title→page_id resolved), `gold` (star schema). Every
  write replaces a partition via DELETE+INSERT in one transaction, which keeps batch reruns idempotent.
- **Batch jobs** (Dagster 1.13.x assets; partitions = hourly/monthly):
  pageviews hourly (manifest-driven backfill + incremental + republication sweep using listing size/mtime vs
  manifest); stub-meta-current XML (stdlib iterparse with element clearing, streamed from HTTP, md5 verified on
  the streamed bytes before the partition commit, one manifest row per part); MediaWiki History TSV (last 3
  months, streamed bz2); pageview_complete daily (reconciliation sample); enrichment (Action API
  categories/pageprops/redirects + Wikidata P31 for pages in gold outputs only, token bucket at 150/min,
  serial, maxlag=5, Retry-After, 7-day cache in Postgres `enrich.*`); dbt build; publish-to-serving.
  Manifest: `ops.batch_manifest` (source, url, partition_key, etag, last_modified, size, checksum, status,
  rows_in, rows_out, run_id, loaded_at).
- **dbt** (dbt-core 1.12.x + dbt-duckdb 1.11.x on DuckLake) builds silver→gold. **publish** copies the gold
  partitions the API needs into Postgres `mart.*` (delete+insert per partition in one Postgres transaction).
  The full page-hour pageview fact stays in the lake (~2.3M rows/hour for en+de), so Postgres B1ms only holds
  serving-sized marts.
- **Postgres 18**: schemas `ops`, `stream`, `enrich`, `mart`; plain-SQL migrations with dbmate for every
  non-dbt table. Read-only role for API and Grafana.
- **API**: FastAPI 0.162, psycopg 3.3 + psycopg_pool, keyset pagination, filters (wiki, namespace, editor_type,
  time range), `/healthz`, `/readyz`, `/v1/freshness`. The OpenAPI spec is the frozen contract, and the Postman
  collection is generated from it and run with Newman in CI.
- **Dashboard**: Grafana OSS 13.x with dashboards as code (provisioned JSON) over Postgres. One analyst
  dashboard and one ops dashboard (lag, freshness, DLQ/late rates, manifest status).
- **Observability**: JSON structured logs to stdout. Metrics use the OpenTelemetry API: OTLP → Prometheus locally,
  Azure Monitor exporter in cloud. Freshness and consumer lag are also materialised in `ops` views, so both
  environments can show them.
- Local memory budget (compose limits): kafka 768M (heap 512M), postgres 512M, ingest 128M, stream-processor
  384M, dagster webserver 384M + daemon/run 1.25G (DuckDB memory_limit 1 GB, 2 threads), api 128M, grafana 256M,
  prometheus 192M. Steady state is about 2.6 GB and peak during dbt/backfill about 4.0 GB. The build measures
  both.
- Local disk budget. SSD: Kafka 3-day retention ~1.4 GB, Postgres ≤3 GB, RocksDB <0.5 GB, DuckDB spill ≤4 GB.
  HDD, for the 35-day window:
  - raw pageview .gz: 35 × 1.42 GB ≈ 50 GB (full files kept as published, so filtering stays reproducible)
  - raw XML en+de: 8.9 GB
  - raw History TSV: ~2 GB
  - bronze stream: ~0.45 GB/day
  - silver/gold pageviews en+de: ~13 GB

  Total about 90 GB of the 194 GB free. Raw retention is configurable (`lake.raw_retention_days`). On Azure, raw
  files are retained 90 days via lifecycle policy.

## Coverage (chosen)
Stream: all public wikis, with page-level detail for en+de only. Pageviews: en, en.m, de, de.m. Local and Azure use the
same 35-day window (28-day evaluation + 7-day baseline): 840 hourly files and ~51 GB downloaded. At 3
connections (the dumps host limit) and the measured ~72 s per file, that is about 5.6 h one-time, then
incremental. A `quick` profile backfills 8 days for development. XML: stub-meta-current en+de, latest monthly run. History TSV:
the 3 most recent complete months, en+de. The first milestone slice uses dewiki only (1.09 GB XML) to keep it
fast.

## Azure (eastus retail prices) and cost
Event Hubs Standard 1 TU (Kafka endpoint; Basic has none) at $21.90/mo plus ~$1.3 ingress. PG Flexible B1ms
with 32 GB at $16.1/mo (stop/start, 7-day auto-restart; free 12-month offer if the subscription is eligible).
Container Apps: ingest 0.25 vCPU/0.5 GiB and stream-processor 0.5 vCPU/1 GiB always on is ≈$54/mo after free
grants. The API and Grafana scale to zero. Dagster runs as scheduled Container Apps Jobs invoking
`dagster asset materialize` (no always-on daemon in cloud; the UI is local only), ≈$5–8/mo. ADLS <$1. Logs
capped at 5 GB/day free tier via a daily cap. Key Vault ~$0. Images go to GHCR instead of ACR (saves $5 and a
service). Total ≈ $100/mo if left on 24/7 ≈ $3.3/day. The plan is `make azure-up` / `make azure-down` sessions
(e.g. 10 days/month ≈ $35). Terraform azurerm 5.11.0, state in a bootstrap storage account, and GitHub Actions
`azure/login@v3` with OIDC.

GCP mapping section: GCS, Managed Service for Apache Kafka, Cloud SQL, Cloud Run services/jobs, Secret
Manager, Cloud Logging/Monitoring, Workload Identity Federation, API Gateway; BigQuery as the alternative gold
engine.

## Analytics and evaluation (docs/data-model.md)
- **Trending reads**: robust z-score of v(p,h) against the median/MAD of the trailing 168 h (missing hours = 0),
  with a per-wiki view floor (config). Evaluated on enwiki: labels are articles linked from "Portal:Current
  events/<date>" (d−1..d+1, fetched via the Action API) and, as a separate label set, Main Page links per day.
  Metrics are daily precision@20 and recall of labelled pages above the floor, against two baselines (raw
  views, day-over-day ratio). dewiki is shown but not evaluated, and the doc says so.
- **Edit bursts**: 60-min window edit count e (non-bot, ≥3 distinct pseudonymous editors) with a Poisson
  upper-tail p < 1e-4 against the page's 28-day hourly rate λ (history TSV + stream, floor λ). Evaluated with
  precision@20/day against the same labels, versus a raw edit-count ranking.
- **Reverts**: `revision.revert` from page_change, validated against History TSV `revision_is_identity_revert`
  for the overlapping period (agreement rate reported).
- **Overlap**: read-surge ∩ edit-burst for the same page within ±1 h, with the lead/lag distribution.
- **Editor mix** and **per-wiki activity** (all wikis).
- **Reconciliations**: stream vs batch hourly aggregates (difference ≤ recorded late+dup counts); our resolved
  hourly views summed per day vs pageview_complete per page_id (match rate threshold); stream edits vs History
  TSV per wiki-day.
- Star schema: dims are wiki, namespace, editor_type, date/hour, and page (SCD2 keyed by (wiki_id, page_id): title,
  namespace, is_redirect, redirect_target_page_id, valid_from/to, deleted_at, built from monthly snapshots +
  stream create/move/delete/undelete), plus page_enrichment and bridge_page_category. Facts with grain:
  fct_pageviews_hourly (wiki, page_id, hour, access_method; lake only), fct_revision (wiki, rev_id),
  fct_page_lifecycle_event (wiki, meta_id), fct_page_activity_hourly (wiki, page_id, hour), fct_wiki_activity_hourly
  (wiki, hour, editor_type), fct_trending_hourly (wiki, page_id, hour; candidates above floor),
  fct_edit_burst (wiki, page_id, window_start). Unresolved titles go to `silver.pageviews_unresolved`, with the
  resolved-views share as a DQ metric. Title normalisation: percent-decode, spaces→underscores, first-letter
  case rule per wiki, redirects to target page_id with a `via_redirect` flag.
- Privacy: no per-person outputs, only distinct counts; HMAC key in Key Vault; IPs never stored; no article
  text stored.

## Files to create
- `README.md` — stub: what WikiPulse is, sources, status, link to docs.
- `docs/architecture.md` — all sections above with a Mermaid flowchart, local/Azure topology, GCP mapping,
  delivery/failure table (failure → detection → handling), privacy/security/observability, volumes, coverage,
  cost with assumptions.
- `docs/sources.md` — the 5 sources: EventStreams, pageviews hourly + pageview_complete, XML stub-meta-current,
  MediaWiki History TSV, Action API/Wikidata. Each with format, cadence, licence (CC0 for pageviews/analytics
  and History TSV; CC BY-SA for content-derived metadata; Wikidata CC0), access rules, and known issues
  (canary, multi-DC ids, old timestamps, republication, no checksums, `""` codes, sunset).
- `docs/decisions/`, about 12 ADRs (0001–0012):
  - page_change.v1 as the stream (vs recentchange)
  - Kafka protocol, local apache/kafka 4.4.0 vs Redpanda; Event Hubs Standard
  - Quix Streams (vs PyFlink, Spark, Bytewax, Faust)
  - DuckLake on DuckDB 2.0 (vs Delta via delta-rs, Iceberg)
  - dbt-duckdb + publish to Postgres (vs dbt-postgres for everything)
  - Dagster, with cloud Jobs instead of a daemon (vs Airflow 3, Prefect)
  - Grafana (vs Streamlit, Superset, Metabase)
  - revision history from History TSV + stub-meta-current XML
  - coverage en+de
  - pseudonymisation
  - Container Apps + GHCR + Terraform (vs AKS, ACR, Bicep)
  - dbmate migrations
  - local storage placement (HDD for the lake and raw files, SSD for Postgres/Kafka/RocksDB/spill), numbered 0013
- `docs/data-model.md` — layers, star schema (Mermaid ER), grain per fact, SCD2 rules, analytic definitions and
  evaluation methods.
- `docs/contracts/`:
  - `events/` (upstream page_change 1.12.0 field subset reference + internal `page_change_event.v1.json`,
    `dlq_event.v1.json`, `wiki_activity_window.v1.json`)
  - `tables/*.yaml` (columns, types, nullability, keys, grain, partitioning, owner workstream) for every
    bronze/silver/gold/ops/stream/mart table
  - `openapi.yaml` (OpenAPI 3.1 draft)
  - `config.md`-less: config layout lives in `config/wikipulse.toml` comments, with one line in architecture.md
- `docs/plan.md` — milestones, workstreams, parallelisation map, runnable definitions of done.
- `planning/status.json` — every task: id, milestone, workstream, owner_dirs, depends_on, status, done_check.
- Empty dirs (with `.gitkeep`): `config/`, `libs/common/`, `services/ingest/`, `services/stream_processor/`,
  `services/api/`, `orchestration/`, `transform/`, `migrations/`, `dashboards/grafana/`, `deploy/local/`,
  `infra/terraform/`, `tests/e2e/`, `tests/fixtures/`, `.github/workflows/`. Python is a uv workspace (one member
  per service dir, lock file owned by the lead). Nothing written except docs, status.json, and .gitkeep files.

## Milestones (docs/plan.md)
- **M0 Foundations & frozen contracts** (core): uv workspace, compose base (kafka, postgres, prometheus,
  grafana), config loader, common lib (http client with UA/rate limit/backoff, kafka config factory with
  local/Event Hubs profiles, logging, telemetry), dbmate baseline, CI lint/test. Real fixtures captured
  (2,000 page_change events, first 50k lines of one pageview hour, one dewiki stub part slice cut at a page
  boundary, 10k History TSV lines, each with source URL + timestamp).
- **M1 Thin E2E slice** (core): live page_change → Kafka → processor (dedup, per-wiki 1-min windows) →
  bronze/silver + `stream.wiki_activity_minute`. One real dewiki pageview hour + dewiki stub-meta-current →
  silver resolved → dbt gold → publish → API (`/v1/wikis/{wiki}/activity`, `/v1/pages/top-viewed`,
  `/v1/freshness`) → Grafana panels with freshness. DoD: `make up && make slice-check` asserts rows in every
  layer and non-empty API responses.
- **M2 Batch depth** (core): manifest backfill/incremental/republication, en+de XML, History TSV, SCD2 dim_page,
  redirect resolution, pageview_complete reconciliation, DQ tests.
- **M3 Stream depth** (core): resume/chaos tests (kill -9, dropped connection, forced 15-min disconnect replay),
  schema drift/DLQ, late-data routing, per-page windows, stream-vs-batch reconciliation, throughput/memory
  measurements.
- **M4 Analytics & evaluation** (core): trending, edit bursts, reverts validation, overlap, editor mix,
  enrichment, evaluation harness with reported metrics.
- **M5 Serving** (core; can overlap M4 after contracts freeze): full API, conformance tests, Postman/Newman,
  full dashboards.
- **M6 Azure** (core): Terraform, managed identity/Key Vault, Event Hubs compatibility spike first (Quix
  changelog topics pre-created, no compression), Container Apps + Jobs, OIDC CI/CD, App Insights, up/down
  timing and cost check.
- **M7 Docs with measured results** (core).
- Optional: O1 API Management Consumption gateway; O2 Lift Wing topic enrichment; O3 dewiki trending labels;
  O4 Dagster UI in Azure.

Workstreams own disjoint dirs: WS-common (`libs/common`, lead), WS-ingest (`services/ingest`), WS-stream
(`services/stream_processor`), WS-batch (`orchestration/` loaders), WS-transform (`transform/`), WS-serving
(`services/api`, `docs/contracts/postman` output), WS-dash (`dashboards/`), WS-infra (`infra/`,
`.github/workflows`, `deploy/`), WS-db (`migrations/`), lead-only (`docs/`, `config/`, `compose.yaml`,
`pyproject.toml`, `uv.lock`, `tests/e2e`, `planning/`). Parallelism: after M0 contracts freeze, ingest, stream,
batch, transform, serving and dash run in parallel within M1. M2 and M3 run in parallel. M4 follows M2+M3. M5
parallels M4. M6 starts after M1 (infra) and finishes after M5.

Once approved, I also save a memory entry: large data for this project goes on the external HDD at
`/media/akasdh/AKASH..`, with Postgres/Kafka kept on the SSD because of NTFS-FUSE limits. This session creates
nothing on the HDD; the build sessions create `wikipulse/` there.

## Verification (this session)
- `find docs planning -type f | sort` lists exactly the planned files; no extra markdown.
- `python3 -I -c "import json;json.load(open('planning/status.json'))"` parses, and every task has the required
  fields.
- OpenAPI draft validates (`npx @redocly/cli lint docs/contracts/openapi.yaml` run once via a throwaway container,
  removed afterwards), and the JSON Schemas parse.
- Mermaid blocks are checked by eye for syntax. A grep for banned words/emojis across docs comes back empty.
- `git status` shows only the planned paths. Then send the commit command:
  `git add README.md docs planning config libs services orchestration transform migrations dashboards deploy infra tests .github && git commit -m "docs: add WikiPulse architecture, contracts and build plan"`
  (explicit paths, no trailers, per the user's instructions, which override the default attribution).
