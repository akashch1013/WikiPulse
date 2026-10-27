# WikiPulse

WikiPulse captures live editing activity across Wikimedia projects, joins it with hourly pageview data, and
serves activity, read-surge and edit-burst analytics through a REST API and a Grafana dashboard.

Inputs, all public Wikimedia data:

- Wikimedia EventStreams `mediawiki.page_change.v1` (Server-Sent Events, about 17 events/s across all wikis)
- Hourly pageview dumps (about 1.4 GB compressed per day)
- MediaWiki `stub-meta-current` XML dumps and MediaWiki History TSV dumps for enwiki and dewiki
- The MediaWiki Action API and Wikidata, for page enrichment

The platform runs locally with Docker Compose and on Azure (Event Hubs Kafka endpoint, ADLS Gen2, Container Apps,
PostgreSQL Flexible Server).

Status: design complete, build not started. The design is in:

- [docs/architecture.md](docs/architecture.md): components, data flow, delivery semantics, measured volumes, cost
- [docs/data-model.md](docs/data-model.md): layers, star schema, analytic definitions and their evaluation
- [docs/sources.md](docs/sources.md): sources, formats, licences, known data issues
- [docs/decisions/](docs/decisions/): architecture decision records
- [docs/contracts/](docs/contracts/): event schemas, table contracts, OpenAPI specification
- [docs/plan.md](docs/plan.md): build milestones and workstreams
