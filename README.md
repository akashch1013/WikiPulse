# WikiPulse

WikiPulse is a data platform for exploring Wikimedia editing activity alongside pageviews. It brings together a live stream of page changes and public Wikimedia datasets to support activity, trending, and edit-burst analytics.

The project is **under development**. The repository contains ingestion, streaming, batch orchestration, transformations, API and dashboard components; the [build plan](docs/plan.md) describes the remaining milestones and acceptance criteria.

## How it works

Wikimedia EventStreams supplies page changes; hourly pageview dumps and MediaWiki snapshots supply batch data. Kafka carries the stream, Dagster orchestrates batch work, and DuckLake and dbt support storage and transformation. PostgreSQL serves data to the API and Grafana. The design also covers an Azure deployment.

Read the [architecture](docs/architecture.md), [data model](docs/data-model.md), and [source documentation](docs/sources.md) for details and limitations.

## Get started

You need Python 3.12, [uv](https://docs.astral.sh/uv/), Docker with Compose, and Make. The base stack downloads container images on its first run.

1. Clone the repository and install the Python workspace:

   ```bash
   git clone https://github.com/akashch1013/WikiPulse.git
   cd WikiPulse
   make sync
   ```

2. Create a local configuration and fill in the values described in the file:

   ```bash
   cp .env.example .env
   ```

   Set a Wikimedia contact, a writable lake data directory, the PostgreSQL and Grafana passwords, and a 64-character hexadecimal HMAC key. Generate a key with `python3 -c "import secrets; print(secrets.token_hex(32))"`. Keep `.env` private. The lake path must meet the mount requirements described in [the storage decision](docs/decisions/0013-local-storage-placement.md).

3. Start the base services and apply database migrations:

   ```bash
   make up-base
   make ps-healthy
   make db-migrate
   ```

   Grafana is available at [localhost:3000](http://localhost:3000) and Prometheus at [localhost:9090](http://localhost:9090). The base stack starts infrastructure only; see the [build plan](docs/plan.md) for the status of the end-to-end pipeline.

4. Stop the stack when finished:

   ```bash
   make down
   ```

   Named volumes are retained.

## Development

```bash
make help             # List available commands
make lint             # Check lint and formatting
make test             # Run workspace unit and contract tests
make contracts-check  # Validate contracts, including OpenAPI (requires Docker)
```

The workspace is organized into `services/` (ingestion, stream processing and API), `orchestration/` (Dagster), `transform/` (dbt), `libs/common/`, `migrations/`, and `dashboards/`. Configuration lives in `config/` and `.env`.

## Documentation

The [documentation site](https://akashch1013.github.io/WikiPulse/) contains the architecture, data model, sources, build plan and decisions. The repository also contains [event and table contracts and the OpenAPI specification](docs/contracts/).

To build the documentation locally:

```bash
uv sync --frozen --only-group docs
uv run --frozen --only-group docs zensical build --strict --clean
```

The generated site is in `site/`. The GitHub Actions documentation workflow publishes it to GitHub Pages on changes to documentation and can also be run manually. In repository **Settings → Pages**, set the build and deployment source to **GitHub Actions** before the first deployment.
