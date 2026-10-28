SHELL := /bin/bash
.DEFAULT_GOAL := help

BASE_SERVICES := kafka kafka-topics postgres prometheus grafana
DBMATE := docker run --rm --network host -v $(CURDIR)/migrations:/db/migrations:ro \
	-e DATABASE_URL="postgres://wikipulse:$${WIKIPULSE_POSTGRES__PASSWORD}@127.0.0.1:55432/wikipulse?sslmode=disable" \
	-e DBMATE_MIGRATIONS_DIR=/db/migrations -e DBMATE_NO_DUMP_SCHEMA=true amacneil/dbmate:2.37.0

-include .env
export

.PHONY: help
help: ## List targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-20s %s\n", $$1, $$2}'

.PHONY: sync
sync: ## Install the Python workspace from the lock file
	uv sync --frozen --all-packages

.PHONY: lint
lint: ## Ruff lint and format check
	uv run ruff check .
	uv run ruff format --check .

.PHONY: test
test: ## Unit and contract tests for every workspace member
	uv run pytest libs/common services/ingest orchestration tests/contracts

.PHONY: contracts-check
contracts-check: ## Validate event schemas, table contracts and the OpenAPI document
	uv run pytest tests/contracts
	docker run --rm -v $(CURDIR)/docs/contracts:/spec:ro redocly/cli:2.0.8 lint /spec/openapi.yaml

.PHONY: up-base
up-base: ## Start Kafka, PostgreSQL, Prometheus and Grafana
	docker compose up -d --wait $(filter-out kafka-topics,$(BASE_SERVICES))
	docker compose up kafka-topics --exit-code-from kafka-topics

.PHONY: ps-healthy
ps-healthy: ## Fail unless every running long-lived container is healthy
	@docker compose ps --format json | python3 -I -c 'import json,sys; \
	rows=[json.loads(l) for l in sys.stdin if l.strip()]; \
	bad=[r["Service"]+":"+(r.get("Health") or r["State"]) for r in rows if r["State"]=="running" and r.get("Health")!="healthy"]; \
	print("\n".join(r["Service"]+" "+(r.get("Health") or r["State"]) for r in rows)); sys.exit(1 if bad or not rows else 0)'

.PHONY: down
down: ## Stop the stack, keeping volumes
	docker compose down

.PHONY: db-migrate
db-migrate: ## Apply PostgreSQL migrations
	$(DBMATE) --wait up

.PHONY: db-rollback-check
db-rollback-check: ## Apply all migrations, roll every one back, apply again
	$(DBMATE) --wait up
	n=$$(ls migrations/*.sql | wc -l); for i in $$(seq $$n); do $(DBMATE) rollback || exit 1; done
	$(DBMATE) up

.PHONY: contracts-check-db
contracts-check-db: ## Compare PostgreSQL tables with docs/contracts/tables
	uv run pytest migrations/tests
