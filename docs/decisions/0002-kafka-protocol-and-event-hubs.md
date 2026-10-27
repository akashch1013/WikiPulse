# 0002. Kafka protocol: Apache Kafka locally, Event Hubs Standard on Azure

Status: Accepted (2026-10-28)

## Context
The live stream must be buffered so that ingest, stream processing and lake landing fail and restart
independently. The same client code (confluent-kafka) must run against a local broker on a 4-core / 7 GB machine
and against Azure.

Load is small: 17.25 ev/s, about 48 KB/s raw. After ingest drops fields, events are about 1.2 KB, which is
roughly 1.8 GB/day before compression.

## Decision
- **Local:** single-node `apache/kafka:4.4.0` in KRaft mode with `KAFKA_HEAP_OPTS=-Xmx512M -Xms512M`, a 768 MB
  container limit, and zstd producer compression.
  - Topic `wikipulse.page_change.v1` has 4 partitions, keyed by `wiki_id:page_id`, with 3-day retention.
  - DLQ topic `wikipulse.page_change.dlq.v1` and late-event topic `wikipulse.page_change.late.v1`.
- **Azure:** Event Hubs Standard with 1 throughput unit and Kafka endpoint enabled, at $0.03/h ($21.90/month)
  plus $0.028 per million events (≈$1.3/month at the measured rate). Retention is 1 day.
  - The topics, plus the Quix changelog and repartition topics, are created by Terraform, not auto-created.
  - Producer compression is `none`, because Standard does not support Kafka compression.
- A single config profile switch (`kafka.profile = local | eventhubs`) selects:
  - bootstrap servers
  - SASL OAUTHBEARER with managed identity
  - compression

## Alternatives considered
- **Event Hubs Basic ($10.95/month).** It has no Kafka endpoint.
- **Event Hubs Premium ($749.71/month per PU).** It supports Kafka transactions and compression, but costs far
  beyond a $100 student credit.
- **Redpanda (v26.2.6).** It has a smaller footprint, but the Community Edition is BSL-licensed. The project
  also gains nothing over upstream Kafka at this load.
- **Kafka on a Container App in Azure.** It would avoid the TU charge, but a self-managed broker on
  consumption-plan storage gives up durability guarantees and adds the operational work Event Hubs removes.
- **Azure Event Hubs native SDK.** That would mean two client code paths.

## Consequences
- Event Hubs Standard supports idempotent producers but not Kafka transactions. End-to-end delivery is
  therefore stated as at-least-once with idempotent writes, never exactly-once (see 0003).
- Standard allows 20 consumer groups per event hub, 32 partitions and 7-day maximum retention. The design uses
  2 consumer groups and 4 partitions.
- Behaviour differences between local Kafka and Event Hubs (topic admin, compaction of changelog topics) are
  checked by a dedicated spike at the start of the Azure milestone, before any Azure deployment work.
