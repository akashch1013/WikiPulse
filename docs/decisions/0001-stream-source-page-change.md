# 0001. Use mediawiki.page_change.v1 as the live stream

Status: Accepted (2026-10-28)

## Context
EventStreams (stream.wikimedia.org) exposes several public streams over Server-Sent Events. The platform needs
one live input that carries page identity, revision lineage, editor classification and revert information,
because page-level analytics join the stream with pageviews by `page_id`, not by title.

Measured on 2026-10-28:

| Stream | Rate | Avg event size | Notes |
|---|---|---|---|
| `recentchange` | 34.9 ev/s | 1,358 B | 49% `categorize` events; no `page_id`; `id` missing on 94 of 4,192 events; max event-time lag ~14.9 years |
| `mediawiki.page_change.v1` (schema 1.12.0) | 17.25 ev/s | 2,775 B | edit/create/delete/move/undelete in one stream; `page.page_id`, `revision.rev_sha1`, `revision.revert`, `performer.is_bot`, `performer.is_temp` |

No IP editors appeared in either sample. Temporary accounts (`~2026-NNNNN-NN`) made up about 3% of events, and
only `page_change` flags them explicitly (`performer.is_temp`).

## Decision
Ingest `mediawiki.page_change.v1` for all public wikis. Do not ingest `recentchange`.

- Event time is `dt`. `meta.id` is the deduplication key.
- Resume uses the SSE `id:` array, which holds per-datacenter topic, partition and timestamp entries.
- Canary events (`meta.domain == "canary"`) are dropped at ingest.

## Alternatives considered
- **`recentchange`.** It lacks `page_id`, so every event would need a title→page_id lookup that fails for moves and
  deletes. Half of its volume is category membership noise, it has no temp-account flag, and it occasionally
  carries timestamps years old.
- **The legacy per-kind streams** (`revision-create`, `page-create`, `page-move`, `page-delete`). Using them means
  four connections, four checkpoints, and a merge step to recover one ordered change log per page. `page_change`
  already provides that.
- **Ingesting several streams and joining them downstream.** This adds services and failure modes without adding a
  field the analytics need.

## Consequences
- Log actions outside page lifecycle (protections, blocks, user rights) are not captured. Protection events
  therefore cannot be used as evaluation labels.
- Events are about twice the size of `recentchange` events (~4.1 GB/day raw JSON). Ingest drops unused fields
  (comments, content slot details, editor counters) before producing, which shrinks what Kafka and the lake
  store.
- The stream is newer than `recentchange` and its schema evolves (1.12.0 today). Schema drift handling (see
  docs/architecture.md) is mandatory, not optional.
