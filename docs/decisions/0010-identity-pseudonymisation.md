# 0010. Editor identity: classify and pseudonymise at ingest

Status: Accepted (2026-10-28)

## Context
`page_change` events carry identifiers that relate to real people:
- `performer.user_text`, `user_id` and `user_central_id`
- edit counts and registration dates
- edit comments

Historic data (the History TSV) can contain IP addresses for anonymous edits. The analytics need editor type
and distinct-editor counts, never who edited. The project must not build per-person profiles or rankings.

## Decision
- At the ingest boundary, before anything reaches Kafka or the lake:
  - derive `editor_type ∈ {bot, temp, registered, anonymous_ip, system}` from `is_bot`, `is_temp`, `is_system`,
    and IP-shaped user text
  - replace `user_id`/`user_text` with `editor_key = HMAC-SHA256(key, "<wiki_id>:<user_id>")`, falling back to
    `"<wiki_id>:<user_text>"` when there is no user id, truncated to 16 bytes (32 hex characters). The per-wiki
    local user id is used because it exists in both the stream and the History TSV, so both produce the same
    key; `user_central_id` is dropped, which also prevents linking one person across wikis.
  - drop comments, user groups, edit counts, registration and first-edit dates, and IP addresses
- History TSV loading applies the same function before writing bronze.
- The HMAC key lives in Key Vault (Azure) or the gitignored `.env` (local) and is never logged.
- Outputs expose only counts (edits, distinct editors). The API has no per-editor endpoint or filter.

## Alternatives considered
- **Storing usernames and filtering at the API.** Raw identifiers would sit in Kafka, the lake and backups,
  contrary to the brief.
- **Dropping identifiers entirely.** That loses distinct-editor counts, which edit-burst detection needs (at
  least 3 distinct editors).
- **A daily-rotated key.** It prevents linking across days but breaks 28-day distinct-editor baselines.

## Consequences
- A fixed key makes `editor_key` linkable within the dataset across the whole retention period. Anyone with lake
  access could group activity by key. The mitigation is that no output or endpoint does so, plus access control
  on the lake.
- Losing the key does not expose identities, but it breaks continuity of distinct counts. Rotating it is a
  documented re-key of bronze.
- Bronze is not a byte-for-byte copy of upstream events. The upstream payload is validated before
  transformation, and the field mapping is part of the event contract.
