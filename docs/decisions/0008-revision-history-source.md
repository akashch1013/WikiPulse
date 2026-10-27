# 0008. Revision history from MediaWiki History TSV; XML for the page dimension

Status: Accepted (2026-10-28)

## Context
Two analytics need per-revision history from before the stream started:
- the 28-day page edit rate behind edit-burst detection
- revert ground truth for validating stream revert counts

The original brief named MediaWiki XML dumps for page and revision history.

Measured on 2026-10-27/28:

| Source | enwiki | dewiki | Status |
|---|---|---|---|
| `stub-meta-history` (XML, run 20261001) | 124.0 GB gz | 23.8 GB gz | Deprecated; last run Dec 2026; none from Jan 2027; blackouts Oct 26–31, Nov 15–30, Dec 6–31 |
| `mediawiki_content_history` (XML with full text) | 1.94 TB bz2 | 496 GB bz2 | Supported replacement; includes article text |
| `stub-meta-current` (XML) | 7.85 GB gz (27 parts) | 1.09 GB gz | Not affected by the sunset; md5/sha1 published |
| MediaWiki History TSV (2026-09 snapshot) | 520–585 MB bz2 per month | ~85 MB per month | Monthly; includes `revision_is_identity_revert`, `revision_is_identity_reverted` |

## Decision
- **Page dimension snapshot:** `stub-meta-current` XML for enwiki and dewiki. It provides page id, namespace,
  title, redirect target and latest revision, from the latest monthly run. It is parsed by streaming with md5
  verification.
- **Revision history:** the MediaWiki History TSV for the three most recent complete months, for enwiki and
  dewiki. It is streamed through bz2, and only revision events are kept.

## Alternatives considered
- **`stub-meta-history` XML.** It is not reproducible after January 2027 and is in blackout windows during the
  build period. Its size (124 GB) also exceeds a reasonable local run.
- **`mediawiki_content_history` XML for en/de.** At 0.5–1.9 TB, almost all of it article text the project does not
  store, it is infeasible to download.
- **`mediawiki_content_history` for simplewiki (4.35 GB).** Revision history would cover a different wiki from
  pageviews and the stream.
- **Action API revisions queries.** At 200 requests/min, three months of en+de revisions is not practical.

## Consequences
- XML is still processed at scale: enwiki's 7.85 GB across 27 parts is streamed in bounded memory with per-part
  manifest rows and checksum verification. It covers the current state, not revision history.
- The project adds a fifth source (TSV, CC0). Its documented format and reported data issues go into
  docs/sources.md.
- The History TSV is published monthly with a lag, so the newest days of the baseline come from the stream
  itself. The boundary between the two sources is recorded per page in the baseline table.
