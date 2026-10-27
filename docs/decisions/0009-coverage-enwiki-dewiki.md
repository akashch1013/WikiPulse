# 0009. Page-level coverage: enwiki and dewiki; stream for all wikis

Status: Accepted (2026-10-28)

## Context
Pageview hourly files cover all 1,756 project codes: a median 61.0 MB gz per hour, 5.94M lines, and 1.42 GB per
day. Page-level analytics need a page dimension per wiki, built from stub-meta-current and History TSV. The cost
of those grows with the wiki. Trending evaluation needs public, dated labels.

The stream itself is cheap (17.25 ev/s across all wikis).

## Decision
- **Stream:** ingest all public wikis. Per-wiki activity and editor mix cover every wiki.
- **Page-level analytics** (pageviews, page dimension, trending, edit bursts, overlap) cover enwiki and dewiki.
  - Pageview codes: `en`, `en.m`, `de`, `de.m`. In the measured hour these held 2.29M lines (38.6%) and 7.82M
    views (48%).
- **Window:** 35 days (28-day evaluation + 7-day baseline) both locally and on Azure. A `quick` profile uses 8
  days for development.
- **XML:** stub-meta-current for en+de.
- **History TSV:** 3 months for en+de.
- **First end-to-end slice:** dewiki only, using 1.09 GB of XML.

## Alternatives considered
- **enwiki only.** That shows a single wiki at page level, and the first slice would depend on 7.85 GB of XML.
- **dewiki + frwiki.** It is cheaper, but there is no structured, dated news label set comparable to enwiki
  "Portal:Current events", so trending would go unevaluated.
- **All wikis at page level.** That needs page dimensions for about 1,000 wikis, beyond the disk and runtime
  budget.

## Consequences
- Trending and edit-burst detection are evaluated on enwiki only. dewiki results are shown but marked as not
  evaluated (optional item O3 adds German labels).
- The 35-day backfill downloads about 51 GB (840 files). At 3 connections and about 72 s per file, it takes
  about 5.6 h one-time.
- The `app` access method is not in hourly files. Mobile-app views are therefore absent from hourly facts, and the
  data model states this.
