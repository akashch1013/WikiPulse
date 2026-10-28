# Sources

Every request to a Wikimedia host carries `User-Agent: WikiPulse/<version> (<contact>) python-httpx/<version>`.
The contact is set once, as `WIKIPULSE_HTTP__CONTACT` in `.env`. Requests without a User-Agent are refused with
HTTP 403.

Access policy references:

- User-Agent policy: https://foundation.wikimedia.org/wiki/Policy:User-Agent_policy
- API etiquette: https://www.mediawiki.org/wiki/API:Etiquette
- Rate limits: https://www.mediawiki.org/wiki/Wikimedia_APIs/Rate_limits

## 1. Wikimedia EventStreams: `mediawiki.page_change.v1`

| | |
|---|---|
| Endpoint | `https://stream.wikimedia.org/v2/stream/mediawiki.page_change.v1` |
| Docs | https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams_HTTP_Service |
| Format | Server-Sent Events; each `data:` line is one JSON event; each `id:` line is a JSON array of `{topic, partition, timestamp\|offset}` |
| Schema | `mediawiki/page/change` 1.12.0, https://schema.wikimedia.org/repositories/primary/jsonschema/mediawiki/page/change/ |
| Frequency | continuous; 17.25 events/s measured, 2,775 B mean |
| Licence | Event metadata is published by the Wikimedia Foundation with the projects' content. Page titles and edit metadata derive from CC BY-SA content; WikiPulse stores titles and numeric metadata only. |
| Access rules | no documented rate or connection limit; no server-side filtering, so clients consume the whole stream. The service documents a 15-minute connection timeout; observed sessions on 2026-10-28 ended after about 2.3 minutes. |
| Resume | `Last-Event-ID` header with the last `id:` array; `?since=<epoch ms or ISO>` is ignored when `Last-Event-ID` is set, and silently ignored if no offset exists for that time. Retention is 7 to 31 days, depending on the stream. |

Fields used by WikiPulse are listed in `docs/contracts/events/upstream-page-change-1.12.0.subset.json`:

- `page.page_id`, `page.page_title`, `page.namespace_id`, `page.is_redirect`, `page.wikibase_item_id`
- `performer.is_bot`, `performer.is_temp`, `performer.is_system`
- `revision.rev_id`, `revision.rev_sha1`, `revision.revert`
- `prior_state` (for moves)

Known data issues:

- **Canary events** (`meta.domain == "canary"`) are emitted several times per hour to public streams. Ingest
  drops them.
- **Two datacenters** (eqiad and codfw) produce independently. The `id:` array has one entry per datacenter topic;
  an inactive datacenter shows `offset: -1`. The whole array is stored as the resume position.
- **Schema versions:** the `$schema` in events can lag the latest published version (`recentchange` events
  declare 1.0.0 while 1.0.1 is published). Validation is per declared version.
- **Duplicates** are not addressed by the documentation. None appeared in 735 sampled events, but resume replays
  produce them by design; downstream dedups on `meta.id`.
- **Timestamps:** on the related `recentchange` stream, `timestamp` lagged real time by up to 14.9 years
  (categorize and log events on old pages). WikiPulse uses `dt` as event time and treats anything older than the
  window grace as late.
- **IP editors:** none appeared in the samples. Temporary accounts (`performer.is_temp`, usernames like
  `~2026-57395-12`) replaced IP editing; they made up 3.1% of sampled events.
- **`recentchange`** was considered and rejected: no `page_id`, 49% `categorize` events, no temp-account flag.
  See ADR 0001.

## 2. Pageview dumps

### Hourly: `pageviews`

| | |
|---|---|
| URL | `https://dumps.wikimedia.org/other/pageviews/YYYY/YYYY-MM/pageviews-YYYYMMDD-HH0000.gz` |
| Docs | https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageviews |
| Format | gzip text, one row per line, space-separated: `domain_code page_title count_views total_response_size` (the last field is always 0) |
| Frequency | hourly; published a median 2.20 h after the hour (p95 2.77 h, max 20.1 h in September 2026) |
| Size | median 61.0 MB gzip per hour, 185 MB uncompressed, 5.94M lines |
| Licence | CC0 |
| Access rules | at most 3 concurrent connections per IP; User-Agent policy applies |

Domain codes:

- `en` is desktop and `en.m` is mobile web.
- Other projects carry a suffix: `.d` Wiktionary, `.b` Wikibooks, `.q` Wikiquote, `.s` Wikisource, `.voy`
  Wikivoyage.
- Wikidata is `www.wd` and `m.wd`.
- WikiPulse maps `en` and `en.m` to `enwiki`, and `de` and `de.m` to `dewiki`.

Known data issues:

- There is no `page_id`. Titles are as requested, with raw percent-encoding variants, and redirects are not
  resolved, so one article can appear under many titles. WikiPulse resolves titles to page IDs (data-model.md).
- Only user-agent traffic is counted: spiders and automated agents are filtered upstream, and app traffic is not
  included.
- No checksums are published. The manifest records size, Last-Modified, and a SHA-256 computed on download.
- **Files are republished.** The 2026-06-20 01:00 to 04:00 hours were rewritten on 2026-10-27. The manifest
  detects changed size or mtime and reloads the partition.
- Some lines have a literal `""` domain code (572 of 6,215,561 lines in the 2026-10-26 11:00 hour). They are
  counted and excluded, since they map to no wiki.
- The filename timestamp is the end of the covered hour. Verified 2026-10-28: `pageviews-20261026-120000`
  matched the 11:00 hourly count in `pageview_complete` for 250 of 251 desktop titles, and 12:00 for 33 of 955
  compared rows. WikiPulse sets `hour_utc` to the filename time minus one hour.
- Known historical anomaly: T141506 (Chrome 41 Main_Page spike, 2016). It is outside the coverage window.

### Daily: `pageview_complete`

| | |
|---|---|
| URL | `https://dumps.wikimedia.org/other/pageview_complete/YYYY/YYYY-MM/pageviews-YYYYMMDD-user.bz2` |
| Docs | https://dumps.wikimedia.org/other/pageview_complete/readme.html |
| Format | bz2 text, space-separated: `wiki_code page_title page_id access daily_total hourly_counts`, for example `en.wikipedia ! 7712754 mobile-web 16 C1F1I2J1L1O1P4V1W1X3` (hours A=00 to X=23) |
| Frequency | daily, around 02:20 to 03:40 UTC the next day |
| Size | user file 0.72 to 0.86 GB per day |
| Licence | CC0 |
| Use | reconciliation only: Wikimedia's own `page_id` attribution checks WikiPulse's title resolution |

Known issues:

- `page_id` is the literal `null` when unknown. One title can appear under several page_ids, so comparisons with
  the hourly files sum per title. The hourly letter counts sum to `daily_total` on every sampled row. The readme notes that such rows may have 5 columns instead of 6;
  the parser accepts both.
- Spider files stopped after 2023-03-12.

## 3. MediaWiki XML dumps: `stub-meta-current`

| | |
|---|---|
| URL | `https://dumps.wikimedia.org/<wiki>/<YYYYMMDD>/<wiki>-<YYYYMMDD>-stub-meta-current[N].xml.gz` |
| Docs | https://meta.wikimedia.org/wiki/Data_dumps/Dump_format; schema https://www.mediawiki.org/xml/export-0.11.xsd |
| Format | gzip XML, export schema 0.11. Each `<page>` has `title`, `ns`, `id`, optional `<redirect title>`, and the latest `<revision>` with `id`, `parentid`, `timestamp`, `contributor`, `<text bytes= sha1=>` (no text body), `sha1`. |
| Frequency | monthly run on the 1st (no run on the 20th in 2026) |
| Size (run 2026-10-01) | enwiki 7.85 GB in 27 parts; dewiki 1.09 GB |
| Checksums | `<wiki>-<date>-md5sums.txt`, `-sha1sums.txt`, `-sha1sums.json`, and `dumpstatus.json` per run |
| Licence | page titles and metadata from CC BY-SA projects (text itself is not read) |
| Access rules | at most 3 connections per IP; mirrors listed at https://meta.wikimedia.org/wiki/Mirroring_Wikimedia_project_XML_dumps |

Known issues and changes:

- **Sunset.** The legacy `stub-meta-history`, `pages-meta-current` and `pages-meta-history` dumps are deprecated;
  their last run is in December 2026, with blackouts on Oct 26 to 31, Nov 15 to 30 and Dec 6 to 31
  (https://wikitech.wikimedia.org/wiki/Legacy_content_dumps_sunset). `stub-meta-current` and the SQL table dumps
  are not affected. This is why WikiPulse reads revision history from source 4 (ADR 0008).
- **Suppressed data.** Deleted contributors and comments appear as `<contributor deleted="deleted"/>` and
  `<comment deleted="deleted"/>`.
- **Run timing.** A run is complete only when `dumpstatus.json` reports `done` for the job; the asset checks this
  before listing parts.

## 4. MediaWiki History TSV

| | |
|---|---|
| URL | `https://dumps.wikimedia.org/other/mediawiki_history/<snapshot YYYY-MM>/<wiki>/<snapshot>.<wiki>.<YYYY-MM>.tsv.bz2` (dewiki is packaged per year) |
| Docs | https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Edits/MediaWiki_history_dumps |
| Format | bz2 TSV, no header. One row per revision, page or user event, with denormalised page and user state and revert flags (`revision_is_identity_reverted`, `revision_is_identity_revert`, `revision_seconds_to_identity_revert`). |
| Frequency | monthly snapshot, published early in the following month |
| Size | enwiki 520 to 585 MB per month; dewiki about 85 MB per month |
| Licence | CC0 for the dataset; titles derive from CC BY-SA content |
| Use | 28-day per-page edit-rate baselines; revert ground truth; backfill of stream gaps |

Known issues:

- The full column list is defined in the docs page and has changed between snapshots. The loader checks the
  column count against the vendored column list and fails on a mismatch.
- Rows carry user names and IPs, which are dropped during parsing (architecture.md, Privacy).
- A snapshot covers history up to its month only.

## 5. MediaWiki Action API and Wikidata

| | |
|---|---|
| Endpoints | `https://<lang>.wikipedia.org/w/api.php`, `https://www.wikidata.org/w/api.php` |
| Calls | `action=query&prop=info\|categories\|pageprops&ppprop=wikibase_item&redirects=1&pageids=...&cllimit=max&formatversion=2&maxlag=5`; `action=query&prop=links` for evaluation labels; `action=wbgetentities&ids=...&props=claims` for P31 (instance of) |
| Batch size | 50 IDs or titles per request |
| Rate limits (2026) | unidentified clients 10 requests/min; clients with a compliant User-Agent 200 requests/min. Over the limit returns 429 (rate) or 503 (overload), usually with `Retry-After`. No rate-limit headers were returned. |
| Licence | Wikipedia metadata derives from CC BY-SA; Wikidata structured data is CC0 |
| Use | categories, Wikidata item, entity type for pages that appear in WikiPulse outputs; Portal:Current events and Main Page links for evaluation |

Known issues:

- `categories` paginates via `clcontinue`.
- `page_change` already carries `namespace_id`, `is_redirect` and `wikibase_item_id`, so the Action API is called
  only for categories and for pages first seen in dumps.
- The Lift Wing topic model (`outlink-topic-model`) is publicly callable and could add topic labels. It is
  optional work (O2) because the ML team has signalled possible restrictions on external access (T426749).
