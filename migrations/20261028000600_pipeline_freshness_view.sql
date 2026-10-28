-- migrate:up
-- Manifest partition keys follow the Dagster partition definitions:
--   pageviews_hourly   'YYYY-MM-DD-HH:MM' (hour start, UTC)
--   page_snapshot_xml  '<wiki_id>/YYYYMMDD' (dump run date)
--   mediawiki_history  '<wiki_id>/YYYY-MM'
-- Multi-wiki sources report the stalest wiki, so a lagging wiki is not hidden by a current one.
CREATE VIEW ops.pipeline_freshness AS
WITH periods AS (
    SELECT 'stream'::text AS dataset,
           (SELECT max(window_end) FROM stream.wiki_activity_minute) AS last_complete_period,
           'stream.wiki_activity_minute max(window_end)'::text AS source_detail
    UNION ALL
    SELECT 'pageviews_hourly',
           max((substr(partition_key, 1, 10) || 'T' || substr(partition_key, 12, 5) || ':00+00')::timestamptz
               + interval '1 hour'),
           'ops.batch_manifest source=pageviews_hourly status=loaded'
    FROM ops.batch_manifest
    WHERE source = 'pageviews_hourly' AND status = 'loaded'
      AND partition_key ~ '^\d{4}-\d{2}-\d{2}-\d{2}:\d{2}$'
    UNION ALL
    SELECT 'page_snapshot', min(latest), 'ops.batch_manifest source=page_snapshot_xml status=loaded, stalest wiki'
    FROM (
        SELECT split_part(partition_key, '/', 1) AS wiki_id,
               max(to_date(split_part(partition_key, '/', 2), 'YYYYMMDD')::timestamp AT TIME ZONE 'UTC') AS latest
        FROM ops.batch_manifest
        WHERE source = 'page_snapshot_xml' AND status = 'loaded' AND partition_key ~ '^[a-z_]+/\d{8}$'
        GROUP BY 1
    ) per_wiki
    UNION ALL
    SELECT 'mediawiki_history', min(latest), 'ops.batch_manifest source=mediawiki_history status=loaded, stalest wiki'
    FROM (
        SELECT split_part(partition_key, '/', 1) AS wiki_id,
               max((to_date(split_part(partition_key, '/', 2) || '-01', 'YYYY-MM-DD') + interval '1 month')
                   AT TIME ZONE 'UTC') AS latest
        FROM ops.batch_manifest
        WHERE source = 'mediawiki_history' AND status = 'loaded' AND partition_key ~ '^[a-z_]+/\d{4}-\d{2}$'
        GROUP BY 1
    ) per_wiki
    UNION ALL
    SELECT 'marts',
           (SELECT max(hour_utc) + interval '1 hour' FROM mart.wiki_activity_hourly),
           'mart.wiki_activity_hourly max(hour_utc) + 1 hour'
)
SELECT dataset,
       last_complete_period,
       extract(epoch FROM now() - last_complete_period)::bigint AS lag_seconds,
       source_detail
FROM periods;

-- migrate:down
DROP VIEW ops.pipeline_freshness;
