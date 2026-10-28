-- migrate:up
CREATE TABLE mart.wiki_activity_hourly (
    wiki_id varchar NOT NULL,
    hour_utc timestamptz NOT NULL,
    editor_type text NOT NULL CHECK (editor_type IN ('bot', 'temp', 'registered', 'anonymous_ip', 'system')),
    edits bigint NOT NULL,
    creates bigint NOT NULL,
    moves bigint NOT NULL,
    deletes bigint NOT NULL,
    reverts bigint NOT NULL,
    distinct_pages bigint NOT NULL,
    distinct_editors bigint NOT NULL,
    PRIMARY KEY (wiki_id, hour_utc, editor_type)
) PARTITION BY RANGE (hour_utc);

CREATE TABLE mart.page_activity_hourly (
    wiki_id varchar NOT NULL,
    page_id bigint NOT NULL,
    hour_utc timestamptz NOT NULL,
    namespace_id integer NOT NULL,
    views bigint NOT NULL,
    desktop_views bigint NOT NULL,
    mobile_web_views bigint NOT NULL,
    edits integer NOT NULL,
    human_edits integer NOT NULL,
    reverts integer NOT NULL,
    distinct_editors integer NOT NULL,
    PRIMARY KEY (wiki_id, page_id, hour_utc)
) PARTITION BY RANGE (hour_utc);
CREATE INDEX page_activity_hourly_wiki_hour_views_idx ON mart.page_activity_hourly (wiki_id, hour_utc, views DESC);

CREATE TABLE mart.trending_hourly (
    wiki_id varchar NOT NULL,
    page_id bigint NOT NULL,
    hour_utc timestamptz NOT NULL,
    title text NOT NULL,
    namespace_id integer NOT NULL,
    views bigint NOT NULL,
    baseline_median double precision NOT NULL,
    baseline_mad double precision NOT NULL,
    robust_z double precision NOT NULL,
    ratio double precision NOT NULL,
    rank_in_wiki_hour integer NOT NULL,
    is_trending boolean NOT NULL,
    PRIMARY KEY (wiki_id, page_id, hour_utc)
) PARTITION BY RANGE (hour_utc);
CREATE INDEX trending_hourly_wiki_hour_z_idx ON mart.trending_hourly (wiki_id, hour_utc, robust_z DESC, page_id);

CREATE TABLE mart.editor_mix_hourly (
    wiki_id varchar NOT NULL,
    hour_utc timestamptz NOT NULL,
    namespace_id integer NOT NULL,
    editor_type text NOT NULL CHECK (editor_type IN ('bot', 'temp', 'registered', 'anonymous_ip', 'system')),
    edits bigint NOT NULL,
    reverts bigint NOT NULL,
    distinct_editors bigint NOT NULL,
    PRIMARY KEY (wiki_id, hour_utc, namespace_id, editor_type)
) PARTITION BY RANGE (hour_utc);

SELECT ops.ensure_month_partitions(t, (now() AT TIME ZONE 'UTC')::date - 62, (now() AT TIME ZONE 'UTC')::date + 31)
FROM unnest(ARRAY[
    'mart.wiki_activity_hourly', 'mart.page_activity_hourly', 'mart.trending_hourly', 'mart.editor_mix_hourly'
]::regclass[]) AS t;

CREATE TABLE mart.edit_burst (
    wiki_id varchar NOT NULL,
    page_id bigint NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    title text NOT NULL,
    namespace_id integer NOT NULL,
    edits integer NOT NULL,
    distinct_editors integer NOT NULL,
    baseline_rate_per_hour double precision NOT NULL,
    poisson_p double precision NOT NULL,
    reverts integer NOT NULL,
    PRIMARY KEY (wiki_id, page_id, window_start)
);
CREATE INDEX edit_burst_wiki_start_idx ON mart.edit_burst (wiki_id, window_start DESC, page_id);

CREATE TABLE mart.read_edit_overlap (
    wiki_id varchar NOT NULL,
    page_id bigint NOT NULL,
    trending_hour timestamptz NOT NULL,
    burst_window_start timestamptz NOT NULL,
    title text NOT NULL,
    namespace_id integer NOT NULL,
    lead_minutes integer NOT NULL,
    views bigint NOT NULL,
    robust_z double precision NOT NULL,
    burst_edits integer NOT NULL,
    PRIMARY KEY (wiki_id, page_id, trending_hour, burst_window_start)
);
CREATE INDEX read_edit_overlap_wiki_hour_idx ON mart.read_edit_overlap (wiki_id, trending_hour DESC, page_id);

CREATE TABLE mart.page_current (
    wiki_id varchar NOT NULL,
    page_id bigint NOT NULL,
    title text NOT NULL,
    namespace_id integer NOT NULL,
    is_redirect boolean NOT NULL,
    is_deleted boolean NOT NULL,
    wikibase_item_id text,
    instance_of_label text,
    categories text[],
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (wiki_id, page_id)
);
CREATE INDEX page_current_wiki_namespace_idx ON mart.page_current (wiki_id, namespace_id);

-- The primary key (wiki_id, page_id, valid_from) also serves the contract's lookup index.
CREATE TABLE mart.page_title_history (
    wiki_id varchar NOT NULL,
    page_id bigint NOT NULL,
    title text NOT NULL,
    namespace_id integer NOT NULL,
    is_redirect boolean NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz NOT NULL,
    is_deleted boolean NOT NULL,
    PRIMARY KEY (wiki_id, page_id, valid_from),
    CHECK (valid_to > valid_from)
);

CREATE TABLE mart.evaluation_summary (
    method text NOT NULL,
    label_set text NOT NULL,
    wiki_id varchar NOT NULL,
    window_start date NOT NULL,
    window_end date NOT NULL,
    days integer NOT NULL,
    mean_precision_at_20 double precision NOT NULL,
    mean_recall double precision,
    computed_at timestamptz NOT NULL,
    PRIMARY KEY (method, label_set, wiki_id)
);

-- migrate:down
DROP TABLE mart.evaluation_summary;
DROP TABLE mart.page_title_history;
DROP TABLE mart.page_current;
DROP TABLE mart.read_edit_overlap;
DROP TABLE mart.edit_burst;
DROP TABLE mart.editor_mix_hourly;
DROP TABLE mart.trending_hourly;
DROP TABLE mart.page_activity_hourly;
DROP TABLE mart.wiki_activity_hourly;
