-- migrate:up
CREATE TABLE stream.wiki_activity_minute (
    wiki_id varchar NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    editor_type text NOT NULL CHECK (editor_type IN ('bot', 'temp', 'registered', 'anonymous_ip', 'system')),
    edits integer NOT NULL,
    creates integer NOT NULL,
    moves integer NOT NULL,
    deletes integer NOT NULL,
    reverts integer NOT NULL,
    distinct_pages integer NOT NULL,
    distinct_editors integer NOT NULL,
    emitted_at timestamptz NOT NULL,
    PRIMARY KEY (wiki_id, window_start, editor_type)
) PARTITION BY RANGE (window_start);
CREATE INDEX wiki_activity_minute_window_start_idx ON stream.wiki_activity_minute (window_start);

CREATE TABLE stream.page_activity_window (
    wiki_id varchar NOT NULL,
    page_id bigint NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    edits integer NOT NULL,
    human_edits integer NOT NULL,
    reverts integer NOT NULL,
    distinct_editors integer NOT NULL,
    emitted_at timestamptz NOT NULL,
    PRIMARY KEY (wiki_id, page_id, window_start)
) PARTITION BY RANGE (window_start);
CREATE INDEX page_activity_window_wiki_start_human_idx
    ON stream.page_activity_window (wiki_id, window_start, human_edits DESC);

-- Daily partitions: retention is 7 days (wiki_activity_minute) and 3 days (page_activity_window), enforced by
-- ops.drop_day_partitions_before.
SELECT ops.ensure_day_partitions(t, (now() AT TIME ZONE 'UTC')::date - 7, (now() AT TIME ZONE 'UTC')::date + 3)
FROM unnest(ARRAY['stream.wiki_activity_minute', 'stream.page_activity_window']::regclass[]) AS t;

-- migrate:down
DROP TABLE stream.page_activity_window;
DROP TABLE stream.wiki_activity_minute;
