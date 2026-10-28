-- migrate:up
CREATE TABLE ops.batch_manifest (
    source text NOT NULL,
    url text NOT NULL,
    partition_key text NOT NULL,
    etag text,
    last_modified timestamptz,
    size_bytes bigint,
    checksum text,
    status text NOT NULL CHECK (status IN ('pending', 'loading', 'loaded', 'failed', 'superseded')),
    rows_in bigint,
    rows_out bigint,
    attempts integer NOT NULL DEFAULT 0,
    last_error text,
    run_id text,
    loaded_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (source, url)
);
CREATE INDEX batch_manifest_source_status_idx ON ops.batch_manifest (source, status);
CREATE INDEX batch_manifest_source_partition_idx ON ops.batch_manifest (source, partition_key);

CREATE TABLE ops.stream_checkpoint (
    consumer text PRIMARY KEY,
    last_event_id jsonb NOT NULL,
    last_event_dt timestamptz NOT NULL,
    events_acked bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE ops.stream_gaps (
    gap_id bigserial PRIMARY KEY,
    consumer text NOT NULL,
    gap_start timestamptz NOT NULL,
    gap_end timestamptz NOT NULL,
    reason text NOT NULL CHECK (reason IN ('retention_exceeded', 'upstream_outage', 'ingest_down')),
    detected_at timestamptz NOT NULL DEFAULT now(),
    repaired_by text,
    CHECK (gap_end >= gap_start)
);
CREATE INDEX stream_gaps_consumer_start_idx ON ops.stream_gaps (consumer, gap_start);

-- migrate:down
DROP TABLE ops.stream_gaps;
DROP TABLE ops.stream_checkpoint;
DROP TABLE ops.batch_manifest;
