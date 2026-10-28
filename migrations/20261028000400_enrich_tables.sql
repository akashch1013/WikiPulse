-- migrate:up
CREATE TABLE enrich.page_info_cache (
    wiki_id varchar NOT NULL,
    page_id bigint NOT NULL,
    namespace_id integer NOT NULL,
    title text NOT NULL,
    is_redirect boolean NOT NULL,
    wikibase_item_id text,
    categories text[] NOT NULL,
    hidden_categories text[] NOT NULL,
    http_status integer NOT NULL,
    fetched_at timestamptz NOT NULL,
    PRIMARY KEY (wiki_id, page_id)
);
CREATE INDEX page_info_cache_fetched_at_idx ON enrich.page_info_cache (fetched_at);

CREATE TABLE enrich.wikidata_entity_cache (
    qid text PRIMARY KEY CHECK (qid ~ '^Q[0-9]+$'),
    label_en text,
    instance_of text[] NOT NULL,
    instance_of_labels text[] NOT NULL,
    fetched_at timestamptz NOT NULL
);
CREATE INDEX wikidata_entity_cache_fetched_at_idx ON enrich.wikidata_entity_cache (fetched_at);

-- migrate:down
DROP TABLE enrich.wikidata_entity_cache;
DROP TABLE enrich.page_info_cache;
