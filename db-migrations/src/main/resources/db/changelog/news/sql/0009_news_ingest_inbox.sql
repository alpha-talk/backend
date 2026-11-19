CREATE TABLE news_ingest_feed (
    feed_id       text PRIMARY KEY,
    url           text NOT NULL,
    etag          text,
    last_modified bigint,
    version       bigint NOT NULL
);

CREATE TABLE news_ingest_inbox (
    source_id       text PRIMARY KEY,
    payload         text NOT NULL,
    next_attempt_at timestamp with time zone NOT NULL,
    outcome         varchar(16),
    CONSTRAINT news_ingest_inbox_outcome_check CHECK (outcome IN ('COMPLETED', 'FAILED'))
);

CREATE INDEX idx_news_ingest_inbox_pending
    ON news_ingest_inbox (next_attempt_at, source_id) WHERE outcome IS NULL;
