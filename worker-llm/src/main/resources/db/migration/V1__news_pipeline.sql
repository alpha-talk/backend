CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE news_cluster (
    id                 CHAR(26)    PRIMARY KEY,
    rep_title          TEXT        NOT NULL,
    summary            TEXT,
    scope              TEXT,
    status             TEXT        NOT NULL,
    first_published_at TIMESTAMPTZ NOT NULL,
    last_article_at    TIMESTAMPTZ NOT NULL,
    article_count      INT         NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_news_cluster_last_article_at ON news_cluster (last_article_at);

CREATE TABLE news_article (
    id           BIGSERIAL    PRIMARY KEY,
    source       TEXT         NOT NULL,
    source_id    TEXT         NOT NULL UNIQUE,
    url          TEXT         NOT NULL,
    title        TEXT         NOT NULL,
    excerpt      VARCHAR(200),
    title_hash   CHAR(16)     NOT NULL,
    embedding    vector(1024),
    published_at TIMESTAMPTZ,
    fetched_at   TIMESTAMPTZ  NOT NULL,
    cluster_id   CHAR(26)     NOT NULL REFERENCES news_cluster (id)
);

CREATE INDEX idx_news_article_title_hash ON news_article (title_hash);
CREATE INDEX idx_news_article_published_at ON news_article (published_at);

CREATE TABLE news_cluster_stock (
    cluster_id      CHAR(26)      NOT NULL REFERENCES news_cluster (id),
    code            CHAR(6)       NOT NULL,
    sentiment       TEXT,
    confidence      NUMERIC(3, 2),
    stream_event_id CHAR(26),
    PRIMARY KEY (cluster_id, code)
);

CREATE INDEX idx_news_cluster_stock_code ON news_cluster_stock (code);

CREATE TABLE news_cluster_sector (
    cluster_id  CHAR(26)      NOT NULL REFERENCES news_cluster (id),
    sector_code TEXT          NOT NULL,
    sentiment   TEXT,
    confidence  NUMERIC(3, 2),
    impact      TEXT,
    PRIMARY KEY (cluster_id, sector_code)
);

CREATE TABLE stock_alias (
    code  CHAR(6) NOT NULL,
    alias TEXT    NOT NULL,
    PRIMARY KEY (code, alias)
);
