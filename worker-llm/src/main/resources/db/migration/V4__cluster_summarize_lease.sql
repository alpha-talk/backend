ALTER TABLE news_cluster
    ADD COLUMN summarizing_at TIMESTAMPTZ,
    ADD COLUMN summarizing_token TEXT;
