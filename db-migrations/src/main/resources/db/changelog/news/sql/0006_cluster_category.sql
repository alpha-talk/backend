ALTER TABLE news_cluster
    ADD COLUMN category TEXT NOT NULL DEFAULT 'news',
    ADD CONSTRAINT ck_news_cluster_category
        CHECK (category IN ('news', 'report', 'disclosure'));
