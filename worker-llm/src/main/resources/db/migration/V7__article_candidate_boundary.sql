ALTER TABLE news_article
    ADD COLUMN candidate_codes_empty BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX idx_news_article_candidate_codes_empty
    ON news_article (cluster_id)
    WHERE candidate_codes_empty = TRUE;
