ALTER TABLE news_cluster_stock
    ADD COLUMN rejected BOOLEAN;

UPDATE news_cluster_stock stock
SET rejected = stock.stream_event_id IS NULL
FROM news_cluster cluster
WHERE cluster.id = stock.cluster_id
  AND cluster.status IN ('SUMMARIZED', 'IRRELEVANT');
