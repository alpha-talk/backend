ALTER TABLE watchlist_rev ADD COLUMN redis_pending_since TIMESTAMPTZ;

CREATE INDEX idx_watchlist_rev_redis_pending
    ON watchlist_rev (redis_pending_since, user_id)
    WHERE redis_pending_since IS NOT NULL;
