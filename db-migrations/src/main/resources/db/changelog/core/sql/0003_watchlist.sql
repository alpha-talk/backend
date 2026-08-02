CREATE TABLE IF NOT EXISTS watchlist (
    user_id    BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    code       CHAR(6)     NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, code)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_user_created ON watchlist (user_id, created_at);
