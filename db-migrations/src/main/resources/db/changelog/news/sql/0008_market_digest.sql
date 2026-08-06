CREATE TABLE IF NOT EXISTS market_digest (
    date       DATE PRIMARY KEY,
    payload    JSONB       NOT NULL,
    degraded   BOOLEAN     NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
