CREATE TABLE IF NOT EXISTS financial_backfill (
    code         CHAR(6)     NOT NULL,
    window_years SMALLINT    NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (code)
);
