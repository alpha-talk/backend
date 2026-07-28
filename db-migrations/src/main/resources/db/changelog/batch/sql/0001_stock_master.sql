CREATE TABLE IF NOT EXISTS sector (
    code TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stock_master (
    code                CHAR(6)     PRIMARY KEY,
    name                TEXT        NOT NULL,
    market              TEXT        NOT NULL,
    sector_code         TEXT        NULL,
    shares_outstanding  BIGINT      NULL,
    is_active           BOOLEAN     NOT NULL DEFAULT true,
    listed_at           DATE        NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stock_master_name ON stock_master (name);

CREATE TABLE IF NOT EXISTS batch_job_run (
    id          BIGSERIAL   PRIMARY KEY,
    job         TEXT        NOT NULL,
    run_date    CHAR(8)     NOT NULL,
    status      TEXT        NOT NULL,
    ok_count    INTEGER     NOT NULL DEFAULT 0,
    fail_count  INTEGER     NOT NULL DEFAULT 0,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NULL,
    error       TEXT        NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_batch_job_run_job_date ON batch_job_run (job, run_date);
