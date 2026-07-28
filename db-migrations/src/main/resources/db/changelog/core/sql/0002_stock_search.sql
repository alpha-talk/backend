CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_stock_master_name_trgm
    ON stock_master USING gin (name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_stock_master_active_code
    ON stock_master (code)
    WHERE is_active;
