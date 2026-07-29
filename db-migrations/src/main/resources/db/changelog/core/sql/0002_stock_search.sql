CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE INDEX IF NOT EXISTS idx_stock_master_name_trgm
    ON stock_master USING gin (name gin_trgm_ops);

-- 기본 콜레이션(비 C 로케일)에서는 bpchar_pattern_ops 없이 만든 btree가 LIKE 'prefix%'에 쓰이지 않는다.
CREATE INDEX IF NOT EXISTS idx_stock_master_active_code
    ON stock_master (code bpchar_pattern_ops)
    WHERE is_active;
