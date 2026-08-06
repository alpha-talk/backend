DROP TABLE IF EXISTS stock_industry;
DROP TABLE IF EXISTS industry;

ALTER TABLE dart_corp_map ADD COLUMN IF NOT EXISTS modify_date TEXT NULL;

ALTER TABLE sector ADD COLUMN IF NOT EXISTS level SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE sector ADD COLUMN IF NOT EXISTS parent_code TEXT NULL;
ALTER TABLE sector ADD COLUMN IF NOT EXISTS version TEXT NOT NULL DEFAULT 'KSIC_10';

CREATE INDEX IF NOT EXISTS idx_sector_parent ON sector (parent_code);

ALTER TABLE stock_master ADD COLUMN IF NOT EXISTS dart_induty_code TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_stock_master_sector ON stock_master (sector_code);
