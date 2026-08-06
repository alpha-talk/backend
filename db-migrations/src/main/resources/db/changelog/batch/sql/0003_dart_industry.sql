CREATE TABLE IF NOT EXISTS dart_corp_map (
    corp_code  CHAR(8)     PRIMARY KEY,
    code       CHAR(6)     NULL,
    corp_name  TEXT        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_dart_corp_map_code
    ON dart_corp_map (code) WHERE code IS NOT NULL;

CREATE TABLE IF NOT EXISTS industry (
    code  TEXT     PRIMARY KEY,
    name  TEXT     NOT NULL,
    level SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS stock_industry (
    code          CHAR(6)     PRIMARY KEY,
    induty_code   TEXT        NOT NULL,
    group_code    TEXT        NOT NULL,
    corp_name     TEXT        NULL,
    corp_name_eng TEXT        NULL,
    stock_name    TEXT        NULL,
    homepage      TEXT        NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_stock_industry_group ON stock_industry (group_code);
