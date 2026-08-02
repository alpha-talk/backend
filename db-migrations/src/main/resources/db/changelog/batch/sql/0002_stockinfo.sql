CREATE TABLE IF NOT EXISTS valuation_daily (
    code       CHAR(6)  NOT NULL,
    date       CHAR(8)  NOT NULL,
    per        NUMERIC  NULL,
    pbr        NUMERIC  NULL,
    eps        INTEGER  NULL,
    bps        INTEGER  NULL,
    market_cap BIGINT   NULL,
    PRIMARY KEY (code, date)
);

CREATE TABLE IF NOT EXISTS investor_flow_daily (
    code        CHAR(6) NOT NULL,
    date        CHAR(8) NOT NULL,
    individual  BIGINT  NOT NULL,
    "foreign"   BIGINT  NOT NULL,
    institution BIGINT  NOT NULL,
    PRIMARY KEY (code, date)
);

CREATE TABLE IF NOT EXISTS financial_summary (
    code             CHAR(6)     NOT NULL,
    year             SMALLINT    NOT NULL,
    reprt_code       CHAR(5)     NOT NULL,
    fs_div           CHAR(3)     NOT NULL,
    revenue          BIGINT      NULL,
    operating_profit BIGINT      NULL,
    net_income       BIGINT      NULL,
    assets           BIGINT      NULL,
    liabilities      BIGINT      NULL,
    equity           BIGINT      NULL,
    disclosed_at     TIMESTAMPTZ NULL,
    PRIMARY KEY (code, year, reprt_code)
);
